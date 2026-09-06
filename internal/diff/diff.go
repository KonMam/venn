// Package diff implements the format-agnostic keyed row diff: an in-memory
// hash join over two Sources.
//
// Algorithm (in-memory mode):
//
//	pass 1  scan left, build keyHash → rowHash table (~17 B/row)
//	pass 2  scan right, probe: miss → added; hash mismatch → changed
//	        (store the right row); match → unchanged
//	pass 3  only if anything differed: rescan left to attribute changed
//	        columns and collect removed-key examples
//
// Identical inputs (the CI hot path) do exactly one scan of
// each side and stores nothing but the table.
//
// All passes run on every core: sources deliver rows concurrently (parquet
// decodes row groups in parallel, CSV parses records in a worker pool; see
// source.ParallelScanner) and the join table is striped 64 ways.
package diff

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/KonMam/venn/internal/schema"
	"github.com/KonMam/venn/internal/source"
)

// Options configures a row diff.
type Options struct {
	Keys          []string // key column names (required)
	IgnoreColumns []string // excluded from comparison
	// Rename maps a right-side column name to the left-side name it should
	// be compared under, so a renamed column is an ordinary compared column
	// instead of an added/removed pair.
	Rename map[string]string
	// Keyless diffs without a key: whole rows are matched as a multiset, so
	// leftovers on either side are added/removed and Changed is always 0.
	// Mutually exclusive with Keys, OnDup and Tolerance.
	Keyless bool
	// Mask lists columns whose values are replaced by a stable short token
	// everywhere they would be displayed or exported. Display-only: the
	// comparison still uses the real values.
	Mask []string
	// Where records the row filter the caller applied to both inputs, in
	// rendered form. The engine never evaluates it, since filtering happens
	// in the source layer, but it is recorded in snapshots and reported, so a
	// baseline can never be compared against a differently-filtered file.
	Where string
	// MaxDiff is the CI budget ("1000" or "0.5%"). Setting it lets the run
	// stop as soon as the budget is provably exceeded, marking the Result
	// Aborted with partial counts. The caller still decides the verdict.
	MaxDiff string
	Limit   int // max examples kept per category (default 10)
	Threads int // concurrency (default GOMAXPROCS)
	// Summary skips per-column change attribution and example rows: counts
	// and exit code only. This drops the whole third pass: two scans total,
	// same as the identical-inputs fast path.
	Summary bool
	// Mode selects the join strategy: "auto" (default) picks in-memory for
	// inputs whose key table fits comfortably in RAM and streaming above
	// that; "memory" and "stream" force one. Streaming (a grace hash join
	// spilling hash pairs to TempDir) keeps peak memory bounded by
	// partition size instead of input size.
	Mode string
	// TempDir is where streaming mode spills (default os.TempDir()).
	TempDir string
	// OnDup selects duplicate-key handling: "error" (default) fails the
	// diff; "warn" keeps each key's first occurrence per side and reports
	// how many rows were set aside; "match" pairs duplicate keys' rows as
	// multisets (identical rows cancel, leftovers are added/removed, no
	// change attribution inside a group; see dupmatch.go).
	OnDup string
	// FloatPrecision, when > 0, rounds float comparisons to that many
	// decimal digits before hashing and comparing (hash-consistent
	// alternative to an epsilon tolerance).
	FloatPrecision int
	// IgnoreCase folds string comparisons to lower case.
	IgnoreCase bool
	// Trim strips leading and trailing whitespace from string comparisons.
	Trim bool
	// TimestampPrecision truncates timestamp comparisons to "s", "ms" or
	// "us" (empty or "us" = exact microseconds).
	TimestampPrecision string
	// Tolerance, when set, is the epsilon applied to every numeric column;
	// ColumnTolerance overrides it per column. Rows whose every difference
	// is inside tolerance are counted as WithinTolerance instead of Changed.
	// Requires full diff mode (not Summary, not a snapshot).
	Tolerance       *Tolerance
	ColumnTolerance map[string]Tolerance
	// Sink, when set, receives every differing row (added/removed/changed)
	// with typed values as the diff runs, with no extra scans. Implementations
	// must be safe for concurrent calls. Incompatible with Summary.
	Sink RowSink
	// Progress, when set, is updated as the diff runs (phase + rows seen).
	Progress *Progress
}

// Progress carries live counters a caller can render (atomically updated,
// once per batch, so the overhead is negligible).
type Progress struct {
	phase atomic.Pointer[string]
	rows  atomic.Int64
}

// Snapshot returns the current phase name and row count.
func (p *Progress) Snapshot() (string, int64) {
	ph := p.phase.Load()
	if ph == nil {
		return "", 0
	}
	return *ph, p.rows.Load()
}

func (p *Progress) setPhase(name string) {
	if p == nil {
		return
	}
	p.phase.Store(&name)
	p.rows.Store(0)
}

func (p *Progress) add(n int64) {
	if p == nil {
		return
	}
	p.rows.Add(n)
}

// RowSink receives full diff rows during the run. status is 'a', 'r' or 'c';
// key always holds the key values; left/right hold the compared columns'
// values for the side(s) that have the row (nil otherwise). Values are only
// valid during the call.
type RowSink interface {
	WriteDiffRow(status byte, key, left, right []source.Value) error
}

// ResolveColumns reports the key and compared-column layout a diff of these
// schemas will use: the export writers build their file schemas from it.
// Coerced columns take the comparison-domain type (int-vs-float → float64).
func ResolveColumns(left, right source.Schema, opts Options) (keyNames []string, keyTypes []source.Type, valNames []string, valTypes []source.Type, err error) {
	right, _, err = schema.ApplyRenames(left, right, opts.Rename)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	sd := schema.Compare(left, right)
	var p *plan
	if opts.Keyless {
		p, err = keylessPlan(&sd, left, right, &opts)
	} else {
		p, err = buildPlan(&sd, left, right, &opts)
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}
	modeType := func(m compareMode, leftType source.Type) source.Type {
		switch m {
		case modeFloat:
			return source.TypeFloat64
		case modeBytes:
			return source.TypeString
		default:
			return leftType
		}
	}
	for i, k := range p.keyNames {
		keyNames = append(keyNames, k)
		t := modeType(p.keyModes[i], left.Columns[p.leftKey[i]].Type)
		if p.maskKey != nil && p.maskKey[i] {
			t = source.TypeString // the exported value is a token
		}
		keyTypes = append(keyTypes, t)
	}
	for i, v := range p.valNames {
		valNames = append(valNames, v)
		t := modeType(p.valModes[i], left.Columns[p.leftVal[i]].Type)
		if p.maskVal != nil && p.maskVal[i] {
			t = source.TypeString
		}
		valTypes = append(valTypes, t)
	}
	return keyNames, keyTypes, valNames, valTypes, nil
}

// streamRowThreshold is the auto-mode cutoff: above this many left rows the
// in-memory table would exceed ~700 MB, so auto picks streaming.
const streamRowThreshold = 40_000_000

// streamByteThreshold is the auto-mode cutoff when the row count is unknown
// (CSV): stream when the left file exceeds this size.
const streamByteThreshold = 8 << 30

// ColumnChange is one changed cell in an example row.
type ColumnChange struct {
	Column string `json:"column"`
	Left   string `json:"left"`
	Right  string `json:"right"`
}

// RowExample is one example changed row.
type RowExample struct {
	Key     string         `json:"key"`
	Columns []ColumnChange `json:"columns"`
}

// Result is the complete diff outcome.
type Result struct {
	Schema schema.Diff `json:"schema"`

	LeftRows  int64 `json:"left_rows"`
	RightRows int64 `json:"right_rows"`
	Added     int64 `json:"added"`
	Removed   int64 `json:"removed"`
	Changed   int64 `json:"changed"`
	Unchanged int64 `json:"unchanged"`

	// Aborted is set when the run stopped early because --max-diff was
	// provably exceeded: the verdict was already fixed, so the remaining
	// work was skipped. Column attribution and examples are then absent.
	Aborted bool `json:"aborted,omitempty"`
	// PartialCounts distinguishes the two ways a run aborts: true when the
	// probe scan itself was cancelled mid-flight, so every count is a lower
	// bound rather than a total; false when the counts are complete and only
	// the attribution pass was skipped.
	PartialCounts bool `json:"partial_counts,omitempty"`
	// AbortReason explains an Aborted run, for the report.
	AbortReason string `json:"abort_reason,omitempty"`

	// Comparison names the settings that loosened this comparison
	// (normalizations, tolerances); empty for an exact diff.
	Comparison string `json:"comparison,omitempty"`

	// Masked lists the columns whose values were replaced by a token in this
	// result (--mask). Their values are absent from examples and exports.
	Masked []string `json:"masked,omitempty"`

	// Keyless marks a result produced without a key (--keyless): examples
	// name whole rows rather than keys, and Changed is always 0.
	Keyless bool `json:"keyless,omitempty"`

	// Filter is the row filter both inputs were reduced by (--where), if
	// any. Every count is then of the filtered universe.
	Filter string `json:"filter,omitempty"`
	// FilesPruned counts the data files partition pruning skipped entirely.
	FilesPruned int `json:"files_pruned,omitempty"`

	// WithinTolerance counts row pairs whose every difference fell inside
	// --tolerance. They are excluded from Changed and reported separately so
	// the counts stay honest about what was compared.
	WithinTolerance int64 `json:"within_tolerance,omitempty"`

	// ColumnChanges counts, per column, how many changed rows changed in
	// that column.
	ColumnChanges map[string]int64 `json:"column_changes,omitempty"`

	// ColumnStats carries the same counts plus a match rate and, for numeric
	// columns, the size of the differences.
	ColumnStats map[string]*ColumnStat `json:"column_stats,omitempty"`

	// DupsLeft/DupsRight count rows set aside under --on-dup warn (the
	// first occurrence of each key stays in the diff).
	DupsLeft  int64 `json:"dups_left,omitempty"`
	DupsRight int64 `json:"dups_right,omitempty"`
	// DupKeys counts the keys that appear more than once on the left under
	// --on-dup match, and DupRows the rows in them. (Keys duplicated only
	// on the right are matched as multisets too, but are not counted here.)
	DupKeys int64 `json:"dup_keys,omitempty"`
	DupRows int64 `json:"dup_rows,omitempty"`

	AddedExamples   []string     `json:"added_examples,omitempty"`
	RemovedExamples []string     `json:"removed_examples,omitempty"`
	ChangedExamples []RowExample `json:"changed_examples,omitempty"`
	DupExamples     []string     `json:"dup_examples,omitempty"`
}

// ColumnStat is one compared column's change profile.
type ColumnStat struct {
	// Changed counts the row pairs that differ in this column (differences
	// inside --tolerance are not counted).
	Changed int64 `json:"changed"`
	// MatchRate is the share of compared row pairs that agree in this
	// column, in [0,1].
	MatchRate float64 `json:"match_rate"`
	// MaxAbsDiff/MeanAbsDiff size the differences of a numeric column
	// (int64 and float only, over pairs where neither side is NULL).
	MaxAbsDiff  float64 `json:"max_abs_diff,omitempty"`
	MeanAbsDiff float64 `json:"mean_abs_diff,omitempty"`
	Numeric     bool    `json:"numeric,omitempty"`

	sumAbsDiff float64
	numericN   int64
}

// ComparedRows is the number of row pairs present on both sides:
// denominator of every per-column match rate.
func (r *Result) ComparedRows() int64 {
	return r.Changed + r.Unchanged + r.WithinTolerance
}

// FinishStats derives the per-column match rate and mean difference from the
// accumulated totals. It is idempotent, so a caller that adjusts the row
// counts after the diff (folding back rows skipped as shared between two
// snapshots of one table) can simply call it again.
func (r *Result) FinishStats() {
	compared := r.ComparedRows()
	for _, st := range r.ColumnStats {
		// only derive from accumulated magnitudes, and a Numeric flag set by a
		// caller (a decoded result, a hand-built one) is left alone
		if st.numericN > 0 {
			st.Numeric = true
			st.MeanAbsDiff = st.sumAbsDiff / float64(st.numericN)
		}
		st.MatchRate = 1
		if compared > 0 {
			st.MatchRate = 1 - float64(st.Changed)/float64(compared)
		}
	}
}

// newResult is the empty result every diff mode starts from.
func newResult() *Result {
	return &Result{ColumnChanges: map[string]int64{}}
}

// Same reports whether the inputs are identical (schema and rows). An
// aborted run is never same: it stopped because too many rows differed.
func (r *Result) Same() bool {
	return !r.Aborted && r.Schema.Same() && r.RowsSame()
}

// RowsSame reports whether the compared rows are identical.
func (r *Result) RowsSame() bool {
	return !r.Aborted && r.Added == 0 && r.Removed == 0 && r.Changed == 0
}

// plan is the resolved column layout for one diff run.
type plan struct {
	norm     *normalizer
	keyNames []string
	valNames []string
	valModes []compareMode
	valTypes []source.Type // comparison-domain type, for numeric-only features
	keyModes []compareMode
	// maskKey/maskVal mark the columns whose values are shown as a token
	// instead (--mask); nil when nothing is masked.
	maskKey []bool
	maskVal []bool
	hasMask bool
	// tol holds the per-compared-column epsilon (nil entries compare
	// exactly); hasTol is set when any tolerance is configured, and tolDesc
	// renders the configured bounds for the report.
	tol      []*Tolerance
	hasTol   bool
	tolDesc  []string
	keySalts []uint64
	valSalts []uint64
	// per-side column indexes, same order as valNames / keyNames
	leftVal, rightVal []int
	leftKey, rightKey []int
}

func buildPlan(sd *schema.Diff, left, right source.Schema, opts *Options) (*plan, error) {
	if len(opts.Keys) == 0 {
		return nil, fmt.Errorf("a key column is required (--key)")
	}
	ignored := make(map[string]bool)
	for _, c := range opts.IgnoreColumns {
		ignored[c] = true
	}
	common := make(map[string]bool, len(sd.Common))
	for _, c := range sd.Common {
		common[c] = true
	}

	p := &plan{}
	isKey := make(map[string]bool)
	for _, k := range opts.Keys {
		if !common[k] {
			if left.ColumnIndex(k) < 0 {
				return nil, fmt.Errorf("key column %q not found in left input", k)
			}
			if right.ColumnIndex(k) < 0 {
				return nil, fmt.Errorf("key column %q not found in right input", k)
			}
			return nil, fmt.Errorf("key column %q has incomparable types in the two inputs", k)
		}
		isKey[k] = true
		li, ri := left.ColumnIndex(k), right.ColumnIndex(k)
		p.keyNames = append(p.keyNames, k)
		p.leftKey = append(p.leftKey, li)
		p.rightKey = append(p.rightKey, ri)
		p.keyModes = append(p.keyModes, resolveMode(left.Columns[li].Type, right.Columns[ri].Type))
	}
	for _, c := range sd.Common {
		if isKey[c] || ignored[c] {
			continue
		}
		li, ri := left.ColumnIndex(c), right.ColumnIndex(c)
		p.valNames = append(p.valNames, c)
		p.leftVal = append(p.leftVal, li)
		p.rightVal = append(p.rightVal, ri)
		p.valModes = append(p.valModes, resolveMode(left.Columns[li].Type, right.Columns[ri].Type))
		p.valTypes = append(p.valTypes, left.Columns[li].Type)
	}
	p.keySalts = makeSalts(len(p.keyNames), 1)
	p.valSalts = makeSalts(len(p.valNames), 2)
	norm, err := newNormalizer(opts)
	if err != nil {
		return nil, err
	}
	p.norm = norm
	if err := p.resolveTolerances(opts); err != nil {
		return nil, err
	}
	if err := p.resolveMask(opts.Mask); err != nil {
		return nil, err
	}
	return p, nil
}

// hashRow computes (keyHash, rowHash) for one row using the side-specific
// index mapping.
func (p *plan) hashRow(row []source.Value, keyIdx, valIdx []int) (uint64, uint64) {
	kh := mixKeyHash(combineHashes(row, keyIdx, p.keyModes, p.keySalts, p.norm))
	rh := combineHashes(row, valIdx, p.valModes, p.valSalts, p.norm)
	return kh, rh
}

// lanes holds one worker's per-batch hash accumulators plus per-column
// dictionary-hash memos.
type lanes struct {
	khs, rhs []uint64
	keyMemos []dictMemo
	valMemos []dictMemo
}

func (l *lanes) size(n int) {
	if cap(l.khs) < n {
		l.khs = make([]uint64, n)
		l.rhs = make([]uint64, n)
	}
	l.khs = l.khs[:n]
	l.rhs = l.rhs[:n]
}

// hashKeys fills l.khs with key hashes for all rows of the batch.
func (p *plan) hashKeys(b *source.Batch, keyIdx []int, l *lanes) {
	khs := l.khs
	for r := range khs {
		khs[r] = 0
	}
	if l.keyMemos == nil {
		l.keyMemos = make([]dictMemo, len(keyIdx))
	}
	for i, ci := range keyIdx {
		accumulateColumnMemo(&b.Cols[ci], p.keyModes[i], p.keySalts[i], khs, &l.keyMemos[i], p.norm)
	}
	for r := range khs {
		khs[r] = mixKeyHash(khs[r])
	}
}

// hashVals fills l.rhs with row (value-column) hashes for all rows.
func (p *plan) hashVals(b *source.Batch, valIdx []int, l *lanes) {
	rhs := l.rhs
	for r := range rhs {
		rhs[r] = 0
	}
	if l.valMemos == nil {
		l.valMemos = make([]dictMemo, len(valIdx))
	}
	for i, ci := range valIdx {
		accumulateColumnMemo(&b.Cols[ci], p.valModes[i], p.valSalts[i], rhs, &l.valMemos[i], p.norm)
	}
}

// keyDisplay renders the key column values of a row for output. The result
// never aliases row buffers (Display clones strings).
func (p *plan) keyDisplay(row []source.Value, keyIdx []int) string {
	parts := make([]string, len(keyIdx))
	for i, ci := range keyIdx {
		parts[i] = p.showKey(i, &row[ci])
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, "|")
}

// showKey renders key column i of a row, masked when asked.
func (p *plan) showKey(i int, v *source.Value) string {
	if p.maskKey != nil && p.maskKey[i] {
		return maskToken(v)
	}
	return v.Display()
}

// showVal renders compared column i of a row, masked when asked.
func (p *plan) showVal(i int, v *source.Value) string {
	if p.maskVal != nil && p.maskVal[i] {
		return maskToken(v)
	}
	return v.Display()
}

// gatherRow materializes selected columns of row r into dst (reused).
func gatherRow(b *source.Batch, r int, idx []int, dst []source.Value) []source.Value {
	dst = dst[:0]
	for _, ci := range idx {
		dst = append(dst, b.Cols[ci].Value(r))
	}
	return dst
}

// keyDisplayBatch renders the key of row r of a batch.
func (p *plan) keyDisplayBatch(b *source.Batch, r int, keyIdx []int) string {
	parts := make([]string, len(keyIdx))
	for i, ci := range keyIdx {
		v := b.Cols[ci].Value(r)
		parts[i] = p.showKey(i, &v)
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return strings.Join(parts, "|")
}

// storedRow keeps the comparison-relevant values of one right-side changed
// row for column attribution in pass 3.
type storedRow struct {
	key  string
	vals []source.Value
	// keyVals is kept only when an export sink is active and the row's
	// classification is deferred (--on-dup match), where the sink write
	// happens after the scan that produced the row.
	keyVals []source.Value
}

// stripedTable shards the join table 64 ways so all cores can insert/probe
// concurrently.
type stripedTable struct {
	stripes [64]struct {
		mu sync.Mutex
		t  *keyTable
		// dups holds the row-hash multiset of every duplicated key, and is
		// nil unless --on-dup match is in force: the unique-key fast path
		// keeps the single-slot layout and its lock-free probe untouched.
		dups map[uint64]*dupGroup
		_    [40]byte // keep stripes on separate cache lines
	}
}

func newStripedTable(sizeHint int, withDups bool) *stripedTable {
	st := &stripedTable{}
	for i := range st.stripes {
		st.stripes[i].t = newKeyTable(sizeHint / len(st.stripes))
		if withDups {
			st.stripes[i].dups = map[uint64]*dupGroup{}
		}
	}
	return st
}

func (st *stripedTable) stripe(kh uint64) int { return int(kh >> 58) }

// Run executes the diff.
func Run(left, right source.Source, opts Options) (*Result, error) {
	if opts.Limit == 0 {
		opts.Limit = 10
	}
	if opts.Threads == 0 {
		opts.Threads = runtime.GOMAXPROCS(0)
	}
	if opts.Sink != nil && opts.Summary {
		return nil, fmt.Errorf("--output requires full diff mode (drop --summary)")
	}
	switch opts.OnDup {
	case "", "error", "warn", "match":
	default:
		return nil, fmt.Errorf("unknown --on-dup %q (error, warn, match)", opts.OnDup)
	}
	if opts.Summary && (opts.Tolerance != nil || len(opts.ColumnTolerance) > 0) {
		// a tolerance is decided in the attribution pass, which --summary
		// skips entirely; --float-precision is the hash-consistent knob that
		// does work in summary mode
		return nil, fmt.Errorf("--tolerance requires full diff mode (drop --summary, or use --float-precision)")
	}
	if opts.Keyless {
		if err := checkKeylessOptions(&opts); err != nil {
			return nil, err
		}
		return runKeyless(left, right, opts, newResult())
	}
	ls, rs := left.Schema(), right.Schema()
	rs, renames, err := schema.ApplyRenames(ls, rs, opts.Rename)
	if err != nil {
		return nil, err
	}
	sd := schema.Compare(ls, rs)
	sd.Renames = renames
	res := newResult()
	res.Filter = opts.Where
	res.Schema = sd

	p, err2 := buildPlan(&sd, ls, rs, &opts)
	if err2 != nil {
		return nil, err2
	}
	res.Comparison = p.describeComparison()
	res.Masked = p.maskedNames()

	stream := false
	switch opts.Mode {
	case "", "auto":
		if n, ok := left.(interface{ NumRows() int64 }); ok {
			stream = n.NumRows() >= streamRowThreshold
		} else if sz, ok := left.(interface{ SizeBytes() int64 }); ok {
			stream = sz.SizeBytes() >= streamByteThreshold
		}
		// compressed text is decompressor-bound: streaming scans both sides
		// concurrently, so both decompressors run in parallel (measured 2×)
		for _, src := range []source.Source{left, right} {
			if ps, ok := src.(interface{ PreferStreaming() bool }); ok && ps.PreferStreaming() {
				stream = true
			}
		}
	case "memory":
	case "stream":
		stream = true
	default:
		return nil, fmt.Errorf("unknown mode %q (auto, memory, stream)", opts.Mode)
	}
	if stream && opts.OnDup == "match" {
		// streaming's pass C identifies rows by key hash alone, which cannot
		// pick out which rows of a duplicated key were the leftovers
		return nil, fmt.Errorf("--on-dup match needs the in-memory join, but this input selected streaming mode; pass --mode memory to force it (peak memory then scales with the left side), or de-duplicate upstream")
	}
	if stream {
		return runStream(left, right, opts, p, res)
	}

	sizeHint := 1 << 20
	if n, ok := left.(interface{ NumRows() int64 }); ok {
		if ub, filtered := left.(interface{ RowsAreUpperBound() bool }); filtered && ub.RowsAreUpperBound() {
			// --where: the file's count is a ceiling, not a count. Sizing
			// for it would allocate a table for rows that never arrive, so
			// start small and let the table grow into the real size.
			sizeHint = min(int(n.NumRows()), sizeHint)
		} else {
			sizeHint = int(n.NumRows())
		}
	}
	matchDup := opts.OnDup == "match"
	table := newStripedTable(sizeHint, matchDup)
	e := &engine{p: p, opts: &opts, table: table, res: res, matchDup: matchDup}

	if err := e.pass1(left); err != nil {
		return nil, fmt.Errorf("left: %w", err)
	}
	budget, hasBudget, berr := abortThreshold(&opts, res.LeftRows, right)
	if berr != nil {
		return nil, berr
	}
	e.budget, e.hasBudget = budget, hasBudget
	if err := e.pass2(right); err != nil {
		if over, ok := errAs[errBudgetExceeded](err); ok {
			// the verdict is already fixed: report what was counted and say
			// so, rather than paying for the rest of the scan
			res.abort(over, true)
			return res, nil
		}
		return nil, fmt.Errorf("right: %w", err)
	}
	if matchDup {
		// both sides' leftovers are known now: settle the deferred keys
		// before anything counts them (see dupmatch.go)
		addedRows := e.settlePending()
		res.Removed += e.removedDupLeftovers()
		if opts.Sink != nil {
			if err := e.exportAdded(addedRows); err != nil {
				return nil, err
			}
		}
	}
	for i := range table.stripes {
		res.Removed += table.stripes[i].t.unmatchedCount()
	}
	if !opts.Summary && (res.Removed > 0 || len(e.changed) > 0) {
		if err := e.pass3(left); err != nil {
			return nil, fmt.Errorf("left: %w", err)
		}
	}

	res.finishExamples(opts.Limit)
	res.FinishStats()
	return res, nil
}

// abort records an early stop. Attribution and examples are whatever the
// cancelled pass happened to have collected, so they are dropped rather than
// presented as a complete picture.
func (res *Result) abort(over errBudgetExceeded, partial bool) {
	res.Aborted, res.PartialCounts = true, partial
	if partial {
		// deliberately no count here: the atomic that tripped the budget
		// lags the merged counters and varies with worker scheduling, so
		// quoting it would contradict the counts printed alongside
		res.AbortReason = fmt.Sprintf(
			"--max-diff budget of %d exceeded; the scan stopped early, so the counts are lower bounds",
			over.budget)
		return
	}
	res.AbortReason = fmt.Sprintf(
		"--max-diff budget of %d exceeded (%d differing rows); column attribution skipped",
		over.budget, over.seen)
	res.ColumnChanges = map[string]int64{}
	res.ColumnStats = nil
	res.ChangedExamples = nil
	res.finishExamples(0)
}

// finishExamples puts the collected example keys in deterministic order and
// applies the per-category limit. Every diff mode ends with this.
func (res *Result) finishExamples(limit int) {
	sort.Strings(res.AddedExamples)
	sort.Strings(res.RemovedExamples)
	sort.Slice(res.ChangedExamples, func(i, j int) bool {
		return res.ChangedExamples[i].Key < res.ChangedExamples[j].Key
	})
	res.AddedExamples = trim(res.AddedExamples, limit)
	res.RemovedExamples = trim(res.RemovedExamples, limit)
	res.ChangedExamples = trim(res.ChangedExamples, limit)
}

func trim[T any](s []T, n int) []T {
	if len(s) > n {
		return s[:n]
	}
	return s
}

type engine struct {
	p     *plan
	opts  *Options
	table *stripedTable
	res   *Result

	// early-abort budget: diffSeen accumulates added+changed across workers
	// and, once budget is set, crossing it cancels the scan
	budget    int64
	hasBudget bool
	diffSeen  atomic.Int64

	mu      sync.Mutex // guards res and changed during merges
	changed map[uint64]storedRow
	// --on-dup match only: right rows whose classification waits until both
	// sides' leftover counts are known, and the keys that settled as removed
	// (their table slot is marked so the sweep does not count them twice).
	pending     map[uint64]*pendingKey
	removedKeys map[uint64]bool
	matchDup    bool
	deferredErr error
}

// overBudget records n more differing rows and reports whether the budget is
// now provably blown. Called once per batch, not per row.
func (e *engine) overBudget(n int64) error {
	if !e.hasBudget || n == 0 {
		return nil
	}
	if seen := e.diffSeen.Add(n); seen > e.budget {
		return errBudgetExceeded{seen: seen, budget: e.budget}
	}
	return nil
}

// insertBatch is how many (keyHash, rowHash) pairs a pass-1 worker buffers
// per stripe before taking that stripe's lock once for the whole batch.
const insertBatch = 512

type hashPair struct{ kh, rh uint64 }

// errDuplicateKey carries the key hash of a duplicate so Run can rescan for
// the human-readable key value on the (cold) error path.
type errDuplicateKey struct{ kh uint64 }

func (e errDuplicateKey) Error() string { return "duplicate key" }

// pass1 builds the join table from the left side. Inserts are batched per
// stripe: one lock acquisition per insertBatch rows instead of per row.
func (e *engine) pass1(left source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("scan left")
	var total int64
	err := withMerge(left, e.opts.Threads, func() (source.BatchFunc, func()) {
		var rows int64
		var dupErr error
		var l lanes
		bufs := make([][]hashPair, len(table.stripes))
		flush := func(si int) error {
			s := &table.stripes[si]
			s.mu.Lock()
			for _, pr := range bufs[si] {
				if !s.t.insert(pr.kh, pr.rh) {
					s.mu.Unlock()
					return errDuplicateKey{kh: pr.kh}
				}
			}
			s.mu.Unlock()
			bufs[si] = bufs[si][:0]
			return nil
		}
		warnDup := e.opts.OnDup == "warn"
		matchDup := e.matchDup
		var dups, dupKeys, dupRows int64
		var dupEx []string
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, p.leftKey, &l)
			p.hashVals(b, p.leftVal, &l)
			for r := 0; r < b.N; r++ {
				kh := l.khs[r]
				si := table.stripe(kh)
				if matchDup {
					// row by row under the stripe lock: a duplicate has to
					// be promoted into its key's multiset before the next
					// insert for that key can land
					st := &table.stripes[si]
					st.mu.Lock()
					if !st.t.insert(kh, l.rhs[r]) {
						g := st.dups[kh]
						if g == nil {
							first, _, _ := st.t.probe(kh)
							g = &dupGroup{}
							g.add(first) // the slot's row joins its own group
							st.dups[kh] = g
							dupKeys++
							dupRows++
							if len(dupEx) < e.opts.Limit {
								dupEx = append(dupEx, p.keyDisplayBatch(b, r, p.leftKey))
							}
						}
						g.add(l.rhs[r])
						dupRows++
					}
					st.mu.Unlock()
					continue
				}
				if warnDup {
					// warn mode inserts row by row so duplicates can be
					// skipped with their key still in hand
					st := &table.stripes[si]
					st.mu.Lock()
					ok := st.t.insert(kh, l.rhs[r])
					st.mu.Unlock()
					if !ok {
						dups++
						if len(dupEx) < e.opts.Limit {
							dupEx = append(dupEx, p.keyDisplayBatch(b, r, p.leftKey))
						}
					}
					continue
				}
				bufs[si] = append(bufs[si], hashPair{kh, l.rhs[r]})
				if len(bufs[si]) >= insertBatch {
					if err := flush(si); err != nil {
						return err
					}
				}
			}
			return nil
		}
		return fn, func() {
			for si := range bufs {
				if len(bufs[si]) > 0 && dupErr == nil {
					dupErr = flush(si)
				}
			}
			e.mu.Lock()
			total += rows
			e.res.LeftRows = total
			e.res.DupsLeft += dups
			e.res.DupKeys += dupKeys
			e.res.DupRows += dupRows
			e.res.DupExamples = append(e.res.DupExamples, dupEx...)
			if dupErr != nil && e.deferredErr == nil {
				e.deferredErr = dupErr
			}
			e.mu.Unlock()
		}
	})
	if err == nil {
		err = e.deferredErr
	}
	if dup, ok := errAs[errDuplicateKey](err); ok {
		key := findKeyByHash(left, p, dup.kh)
		return fmt.Errorf("duplicate key %s; keyed diff requires unique keys", key)
	}
	if err != nil {
		return err
	}
	for i := range table.stripes {
		table.stripes[i].t.seal()
		// a duplicated key's leftovers are counted from its multiset, so its
		// table slot must stay out of the unmatched-slot sweep
		for kh := range table.stripes[i].dups {
			if _, slot, found := table.stripes[i].t.probe(kh); found {
				table.stripes[i].t.markMatched(slot)
			}
		}
	}
	return nil
}

// errAs is errors.As without needing an addressable target.
func errAs[T error](err error) (T, bool) {
	var t T
	if err == nil {
		return t, false
	}
	if e, ok := err.(T); ok {
		return e, true
	}
	return t, false
}

// findKeyByHash rescans src for the row whose key hashes to kh, to render a
// useful duplicate-key error. Best-effort: returns a placeholder on failure.
func findKeyByHash(src source.Source, p *plan, kh uint64) string {
	it, err := src.Rows()
	if err != nil {
		return "(unknown)"
	}
	defer it.Close()
	row := make([]source.Value, len(src.Schema().Columns))
	for {
		ok, err := it.Next(row)
		if err != nil || !ok {
			return "(unknown)"
		}
		if k, _ := p.hashRow(row, p.leftKey, p.leftVal); k == kh {
			return p.keyDisplay(row, p.leftKey)
		}
	}
}

// pass2 probes with the right side.
func (e *engine) pass2(right source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("scan right")
	e.changed = make(map[uint64]storedRow)
	return withMerge(right, e.opts.Threads, func() (source.BatchFunc, func()) {
		var rows, added, unchanged, changedCount, dups, deferred int64
		var addedEx []string
		var changedLocal []storedRow
		var changedKh []uint64
		pendingLocal := map[uint64]*pendingKey{}
		var l lanes
		var kBuf, vBuf []source.Value
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, p.rightKey, &l)
			p.hashVals(b, p.rightVal, &l)
			warnDup := e.opts.OnDup == "warn"
			before := added + changedCount + int64(len(changedLocal)) + deferred
			for r := 0; r < b.N; r++ {
				kh, rh := l.khs[r], l.rhs[r]
				st := &table.stripes[table.stripe(kh)]
				if e.matchDup {
					if g := st.dups[kh]; g != nil {
						// duplicated key: cancel against the multiset. The
						// map is frozen after pass 1, so only the group's
						// contents need the stripe lock.
						st.mu.Lock()
						took := g.take(rh)
						st.mu.Unlock()
						if took {
							unchanged++
							continue
						}
						deferred++
						p.deferRight(b, r, pendingLocal, kh, !e.opts.Summary, e.opts.Sink != nil)
						continue
					}
				}
				t := st.t
				lh, slot, found := t.probe(kh) // lock-free: table sealed after pass 1
				if e.matchDup {
					// unique on the left: only an exact match consumes the
					// row. Anything else (a different value, or a second
					// right row for the same key) is settled after pass 2,
					// when both sides' leftover counts are known.
					switch {
					case !found:
						added++
						if len(addedEx) < e.opts.Limit {
							addedEx = append(addedEx, p.keyDisplayBatch(b, r, p.rightKey))
						}
						if e.opts.Sink != nil {
							kBuf = gatherRow(b, r, p.rightKey, kBuf)
							vBuf = gatherRow(b, r, p.rightVal, vBuf)
							maskRow(kBuf, p.maskKey)
							maskRow(vBuf, p.maskVal)
							if err := e.opts.Sink.WriteDiffRow('a', kBuf, nil, vBuf); err != nil {
								return err
							}
						}
					case lh == rh && !t.markMatched(slot):
						unchanged++
					default:
						deferred++
						p.deferRight(b, r, pendingLocal, kh, !e.opts.Summary, e.opts.Sink != nil)
					}
					continue
				}
				switch {
				case !found:
					added++
					if len(addedEx) < e.opts.Limit {
						addedEx = append(addedEx, p.keyDisplayBatch(b, r, p.rightKey))
					}
					if e.opts.Sink != nil {
						kBuf = gatherRow(b, r, p.rightKey, kBuf)
						vBuf = gatherRow(b, r, p.rightVal, vBuf)
						maskRow(kBuf, p.maskKey)
						maskRow(vBuf, p.maskVal)
						if err := e.opts.Sink.WriteDiffRow('a', kBuf, nil, vBuf); err != nil {
							return err
						}
					}
				case lh == rh:
					if t.markMatched(slot) && warnDup {
						dups++
						continue
					}
					unchanged++
				default:
					if t.markMatched(slot) && warnDup {
						dups++
						continue
					}
					if e.opts.Summary {
						changedCount++
						continue
					}
					vals := make([]source.Value, len(p.rightVal))
					for i, ci := range p.rightVal {
						vals[i] = b.Cols[ci].Value(r)
						// retained across calls: deep-copy aliased strings
						vals[i].Str = strings.Clone(vals[i].Str)
					}
					changedLocal = append(changedLocal, storedRow{key: p.keyDisplayBatch(b, r, p.rightKey), vals: vals})
					changedKh = append(changedKh, kh)
				}
			}
			// added+changed can only grow, and removed rows only add to the
			// total, so crossing the budget here settles the verdict
			// deferred rows are added or changed once settled: either way
			// they count toward the budget
			return e.overBudget(added + changedCount + int64(len(changedLocal)) + deferred - before)
		}
		return fn, func() {
			e.mu.Lock()
			if len(pendingLocal) > 0 {
				e.mergePending(pendingLocal)
			}
			e.res.RightRows += rows
			e.res.DupsRight += dups
			e.res.Added += added
			e.res.Unchanged += unchanged
			e.res.Changed += changedCount + int64(len(changedLocal))
			e.res.AddedExamples = append(e.res.AddedExamples, addedEx...)
			for i, kh := range changedKh {
				e.changed[kh] = changedLocal[i]
			}
			e.mu.Unlock()
		}
	})
}

// pass3Worker is one attribution worker's private state. Each worker
// accumulates independently and merges once, so the pass needs no locks
// beyond the duplicate groups' (see dupmatch.go).
type pass3Worker struct {
	e         *engine
	cols      []colAcc
	withinTol int64
	removedEx []string
	changedEx []RowExample
	diffs     []colDiff

	kBuf, vBuf, lBuf []source.Value
}

// changedRow attributes one changed row: which columns differ, whether the
// whole row is only tolerably different, the example, and the export.
func (w *pass3Worker) changedRow(b *source.Batch, r int, sr storedRow) error {
	e, p := w.e, w.e.p
	w.lBuf = gatherRow(b, r, p.leftVal, w.lBuf)
	var beyond int
	w.diffs, beyond = p.rowDiffs(w.lBuf, sr.vals, w.diffs)
	if p.hasTol && beyond == 0 {
		w.withinTol++
		return nil
	}
	var example *RowExample
	if len(w.changedEx) < e.opts.Limit {
		w.changedEx = append(w.changedEx, RowExample{Key: sr.key})
		example = &w.changedEx[len(w.changedEx)-1]
	}
	for di := range w.diffs {
		d := &w.diffs[di]
		if !d.beyond {
			continue // inside tolerance: not a difference
		}
		w.cols[d.i].add(d)
		if example != nil {
			example.Columns = append(example.Columns, ColumnChange{
				Column: p.valNames[d.i],
				Left:   p.showVal(d.i, &w.lBuf[d.i]),
				Right:  p.showVal(d.i, &sr.vals[d.i]),
			})
		}
	}
	if e.opts.Sink != nil {
		w.kBuf = gatherRow(b, r, p.leftKey, w.kBuf)
		maskRow(w.kBuf, p.maskKey)
		// the stored right row is consumed once, after its values have been
		// compared: masking in place is safe
		maskRow(w.lBuf, p.maskVal)
		maskRow(sr.vals, p.maskVal)
		return e.opts.Sink.WriteDiffRow('c', w.kBuf, w.lBuf, sr.vals)
	}
	return nil
}

// removedRow records one left-only row.
func (w *pass3Worker) removedRow(b *source.Batch, r int) error {
	e, p := w.e, w.e.p
	if len(w.removedEx) < e.opts.Limit {
		w.removedEx = append(w.removedEx, p.keyDisplayBatch(b, r, p.leftKey))
	}
	if e.opts.Sink == nil {
		return nil
	}
	w.kBuf = gatherRow(b, r, p.leftKey, w.kBuf)
	w.vBuf = gatherRow(b, r, p.leftVal, w.vBuf)
	maskRow(w.kBuf, p.maskKey)
	maskRow(w.vBuf, p.maskVal)
	return e.opts.Sink.WriteDiffRow('r', w.kBuf, w.vBuf, nil)
}

// pass3 rescans the left side for removed-key examples and changed-column
// attribution. The table and changed map are read-only here.
//
// This is also where a --tolerance verdict is reached: the pass already
// compares every column of every changed row, so a row whose differences are
// all inside tolerance is reclassified here: out of Changed, into
// WithinTolerance, with no example and no exported row.
func (e *engine) pass3(left source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("attribute changes")
	return withMerge(left, e.opts.Threads, func() (source.BatchFunc, func()) {
		w := &pass3Worker{e: e, cols: make([]colAcc, len(p.valNames))}
		var l lanes
		fn := func(b *source.Batch) error {
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, p.leftKey, &l)
			if e.matchDup {
				// identifying which rows of a duplicated key were the
				// leftovers needs their row hashes
				p.hashVals(b, p.leftVal, &l)
			}
			for r := 0; r < b.N; r++ {
				kh := l.khs[r]
				if e.matchDup {
					st := &table.stripes[table.stripe(kh)]
					if g := st.dups[kh]; g != nil {
						// taking from what pass 2 left claims exactly the
						// leftover rows and only them; rows with equal row
						// hashes are interchangeable, so which one is
						// claimed changes no reported value
						st.mu.Lock()
						leftover := g.take(l.rhs[r])
						st.mu.Unlock()
						if !leftover {
							continue // cancelled against a right row
						}
						if sr, isChanged := e.changed[kh]; isChanged {
							if err := w.changedRow(b, r, sr); err != nil {
								return err
							}
							continue
						}
						if err := w.removedRow(b, r); err != nil {
							return err
						}
						continue
					}
					if e.removedKeys[kh] {
						// settled as removed, with its slot marked so the
						// sweep would not count it twice
						if err := w.removedRow(b, r); err != nil {
							return err
						}
						continue
					}
				}
				if sr, isChanged := e.changed[kh]; isChanged {
					if err := w.changedRow(b, r, sr); err != nil {
						return err
					}
					continue
				}
				if !table.stripes[table.stripe(kh)].t.matchedKey(kh) {
					if err := w.removedRow(b, r); err != nil {
						return err
					}
				}
			}
			return nil
		}
		return fn, func() {
			e.mu.Lock()
			e.res.mergeColStats(p.valNames, w.cols)
			e.res.Changed -= w.withinTol
			e.res.WithinTolerance += w.withinTol
			e.res.RemovedExamples = append(e.res.RemovedExamples, w.removedEx...)
			e.res.ChangedExamples = append(e.res.ChangedExamples, w.changedEx...)
			e.mu.Unlock()
		}
	})
}

// withMerge runs one parallel scan where each worker has private state and a
// merge step that runs after the worker delivers its last row.
func withMerge(src source.Source, threads int, makeWorker func() (source.BatchFunc, func())) error {
	var mergeMu sync.Mutex
	var merges []func()
	err := source.Scan(src, threads, func() (source.BatchFunc, error) {
		fn, merge := makeWorker()
		mergeMu.Lock()
		merges = append(merges, merge)
		mergeMu.Unlock()
		return fn, nil
	})
	// merges run even on error: counters stay consistent for reporting
	for _, m := range merges {
		m()
	}
	return err
}
