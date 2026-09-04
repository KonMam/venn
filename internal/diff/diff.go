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
// The identical-inputs case — the CI hot path — does exactly one scan of
// each side and stores nothing but the table.
//
// All passes run on every core: sources deliver rows concurrently (parquet
// decodes row groups in parallel, CSV parses records in a worker pool — see
// source.ParallelScanner) and the join table is striped 64 ways.
package diff

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"tdiff/internal/schema"
	"tdiff/internal/source"
)

// Options configures a row diff.
type Options struct {
	Keys          []string // key column names (required)
	IgnoreColumns []string // excluded from comparison
	Limit         int      // max examples kept per category (default 10)
	Threads       int      // concurrency (default GOMAXPROCS)
	// Summary skips per-column change attribution and example rows: counts
	// and exit code only. This drops the whole third pass — two scans total,
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
	// how many rows were set aside.
	OnDup string
	// FloatPrecision, when > 0, rounds float comparisons to that many
	// decimal digits before hashing and comparing (hash-consistent
	// alternative to an epsilon tolerance).
	FloatPrecision int
	// Sink, when set, receives every differing row (added/removed/changed)
	// with typed values as the diff runs — no extra scans. Implementations
	// must be safe for concurrent calls. Incompatible with Summary.
	Sink RowSink
	// Progress, when set, is updated as the diff runs (phase + rows seen).
	Progress *Progress
}

// Progress carries live counters a caller can render (atomically updated,
// once per batch — negligible overhead).
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
// schemas will use — the export writers build their file schemas from it.
// Coerced columns take the comparison-domain type (int-vs-float → float64).
func ResolveColumns(left, right source.Schema, opts Options) (keyNames []string, keyTypes []source.Type, valNames []string, valTypes []source.Type, err error) {
	sd := schema.Compare(left, right)
	p, err := buildPlan(&sd, left, right, &opts)
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
		keyTypes = append(keyTypes, modeType(p.keyModes[i], left.Columns[p.leftKey[i]].Type))
	}
	for i, v := range p.valNames {
		valNames = append(valNames, v)
		valTypes = append(valTypes, modeType(p.valModes[i], left.Columns[p.leftVal[i]].Type))
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

	// ColumnChanges counts, per column, how many changed rows changed in
	// that column.
	ColumnChanges map[string]int64 `json:"column_changes,omitempty"`

	// DupsLeft/DupsRight count rows set aside under --on-dup warn (the
	// first occurrence of each key stays in the diff).
	DupsLeft  int64 `json:"dups_left,omitempty"`
	DupsRight int64 `json:"dups_right,omitempty"`

	AddedExamples   []string     `json:"added_examples,omitempty"`
	RemovedExamples []string     `json:"removed_examples,omitempty"`
	ChangedExamples []RowExample `json:"changed_examples,omitempty"`
	DupExamples     []string     `json:"dup_examples,omitempty"`
}

// Same reports whether the inputs are identical (schema and rows).
func (r *Result) Same() bool {
	return r.Schema.Same() && r.Added == 0 && r.Removed == 0 && r.Changed == 0
}

// RowsSame reports whether the compared rows are identical.
func (r *Result) RowsSame() bool { return r.Added == 0 && r.Removed == 0 && r.Changed == 0 }

// plan is the resolved column layout for one diff run.
type plan struct {
	quant    *floatQuantizer
	keyNames []string
	valNames []string
	valModes []compareMode
	keyModes []compareMode
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
	}
	p.keySalts = makeSalts(len(p.keyNames), 1)
	p.valSalts = makeSalts(len(p.valNames), 2)
	if opts.FloatPrecision > 0 {
		p.quant = newFloatQuantizer(opts.FloatPrecision)
	}
	return p, nil
}

// hashRow computes (keyHash, rowHash) for one row using the side-specific
// index mapping.
func (p *plan) hashRow(row []source.Value, keyIdx, valIdx []int) (uint64, uint64) {
	kh := mixKeyHash(combineHashesQ(row, keyIdx, p.keyModes, p.keySalts, p.quant))
	rh := combineHashesQ(row, valIdx, p.valModes, p.valSalts, p.quant)
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
		accumulateColumnMemo(&b.Cols[ci], p.keyModes[i], p.keySalts[i], khs, &l.keyMemos[i], p.quant)
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
		accumulateColumnMemo(&b.Cols[ci], p.valModes[i], p.valSalts[i], rhs, &l.valMemos[i], p.quant)
	}
}

// keyDisplay renders the key column values of a row for output. The result
// never aliases row buffers (Display clones strings).
func (p *plan) keyDisplay(row []source.Value, keyIdx []int) string {
	if len(keyIdx) == 1 {
		return row[keyIdx[0]].Display()
	}
	parts := make([]string, len(keyIdx))
	for i, ci := range keyIdx {
		parts[i] = row[ci].Display()
	}
	return strings.Join(parts, "|")
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
	if len(keyIdx) == 1 {
		v := b.Cols[keyIdx[0]].Value(r)
		return v.Display()
	}
	parts := make([]string, len(keyIdx))
	for i, ci := range keyIdx {
		v := b.Cols[ci].Value(r)
		parts[i] = v.Display()
	}
	return strings.Join(parts, "|")
}

// storedRow keeps the comparison-relevant values of one right-side changed
// row for column attribution in pass 3.
type storedRow struct {
	key  string
	vals []source.Value
}

// stripedTable shards the join table 64 ways so all cores can insert/probe
// concurrently.
type stripedTable struct {
	stripes [64]struct {
		mu sync.Mutex
		t  *keyTable
		_  [40]byte // keep stripes on separate cache lines
	}
}

func newStripedTable(sizeHint int) *stripedTable {
	st := &stripedTable{}
	for i := range st.stripes {
		st.stripes[i].t = newKeyTable(sizeHint / len(st.stripes))
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
	ls, rs := left.Schema(), right.Schema()
	sd := schema.Compare(ls, rs)
	res := &Result{Schema: sd, ColumnChanges: map[string]int64{}}

	p, err := buildPlan(&sd, ls, rs, &opts)
	if err != nil {
		return nil, err
	}

	stream := false
	switch opts.Mode {
	case "", "auto":
		if n, ok := left.(interface{ NumRows() int64 }); ok {
			stream = n.NumRows() >= streamRowThreshold
		} else if sz, ok := left.(interface{ SizeBytes() int64 }); ok {
			stream = sz.SizeBytes() >= streamByteThreshold
		}
	case "memory":
	case "stream":
		stream = true
	default:
		return nil, fmt.Errorf("unknown mode %q (auto, memory, stream)", opts.Mode)
	}
	if stream {
		return runStream(left, right, opts, p, res)
	}

	sizeHint := 1 << 20
	if n, ok := left.(interface{ NumRows() int64 }); ok {
		sizeHint = int(n.NumRows())
	}
	table := newStripedTable(sizeHint)
	e := &engine{p: p, opts: &opts, table: table, res: res}

	if err := e.pass1(left); err != nil {
		return nil, fmt.Errorf("left: %w", err)
	}
	if err := e.pass2(right); err != nil {
		return nil, fmt.Errorf("right: %w", err)
	}
	for i := range table.stripes {
		res.Removed += table.stripes[i].t.unmatchedCount()
	}
	if !opts.Summary && (res.Removed > 0 || len(e.changed) > 0) {
		if err := e.pass3(left); err != nil {
			return nil, fmt.Errorf("left: %w", err)
		}
	}

	sort.Strings(res.AddedExamples)
	sort.Strings(res.RemovedExamples)
	sort.Slice(res.ChangedExamples, func(i, j int) bool {
		return res.ChangedExamples[i].Key < res.ChangedExamples[j].Key
	})
	res.AddedExamples = trim(res.AddedExamples, opts.Limit)
	res.RemovedExamples = trim(res.RemovedExamples, opts.Limit)
	res.ChangedExamples = trim(res.ChangedExamples, opts.Limit)
	return res, nil
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

	mu          sync.Mutex // guards res and changed during merges
	changed     map[uint64]storedRow
	deferredErr error
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
		var dups int64
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
		return fmt.Errorf("duplicate key %s — keyed diff requires unique keys", key)
	}
	if err != nil {
		return err
	}
	for i := range table.stripes {
		table.stripes[i].t.seal()
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
		var rows, added, unchanged, changedCount, dups int64
		var addedEx []string
		var changedLocal []storedRow
		var changedKh []uint64
		var l lanes
		var kBuf, vBuf []source.Value
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, p.rightKey, &l)
			p.hashVals(b, p.rightVal, &l)
			warnDup := e.opts.OnDup == "warn"
			for r := 0; r < b.N; r++ {
				kh, rh := l.khs[r], l.rhs[r]
				t := table.stripes[table.stripe(kh)].t
				lh, slot, found := t.probe(kh) // lock-free: table sealed after pass 1
				switch {
				case !found:
					added++
					if len(addedEx) < e.opts.Limit {
						addedEx = append(addedEx, p.keyDisplayBatch(b, r, p.rightKey))
					}
					if e.opts.Sink != nil {
						kBuf = gatherRow(b, r, p.rightKey, kBuf)
						vBuf = gatherRow(b, r, p.rightVal, vBuf)
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
			return nil
		}
		return fn, func() {
			e.mu.Lock()
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

// pass3 rescans the left side for removed-key examples and changed-column
// attribution. The table and changed map are read-only here.
func (e *engine) pass3(left source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("attribute changes")
	return withMerge(left, e.opts.Threads, func() (source.BatchFunc, func()) {
		colChanges := make([]int64, len(p.valNames))
		var removedEx []string
		var changedEx []RowExample
		var l lanes
		var kBuf, vBuf []source.Value
		sink := e.opts.Sink
		fn := func(b *source.Batch) error {
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, p.leftKey, &l)
			for r := 0; r < b.N; r++ {
				kh := l.khs[r]
				if sr, isChanged := e.changed[kh]; isChanged {
					var example *RowExample
					if len(changedEx) < e.opts.Limit {
						changedEx = append(changedEx, RowExample{Key: sr.key})
						example = &changedEx[len(changedEx)-1]
					}
					for i := range p.valNames {
						lval := b.Cols[p.leftVal[i]].Value(r)
						lv := &lval
						rv := &sr.vals[i]
						if !valuesEqualQ(lv, rv, p.valModes[i], p.quant) {
							colChanges[i]++
							if example != nil {
								example.Columns = append(example.Columns, ColumnChange{
									Column: p.valNames[i], Left: lv.Display(), Right: rv.Display(),
								})
							}
						}
					}
					if sink != nil {
						kBuf = gatherRow(b, r, p.leftKey, kBuf)
						vBuf = gatherRow(b, r, p.leftVal, vBuf)
						if err := sink.WriteDiffRow('c', kBuf, vBuf, sr.vals); err != nil {
							return err
						}
					}
				} else if !table.stripes[table.stripe(kh)].t.matchedKey(kh) {
					if len(removedEx) < e.opts.Limit {
						removedEx = append(removedEx, p.keyDisplayBatch(b, r, p.leftKey))
					}
					if sink != nil {
						kBuf = gatherRow(b, r, p.leftKey, kBuf)
						vBuf = gatherRow(b, r, p.leftVal, vBuf)
						if err := sink.WriteDiffRow('r', kBuf, vBuf, nil); err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
		return fn, func() {
			e.mu.Lock()
			for i, n := range colChanges {
				if n > 0 {
					e.res.ColumnChanges[p.valNames[i]] += n
				}
			}
			e.res.RemovedExamples = append(e.res.RemovedExamples, removedEx...)
			e.res.ChangedExamples = append(e.res.ChangedExamples, changedEx...)
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
