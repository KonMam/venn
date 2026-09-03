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

	"venn/internal/schema"
	"venn/internal/source"
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
}

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

	AddedExamples   []string     `json:"added_examples,omitempty"`
	RemovedExamples []string     `json:"removed_examples,omitempty"`
	ChangedExamples []RowExample `json:"changed_examples,omitempty"`
}

// Same reports whether the inputs are identical (schema and rows).
func (r *Result) Same() bool {
	return r.Schema.Same() && r.Added == 0 && r.Removed == 0 && r.Changed == 0
}

// RowsSame reports whether the compared rows are identical.
func (r *Result) RowsSame() bool { return r.Added == 0 && r.Removed == 0 && r.Changed == 0 }

// plan is the resolved column layout for one diff run.
type plan struct {
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
	return p, nil
}

// hashRow computes (keyHash, rowHash) for one row using the side-specific
// index mapping.
func (p *plan) hashRow(row []source.Value, keyIdx, valIdx []int) (uint64, uint64) {
	kh := mixKeyHash(combineHashes(row, keyIdx, p.keyModes, p.keySalts))
	rh := combineHashes(row, valIdx, p.valModes, p.valSalts)
	return kh, rh
}

// lanes holds one worker's per-batch hash accumulators.
type lanes struct {
	khs, rhs []uint64
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
	for i, ci := range keyIdx {
		accumulateColumn(&b.Cols[ci], p.keyModes[i], p.keySalts[i], khs)
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
	for i, ci := range valIdx {
		accumulateColumn(&b.Cols[ci], p.valModes[i], p.valSalts[i], rhs)
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
	ls, rs := left.Schema(), right.Schema()
	sd := schema.Compare(ls, rs)
	res := &Result{Schema: sd, ColumnChanges: map[string]int64{}}

	p, err := buildPlan(&sd, ls, rs, &opts)
	if err != nil {
		return nil, err
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
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			l.size(b.N)
			p.hashKeys(b, p.leftKey, &l)
			p.hashVals(b, p.leftVal, &l)
			for r := 0; r < b.N; r++ {
				kh := l.khs[r]
				si := table.stripe(kh)
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
	e.changed = make(map[uint64]storedRow)
	return withMerge(right, e.opts.Threads, func() (source.BatchFunc, func()) {
		var rows, added, unchanged, changedCount int64
		var addedEx []string
		var changedLocal []storedRow
		var changedKh []uint64
		var l lanes
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			l.size(b.N)
			p.hashKeys(b, p.rightKey, &l)
			p.hashVals(b, p.rightVal, &l)
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
				case lh == rh:
					t.markMatched(slot)
					unchanged++
				default:
					t.markMatched(slot)
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
	return withMerge(left, e.opts.Threads, func() (source.BatchFunc, func()) {
		colChanges := make([]int64, len(p.valNames))
		var removedEx []string
		var changedEx []RowExample
		var l lanes
		fn := func(b *source.Batch) error {
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
						if !valuesEqual(lv, rv, p.valModes[i]) {
							colChanges[i]++
							if example != nil {
								example.Columns = append(example.Columns, ColumnChange{
									Column: p.valNames[i], Left: lv.Display(), Right: rv.Display(),
								})
							}
						}
					}
				} else if len(removedEx) < e.opts.Limit && !table.stripes[table.stripe(kh)].t.matchedKey(kh) {
					removedEx = append(removedEx, p.keyDisplayBatch(b, r, p.leftKey))
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
