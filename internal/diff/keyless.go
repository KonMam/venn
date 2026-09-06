package diff

// Keyless mode (--keyless): a diff with no key at all.
//
// Some data has no key worth joining on: an append-only event log, a
// denormalized export, a config dump. Asking "which right row corresponds to
// this left row?" is then meaningless, and so is "changed": the only
// well-defined question is which rows are present on one side and not the
// other, counting multiplicity.
//
// So this is a multiset diff over whole rows:
//
//	pass 1  scan left, build rowHash -> occurrence count
//	pass 2  scan right, consume one occurrence per row; a row with none
//	        left over is added
//	pass 3  only when rows were removed and examples are wanted: rescan the
//	        left side, claiming the leftovers to name them
//
// Changed is always zero, because there is nothing to attribute a change to, so
// there is no column attribution and no --tolerance (an epsilon needs a
// pairing to compare, and none exists here).
//
// It runs on its own three passes rather than threading a flag through the
// keyed ones: the counted table's contract (bump/takeOne) is different
// enough that sharing the code would make both harder to read, and the keyed
// path stays exactly as fast as it was.

import (
	"fmt"
	"sync"

	"github.com/KonMam/venn/internal/schema"
	"github.com/KonMam/venn/internal/source"
)

// keylessPlan resolves the compared columns without a key.
func keylessPlan(sd *schema.Diff, left, right source.Schema, opts *Options) (*plan, error) {
	ignored := make(map[string]bool)
	for _, c := range opts.IgnoreColumns {
		ignored[c] = true
	}
	p := &plan{}
	for _, c := range sd.Common {
		if ignored[c] {
			continue
		}
		li, ri := left.ColumnIndex(c), right.ColumnIndex(c)
		p.valNames = append(p.valNames, c)
		p.leftVal = append(p.leftVal, li)
		p.rightVal = append(p.rightVal, ri)
		p.valModes = append(p.valModes, resolveMode(left.Columns[li].Type, right.Columns[ri].Type))
		p.valTypes = append(p.valTypes, left.Columns[li].Type)
	}
	if len(p.valNames) == 0 {
		return nil, fmt.Errorf("--keyless needs at least one comparable column in both inputs")
	}
	p.valSalts = makeSalts(len(p.valNames), 2)
	norm, err := newNormalizer(opts)
	if err != nil {
		return nil, err
	}
	p.norm = norm
	if err := p.resolveMask(opts.Mask); err != nil {
		return nil, err
	}
	return p, nil
}

// checkKeylessOptions rejects the combinations that cannot mean anything
// without a key, rather than silently ignoring a flag the caller passed.
func checkKeylessOptions(opts *Options) error {
	if len(opts.Keys) > 0 {
		return fmt.Errorf("--keyless and --key are mutually exclusive")
	}
	if opts.Tolerance != nil || len(opts.ColumnTolerance) > 0 {
		return fmt.Errorf("--tolerance needs a pairing between rows, which --keyless by definition does not have; use --float-precision, which is hash-consistent")
	}
	if opts.OnDup != "" && opts.OnDup != "error" {
		return fmt.Errorf("--on-dup has no meaning with --keyless (there are no keys to duplicate)")
	}
	if opts.Mode == "stream" {
		return fmt.Errorf("--keyless needs the in-memory join; pass --mode memory or drop --keyless")
	}
	return nil
}

// rowDisplay renders a whole row for an example, since there is no key to
// name it by. Masked columns show their token.
func (p *plan) rowDisplay(b *source.Batch, r int, idx []int) string {
	out := make([]byte, 0, 16*len(idx))
	for i, ci := range idx {
		if i > 0 {
			out = append(out, '|')
		}
		v := b.Cols[ci].Value(r)
		out = append(out, p.showVal(i, &v)...)
	}
	return string(out)
}

// runKeyless executes the keyless multiset diff.
func runKeyless(left, right source.Source, opts Options, res *Result) (*Result, error) {
	ls, rs := left.Schema(), right.Schema()
	rs, renames, err := schema.ApplyRenames(ls, rs, opts.Rename)
	if err != nil {
		return nil, err
	}
	sd := schema.Compare(ls, rs)
	sd.Renames = renames
	res.Schema = sd
	res.Keyless = true
	res.Filter = opts.Where

	p, err := keylessPlan(&sd, ls, rs, &opts)
	if err != nil {
		return nil, err
	}
	res.Comparison = p.describeComparison()
	res.Masked = p.maskedNames()

	sizeHint := 1 << 20
	if n, ok := left.(interface{ NumRows() int64 }); ok {
		if ub, filtered := left.(interface{ RowsAreUpperBound() bool }); !filtered || !ub.RowsAreUpperBound() {
			sizeHint = int(n.NumRows())
		}
	}
	table := newStripedTable(sizeHint, false)
	e := &engine{p: p, opts: &opts, table: table, res: res}

	if err := e.keylessBuild(left); err != nil {
		return nil, fmt.Errorf("left: %w", err)
	}
	if err := e.keylessProbe(right); err != nil {
		return nil, fmt.Errorf("right: %w", err)
	}
	for i := range table.stripes {
		res.Removed += table.stripes[i].t.remainingTotal()
	}
	if !opts.Summary && res.Removed > 0 {
		if err := e.keylessRemoved(left); err != nil {
			return nil, fmt.Errorf("left: %w", err)
		}
	}
	res.finishExamples(opts.Limit)
	res.FinishStats()
	return res, nil
}

// keylessBuild counts each distinct left row.
func (e *engine) keylessBuild(left source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("scan left")
	var mu sync.Mutex
	var total int64
	err := withMerge(left, e.opts.Threads, func() (source.BatchFunc, func()) {
		var rows int64
		var l lanes
		bufs := make([][]uint64, len(table.stripes))
		flush := func(si int) {
			s := &table.stripes[si]
			s.mu.Lock()
			for _, rh := range bufs[si] {
				s.t.bump(rh)
			}
			s.mu.Unlock()
			bufs[si] = bufs[si][:0]
		}
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashVals(b, p.leftVal, &l)
			for r := 0; r < b.N; r++ {
				// the row hash is the table key here, so it goes through the
				// same empty-slot remapping a key hash does
				h := mixKeyHash(l.rhs[r])
				si := table.stripe(h)
				bufs[si] = append(bufs[si], h)
				if len(bufs[si]) >= insertBatch {
					flush(si)
				}
			}
			return nil
		}
		return fn, func() {
			for si := range bufs {
				if len(bufs[si]) > 0 {
					flush(si)
				}
			}
			mu.Lock()
			total += rows
			e.res.LeftRows = total
			mu.Unlock()
		}
	})
	if err != nil {
		return err
	}
	for i := range table.stripes {
		table.stripes[i].t.seal()
	}
	return nil
}

// keylessProbe consumes one left occurrence per right row.
func (e *engine) keylessProbe(right source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("scan right")
	return withMerge(right, e.opts.Threads, func() (source.BatchFunc, func()) {
		var rows, added, unchanged int64
		var addedEx []string
		var l lanes
		var vBuf []source.Value
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashVals(b, p.rightVal, &l)
			for r := 0; r < b.N; r++ {
				h := mixKeyHash(l.rhs[r])
				t := table.stripes[table.stripe(h)].t
				_, slot, found := t.probe(h)
				if found && t.takeOne(slot) {
					unchanged++
					continue
				}
				added++
				if len(addedEx) < e.opts.Limit {
					addedEx = append(addedEx, p.rowDisplay(b, r, p.rightVal))
				}
				if e.opts.Sink != nil {
					vBuf = gatherRow(b, r, p.rightVal, vBuf)
					maskRow(vBuf, p.maskVal)
					if err := e.opts.Sink.WriteDiffRow('a', nil, nil, vBuf); err != nil {
						return err
					}
				}
			}
			return nil
		}
		return fn, func() {
			e.mu.Lock()
			e.res.RightRows += rows
			e.res.Added += added
			e.res.Unchanged += unchanged
			e.res.AddedExamples = append(e.res.AddedExamples, addedEx...)
			e.mu.Unlock()
		}
	})
}

// keylessRemoved names the left rows that were never consumed. Claiming them
// out of the same counted table is what keeps the multiplicity right: a row
// present three times on the left and twice on the right yields exactly one
// removed example.
func (e *engine) keylessRemoved(left source.Source) error {
	p, table := e.p, e.table
	e.opts.Progress.setPhase("collect removed")
	return withMerge(left, e.opts.Threads, func() (source.BatchFunc, func()) {
		var removedEx []string
		var l lanes
		var vBuf []source.Value
		fn := func(b *source.Batch) error {
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashVals(b, p.leftVal, &l)
			for r := 0; r < b.N; r++ {
				h := mixKeyHash(l.rhs[r])
				t := table.stripes[table.stripe(h)].t
				_, slot, found := t.probe(h)
				if !found || !t.takeOne(slot) {
					continue // consumed by a right row, or already claimed
				}
				if len(removedEx) < e.opts.Limit {
					removedEx = append(removedEx, p.rowDisplay(b, r, p.leftVal))
				}
				if e.opts.Sink != nil {
					vBuf = gatherRow(b, r, p.leftVal, vBuf)
					maskRow(vBuf, p.maskVal)
					if err := e.opts.Sink.WriteDiffRow('r', nil, vBuf, nil); err != nil {
						return err
					}
				}
			}
			return nil
		}
		return fn, func() {
			e.mu.Lock()
			e.res.RemovedExamples = append(e.res.RemovedExamples, removedEx...)
			e.mu.Unlock()
		}
	})
}
