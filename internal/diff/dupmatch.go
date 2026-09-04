package diff

// Duplicate-key multiset matching (--on-dup match).
//
// A keyed diff needs unique keys: without them, "which right row does this
// left row correspond to?" has no answer. datacompy resolves it by ranking
// rows within each key group and pairing rank 1 with rank 1, which needs a
// deterministic row order. tdiff's scan has none (row groups decode in
// parallel, CSV blocks are parsed by a worker pool), so copying that would
// make counts vary run to run on the same inputs. That is worse than
// refusing.
//
// The order-independent semantics that does fit a hash join is a multiset
// match on the full row:
//
//	within one key, identical rows cancel as unchanged; whatever is left
//	over on the left is removed, on the right is added.
//
// Nothing inside a duplicate group is ever reported as "changed": deciding
// which of three left rows "became" which of three right rows is genuinely
// ambiguous, and guessing reads as authoritative when it is not.
//
// The one refinement is that this rule, applied to every key, would relabel
// ordinary changed rows: a key with one row per side and different values
// becomes "1 removed + 1 added" instead of "1 changed", losing the column
// attribution that is the point of the tool. So a key whose leftovers are
// exactly one row on each side keeps the "changed" label. That is the only
// difference from a pure multiset diff, and it is still order-independent:
// it depends on the leftover counts, not on arrival order.
//
// Right-side duplicates matter as much as left-side ones. A key that is
// unique on the left but appears twice on the right cannot be classified
// while the scan runs (the first right row would win the comparison, and
// which row arrives first is not defined), so classification is deferred:
// pass 2 records the rows that failed to cancel, and the counts are settled
// once between pass 2 and pass 3, when both sides' leftover counts are known.

import (
	"strings"

	"github.com/KonMam/tdiff/internal/source"
)

// dupGroupPromote is the group size past which the multiset switches from a
// linear scan to a counting map. Real duplicate groups are two or three rows,
// where a scan over a slice beats hashing; a pathological key with a million
// copies would make that quadratic.
const dupGroupPromote = 32

// dupGroup is one duplicated key's multiset of left-side row hashes.
// Consumers take entries out of it: pass 2 to cancel matching right rows,
// pass 3 to identify which left rows were the leftovers.
type dupGroup struct {
	list   []uint64         // small groups
	counts map[uint64]int32 // promoted groups
	n      int64            // entries not yet taken
}

func (g *dupGroup) add(rh uint64) {
	g.n++
	if g.counts != nil {
		g.counts[rh]++
		return
	}
	if len(g.list) == dupGroupPromote {
		g.counts = make(map[uint64]int32, dupGroupPromote*2)
		for _, h := range g.list {
			g.counts[h]++
		}
		g.list = nil
		g.counts[rh]++
		return
	}
	g.list = append(g.list, rh)
}

// take removes one occurrence of rh, reporting whether there was one.
func (g *dupGroup) take(rh uint64) bool {
	if g.counts != nil {
		if g.counts[rh] <= 0 {
			return false
		}
		g.counts[rh]--
		g.n--
		return true
	}
	for i, h := range g.list {
		if h == rh {
			last := len(g.list) - 1
			g.list[i] = g.list[last]
			g.list = g.list[:last]
			g.n--
			return true
		}
	}
	return false
}

// remaining reports how many entries have not been taken.
func (g *dupGroup) remaining() int64 { return g.n }

// pendingKey holds the right-side rows of one key that failed to cancel
// against the left side. They are classified as added, or as a single
// changed row, once pass 2 has finished and both leftover counts are known.
type pendingKey struct {
	n    int64       // right rows that failed to cancel
	rows []storedRow // their values; empty under Summary, which needs only n
}

// mergePending folds one worker's pending rows into the engine's map.
// Caller holds the engine lock.
func (e *engine) mergePending(local map[uint64]*pendingKey) {
	if e.pending == nil {
		e.pending = make(map[uint64]*pendingKey, len(local))
	}
	for kh, pk := range local {
		if cur := e.pending[kh]; cur != nil {
			cur.n += pk.n
			cur.rows = append(cur.rows, pk.rows...)
			continue
		}
		e.pending[kh] = pk
	}
}

// deferRight records one right row that failed to cancel. keepRow is false
// under Summary, where only the count is needed and holding every row's
// values would be pure waste.
func (p *plan) deferRight(b *source.Batch, r int, local map[uint64]*pendingKey, kh uint64, keepRow, withKeys bool) {
	pk := local[kh]
	if pk == nil {
		pk = &pendingKey{}
		local[kh] = pk
	}
	pk.n++
	if !keepRow {
		return
	}
	vals := make([]source.Value, len(p.rightVal))
	for i, ci := range p.rightVal {
		vals[i] = b.Cols[ci].Value(r)
		vals[i].Str = strings.Clone(vals[i].Str) // retained across calls
	}
	row := storedRow{key: p.keyDisplayBatch(b, r, p.rightKey), vals: vals}
	if withKeys {
		row.keyVals = make([]source.Value, len(p.rightKey))
		for i, ci := range p.rightKey {
			row.keyVals[i] = b.Cols[ci].Value(r)
			row.keyVals[i].Str = strings.Clone(row.keyVals[i].Str)
		}
	}
	pk.rows = append(pk.rows, row)
}

// exportAdded writes the deferred rows that settled as plain additions. Their
// export could not happen during pass 2: whether they were additions was not
// known until the leftover counts were.
func (e *engine) exportAdded(rows []storedRow) error {
	p := e.p
	for i := range rows {
		row := &rows[i]
		maskRow(row.keyVals, p.maskKey)
		maskRow(row.vals, p.maskVal)
		if err := e.opts.Sink.WriteDiffRow('a', row.keyVals, nil, row.vals); err != nil {
			return err
		}
	}
	return nil
}

// settlePending classifies every deferred key once both sides' leftovers are
// known, and returns the right rows that turned out to be plain additions
// (their export and examples were deferred with them).
//
// The rule, per key: leftovers of exactly one row on each side are one
// changed row; anything else is added + removed.
func (e *engine) settlePending() []storedRow {
	if len(e.pending) == 0 {
		return nil
	}
	limit := e.opts.Limit
	table, res := e.table, e.res
	e.changed = make(map[uint64]storedRow, len(e.pending))
	var addedRows []storedRow
	for kh, pk := range e.pending {
		leftoverR := pk.n
		st := &table.stripes[table.stripe(kh)]
		var leftoverL int64
		if g := st.dups[kh]; g != nil {
			leftoverL = g.remaining()
		} else if _, slot, found := st.t.probe(kh); found && !st.t.isMatched(slot) {
			leftoverL = 1
		}
		if leftoverL == 1 && leftoverR == 1 {
			res.Changed++
			if len(pk.rows) == 1 {
				e.changed[kh] = pk.rows[0]
			}
			if g := st.dups[kh]; g == nil {
				// keep the unmatched-slot sweep from also calling it removed
				_, slot, _ := st.t.probe(kh)
				st.t.markMatched(slot)
			}
			continue
		}
		res.Added += leftoverR
		res.Removed += leftoverL
		for i := range pk.rows {
			if len(res.AddedExamples) < limit {
				res.AddedExamples = append(res.AddedExamples, pk.rows[i].key)
			}
		}
		addedRows = append(addedRows, pk.rows...)
		if g := st.dups[kh]; g == nil && leftoverL == 1 {
			// counted here, so keep it out of the unmatched-slot sweep;
			// pass 3 recognizes it as removed through removedKeys instead
			_, slot, _ := st.t.probe(kh)
			st.t.markMatched(slot)
			if e.removedKeys == nil {
				e.removedKeys = map[uint64]bool{}
			}
			e.removedKeys[kh] = true
		}
	}
	return addedRows
}

// removedDupLeftovers counts the left-side leftovers of duplicate groups that
// settlePending did not already account for.
func (e *engine) removedDupLeftovers() int64 {
	var n int64
	for i := range e.table.stripes {
		for kh, g := range e.table.stripes[i].dups {
			if _, deferred := e.pending[kh]; deferred {
				continue // already counted by settlePending
			}
			n += g.remaining()
		}
	}
	return n
}
