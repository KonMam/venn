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
package diff

import (
	"fmt"
	"sort"
	"strings"

	"venn/internal/schema"
	"venn/internal/source"
)

// Options configures a row diff.
type Options struct {
	Keys          []string // key column names (required)
	IgnoreColumns []string // excluded from comparison
	Limit         int      // max examples kept per category (default 10)
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
	// aligned comparison columns: value columns first (hashed into rowHash),
	// then nothing else. Key columns are hashed separately into keyHash.
	valNames []string
	valModes []compareMode
	keyModes []compareMode
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
	return p, nil
}

// hashRow computes (keyHash, rowHash) for one row using the side-specific
// index mapping.
func (p *plan) hashRow(h *hasher, row []source.Value, keyIdx, valIdx []int) (uint64, uint64) {
	h.reset()
	for i, ci := range keyIdx {
		h.writeValue(&row[ci], p.keyModes[i])
	}
	keyHash := mixKeyHash(h.sum())
	h.reset()
	for i, ci := range valIdx {
		h.writeValue(&row[ci], p.valModes[i])
	}
	return keyHash, h.sum()
}

// keyDisplay renders the key column values of a row for output.
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

// storedRow keeps the comparison-relevant values of one right-side changed
// row for column attribution in pass 3.
type storedRow struct {
	key  string
	vals []source.Value
}

// Run executes the diff.
func Run(left, right source.Source, opts Options) (*Result, error) {
	if opts.Limit == 0 {
		opts.Limit = 10
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
	table := newKeyTable(sizeHint)

	// pass 1: build from left
	lrow := make([]source.Value, len(ls.Columns))
	h := &hasher{}
	it, err := left.Rows()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := it.Next(lrow)
		if err != nil {
			it.Close()
			return nil, fmt.Errorf("left: %w", err)
		}
		if !ok {
			break
		}
		res.LeftRows++
		kh, rh := p.hashRow(h, lrow, p.leftKey, p.leftVal)
		if !table.insert(kh, rh) {
			it.Close()
			return nil, fmt.Errorf("left: duplicate key %s (row %d) — keyed diff requires unique keys", p.keyDisplay(lrow, p.leftKey), res.LeftRows)
		}
	}
	it.Close()

	// pass 2: probe with right
	changedRows := make(map[uint64]storedRow)
	rrow := make([]source.Value, len(rs.Columns))
	it, err = right.Rows()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := it.Next(rrow)
		if err != nil {
			it.Close()
			return nil, fmt.Errorf("right: %w", err)
		}
		if !ok {
			break
		}
		res.RightRows++
		kh, rh := p.hashRow(h, rrow, p.rightKey, p.rightVal)
		lh, found := table.probe(kh)
		switch {
		case !found:
			res.Added++
			if len(res.AddedExamples) < opts.Limit {
				res.AddedExamples = append(res.AddedExamples, p.keyDisplay(rrow, p.rightKey))
			}
		case lh == rh:
			res.Unchanged++
		default:
			res.Changed++
			vals := make([]source.Value, len(p.rightVal))
			for i, ci := range p.rightVal {
				vals[i] = rrow[ci]
			}
			changedRows[kh] = storedRow{key: p.keyDisplay(rrow, p.rightKey), vals: vals}
		}
	}
	it.Close()

	res.Removed = table.unmatchedCount()
	if res.Removed == 0 && len(changedRows) == 0 {
		return res, nil // identical rows (or only additions): no third pass
	}

	// pass 3: rescan left for removed-key examples and changed-column
	// attribution.
	it, err = left.Rows()
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for {
		ok, err := it.Next(lrow)
		if err != nil {
			return nil, fmt.Errorf("left: %w", err)
		}
		if !ok {
			break
		}
		kh, _ := p.hashRow(h, lrow, p.leftKey, p.leftVal)
		if sr, isChanged := changedRows[kh]; isChanged {
			var example *RowExample
			if len(res.ChangedExamples) < opts.Limit {
				res.ChangedExamples = append(res.ChangedExamples, RowExample{Key: sr.key})
				example = &res.ChangedExamples[len(res.ChangedExamples)-1]
			}
			for i, name := range p.valNames {
				lv := &lrow[p.leftVal[i]]
				rv := &sr.vals[i]
				if !valuesEqual(lv, rv, p.valModes[i]) {
					res.ColumnChanges[name]++
					if example != nil {
						example.Columns = append(example.Columns, ColumnChange{
							Column: name, Left: lv.Display(), Right: rv.Display(),
						})
					}
				}
			}
		} else if !table.matched(kh) && len(res.RemovedExamples) < opts.Limit {
			res.RemovedExamples = append(res.RemovedExamples, p.keyDisplay(lrow, p.leftKey))
		}
	}
	sort.Strings(res.AddedExamples)
	sort.Strings(res.RemovedExamples)
	return res, nil
}
