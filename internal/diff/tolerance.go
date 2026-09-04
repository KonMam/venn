package diff

// Epsilon tolerance and per-column statistics: the two things that live
// outside the hash join.
//
// A tolerance is not hash-consistent: "within 0.01" is not transitive, so no
// canonicalization can make two tolerably-equal values hash alike. It
// therefore never touches the join. Rows whose values differ at all still
// land in the changed set after pass 2. Pass 3, which already compares every
// column of every changed row for attribution, reclassifies the ones whose
// every difference was inside tolerance. That costs nothing extra and keeps
// the counts honest: they are reported as WithinTolerance, not folded into
// Unchanged.

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/KonMam/tdiff/internal/source"
)

// Tolerance is an epsilon for numeric columns: two values are tolerably
// equal when they differ by at most Abs, or relatively by at most Rel (of
// the larger magnitude). Either bound alone is enough; a zero bound is off.
type Tolerance struct {
	Abs float64 `json:"abs,omitempty"`
	Rel float64 `json:"rel,omitempty"`
}

func (t Tolerance) String() string {
	switch {
	case t.Abs > 0 && t.Rel > 0:
		return fmt.Sprintf("±%g or ±%g relative", t.Abs, t.Rel)
	case t.Rel > 0:
		return fmt.Sprintf("±%g relative", t.Rel)
	default:
		return fmt.Sprintf("±%g", t.Abs)
	}
}

// covers reports whether a difference of absDiff between a and b is inside
// the tolerance.
func (t *Tolerance) covers(absDiff, a, b float64) bool {
	if t == nil {
		return false
	}
	if t.Abs > 0 && absDiff <= t.Abs {
		return true
	}
	if t.Rel > 0 {
		return absDiff <= t.Rel*math.Max(math.Abs(a), math.Abs(b))
	}
	return false
}

// ParseTolerance parses one --tolerance spec:
//
//	0.01               absolute, every numeric column
//	0.01,rel=0.001     absolute and relative
//	rel=1e-6           relative only
//	price=0.01         one column
//	price=0.01,rel=0.001
//
// An empty column name means "every numeric column".
func ParseTolerance(spec string) (string, Tolerance, error) {
	var t Tolerance
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "", t, fmt.Errorf("empty --tolerance")
	}
	items := strings.Split(spec, ",")
	// an optional "<column>=" prefix: the first item's left-hand side, when
	// it is a name rather than one of the bound keywords
	col := ""
	if lhs, rhs, ok := strings.Cut(items[0], "="); ok {
		switch strings.TrimSpace(lhs) {
		case "abs", "rel":
		default:
			col = strings.TrimSpace(lhs)
			if col == "" {
				return "", t, fmt.Errorf("bad --tolerance %q: empty column name", spec)
			}
			items[0] = rhs
		}
	}
	any := false
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		key, val, hasKey := strings.Cut(it, "=")
		if !hasKey {
			key, val = "abs", it
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil || f < 0 || math.IsInf(f, 0) || math.IsNaN(f) {
			return "", t, fmt.Errorf("bad --tolerance %q: %q is not a non-negative number", spec, val)
		}
		switch strings.TrimSpace(key) {
		case "abs":
			t.Abs = f
		case "rel":
			t.Rel = f
		default:
			return "", t, fmt.Errorf("bad --tolerance %q: unknown bound %q (want abs or rel)", spec, key)
		}
		any = true
	}
	if !any || (t.Abs == 0 && t.Rel == 0) {
		return "", t, fmt.Errorf("bad --tolerance %q: no non-zero bound given", spec)
	}
	return col, t, nil
}

// resolveTolerances maps the option tolerances onto the compared columns.
// Only numeric columns can carry one; naming a non-numeric or unknown column
// is an error rather than a silently ignored flag.
func (p *plan) resolveTolerances(opts *Options) error {
	if opts.Tolerance == nil && len(opts.ColumnTolerance) == 0 {
		return nil
	}
	byName := make(map[string]int, len(p.valNames))
	for i, n := range p.valNames {
		byName[n] = i
	}
	p.tol = make([]*Tolerance, len(p.valNames))
	if t := opts.Tolerance; t != nil {
		for i := range p.valNames {
			if p.numericCol(i) {
				tc := *t
				p.tol[i] = &tc
			}
		}
		p.tolDesc = append(p.tolDesc, "tolerance "+t.String())
	}
	// group the per-column tolerances by bound so the report names each once
	perCol := map[string][]string{}
	for name, t := range opts.ColumnTolerance {
		i, ok := byName[name]
		if !ok {
			return fmt.Errorf("--tolerance names column %q, which is not compared (not in both inputs, or ignored)", name)
		}
		if !p.numericCol(i) {
			return fmt.Errorf("--tolerance names column %q, which is not numeric; a tolerance applies to int64 and float columns only", name)
		}
		tc := t
		p.tol[i] = &tc
		perCol[t.String()] = append(perCol[t.String()], name)
	}
	specs := make([]string, 0, len(perCol))
	for spec := range perCol {
		specs = append(specs, spec)
	}
	sort.Strings(specs)
	for _, spec := range specs {
		cols := perCol[spec]
		sort.Strings(cols)
		p.tolDesc = append(p.tolDesc, fmt.Sprintf("tolerance %s on %s", spec, strings.Join(cols, ", ")))
	}
	p.hasTol = true
	return nil
}

// numericCol reports whether compared column i carries a magnitude that a
// tolerance or a mean/max difference can be measured on. Timestamps, dates
// and bools deliberately do not: --timestamp-precision is the
// hash-consistent way to loosen a timestamp comparison.
func (p *plan) numericCol(i int) bool {
	if p.valModes[i] == modeFloat {
		return true
	}
	return p.valModes[i] == modeInt && p.valTypes[i] == source.TypeInt64
}

func (p *plan) tolFor(i int) *Tolerance {
	if p.tol == nil {
		return nil
	}
	return p.tol[i]
}

// numericPair returns both values as float64 when the column carries a
// measurable magnitude, so a difference can be sized.
func numericPair(a, b *source.Value, mode compareMode) (float64, float64, bool) {
	if a.Null || b.Null {
		return 0, 0, false
	}
	asFloat := func(v *source.Value) (float64, bool) {
		switch v.Type {
		case source.TypeFloat64:
			return v.Float, true
		case source.TypeInt64:
			return float64(v.Int), true
		}
		return 0, false
	}
	if mode != modeFloat && mode != modeInt {
		return 0, 0, false
	}
	fa, oka := asFloat(a)
	fb, okb := asFloat(b)
	if !oka || !okb || math.IsNaN(fa) || math.IsNaN(fb) {
		return 0, 0, false
	}
	return fa, fb, true
}

// colDiff is one column of one matched row pair that differs.
type colDiff struct {
	i       int
	absDiff float64 // magnitude, when the column is numeric
	numeric bool
	beyond  bool // differs by more than this column's tolerance
}

// rowDiffs compares the compared columns of one matched row pair. It appends
// every differing column to diffs (reusing its storage) and reports how many
// of them fall outside tolerance. Zero means the row is only tolerably
// different.
func (p *plan) rowDiffs(lvals, rvals []source.Value, diffs []colDiff) ([]colDiff, int) {
	diffs = diffs[:0]
	beyond := 0
	for i := range p.valNames {
		lv, rv := &lvals[i], &rvals[i]
		if valuesEqual(lv, rv, p.valModes[i], p.norm) {
			continue
		}
		d := colDiff{i: i, beyond: true}
		if fa, fb, ok := numericPair(lv, rv, p.valModes[i]); ok {
			d.absDiff, d.numeric = math.Abs(fa-fb), true
			if p.tolFor(i).covers(d.absDiff, fa, fb) {
				d.beyond = false
			}
		}
		if d.beyond {
			beyond++
		}
		diffs = append(diffs, d)
	}
	return diffs, beyond
}

// colAcc accumulates one worker's per-column statistics.
type colAcc struct {
	count      int64
	numericN   int64
	sumAbsDiff float64
	maxAbsDiff float64
}

func (a *colAcc) add(d *colDiff) {
	a.count++
	if !d.numeric {
		return
	}
	a.numericN++
	a.sumAbsDiff += d.absDiff
	if d.absDiff > a.maxAbsDiff {
		a.maxAbsDiff = d.absDiff
	}
}

func (a *colAcc) merge(b *colAcc) {
	a.count += b.count
	a.numericN += b.numericN
	a.sumAbsDiff += b.sumAbsDiff
	if b.maxAbsDiff > a.maxAbsDiff {
		a.maxAbsDiff = b.maxAbsDiff
	}
}

// mergeColStats folds one worker's accumulators into the result. Caller holds
// the result lock.
func (r *Result) mergeColStats(names []string, accs []colAcc) {
	for i := range accs {
		a := &accs[i]
		if a.count == 0 {
			continue
		}
		r.ColumnChanges[names[i]] += a.count
		if r.ColumnStats == nil {
			r.ColumnStats = map[string]*ColumnStat{}
		}
		st := r.ColumnStats[names[i]]
		if st == nil {
			st = &ColumnStat{}
			r.ColumnStats[names[i]] = st
		}
		st.Changed += a.count
		st.numericN += a.numericN
		st.sumAbsDiff += a.sumAbsDiff
		if a.maxAbsDiff > st.MaxAbsDiff {
			st.MaxAbsDiff = a.maxAbsDiff
		}
	}
}
