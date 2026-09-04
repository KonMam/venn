// Package schema computes schema diffs between two sources and decides which
// columns are comparable for the row diff.
package schema

import (
	"fmt"
	"slices"

	"github.com/KonMam/venn/internal/source"
)

// TypeChange describes one column present in both schemas with different
// logical types.
type TypeChange struct {
	Column     string      `json:"column"`
	LeftType   source.Type `json:"-"`
	RightType  source.Type `json:"-"`
	Left       string      `json:"left"`  // logical (physical) rendering
	Right      string      `json:"right"` // logical (physical) rendering
	Comparable bool        `json:"comparable"`
}

// NullabilityChange describes a column whose nullability differs.
type NullabilityChange struct {
	Column        string `json:"column"`
	LeftNullable  bool   `json:"left_nullable"`
	RightNullable bool   `json:"right_nullable"`
}

// Rename records a right-side column that was compared under a left-side
// name (--rename). Renamed pairs are ordinary Common columns, so they are not
// reported as an added/removed pair. Either way they are reported.
type Rename struct {
	Left  string `json:"left"`  // the name the column is compared under
	Right string `json:"right"` // the right input's own name for it
}

// ApplyRenames returns a copy of right with columns renamed to their
// left-side names, plus the renames actually applied. m is keyed by the
// right-side name; a rename to a name the left does not have, or that
// collides with another right-side column, is an error rather than a
// silently unmatched column.
func ApplyRenames(left, right source.Schema, m map[string]string) (source.Schema, []Rename, error) {
	if len(m) == 0 {
		return right, nil, nil
	}
	out := source.Schema{Columns: slices.Clone(right.Columns)}
	var applied []Rename
	// deterministic order: report and validate by right-side name
	names := make([]string, 0, len(m))
	for rname := range m {
		names = append(names, rname)
	}
	slices.Sort(names)
	for _, rname := range names {
		lname := m[rname]
		ri := out.ColumnIndex(rname)
		if ri < 0 {
			return right, nil, fmt.Errorf("--rename %s=%s: the right input has no column %q", rname, lname, rname)
		}
		if left.ColumnIndex(lname) < 0 {
			return right, nil, fmt.Errorf("--rename %s=%s: the left input has no column %q", rname, lname, lname)
		}
		if rname == lname {
			continue
		}
		if i := out.ColumnIndex(lname); i >= 0 {
			return right, nil, fmt.Errorf("--rename %s=%s: the right input already has a column %q", rname, lname, lname)
		}
		out.Columns[ri].Name = lname
		applied = append(applied, Rename{Left: lname, Right: rname})
	}
	return out, applied, nil
}

// Diff is the full schema comparison result.
type Diff struct {
	AddedColumns   []string            `json:"added_columns"`   // in right only
	RemovedColumns []string            `json:"removed_columns"` // in left only
	TypeChanges    []TypeChange        `json:"type_changes"`
	Nullability    []NullabilityChange `json:"nullability_changes"`
	// Renames lists the right-side columns compared under a left-side name
	// (--rename). They are Common columns, not an added/removed pair, and
	// deliberately do not make the schemas differ, since the caller declared
	// them equivalent, but they are always reported.
	Renames []Rename `json:"renames,omitempty"`
	// Common lists columns present in both schemas whose values can be
	// compared (identical or numerically coercible logical types), in
	// left-schema order.
	Common []string `json:"-"`
}

// Same reports whether the schemas match exactly (ignoring column order).
func (d *Diff) Same() bool {
	return len(d.AddedColumns) == 0 && len(d.RemovedColumns) == 0 &&
		len(d.TypeChanges) == 0 && len(d.Nullability) == 0
}

// Comparable reports whether two logical types can be meaningfully compared
// value-by-value. Identical types always can; int64 and float64 are compared
// numerically; date and timestamp are not conflated.
func Comparable(a, b source.Type) bool {
	if a == b {
		return true
	}
	num := func(t source.Type) bool { return t == source.TypeInt64 || t == source.TypeFloat64 }
	if num(a) && num(b) {
		return true
	}
	// String vs bytes: compare raw bytes.
	str := func(t source.Type) bool { return t == source.TypeString || t == source.TypeBytes }
	return str(a) && str(b)
}

// Compare diffs two schemas. Left is "before", right is "after".
func Compare(left, right source.Schema) Diff {
	var d Diff
	rIdx := make(map[string]int, len(right.Columns))
	for i, c := range right.Columns {
		rIdx[c.Name] = i
	}
	lSeen := make(map[string]bool, len(left.Columns))

	for _, lc := range left.Columns {
		lSeen[lc.Name] = true
		ri, ok := rIdx[lc.Name]
		if !ok {
			d.RemovedColumns = append(d.RemovedColumns, lc.Name)
			continue
		}
		rc := right.Columns[ri]
		comparable := Comparable(lc.Type, rc.Type)
		if lc.Type != rc.Type {
			d.TypeChanges = append(d.TypeChanges, TypeChange{
				Column:     lc.Name,
				LeftType:   lc.Type,
				RightType:  rc.Type,
				Left:       renderType(lc),
				Right:      renderType(rc),
				Comparable: comparable,
			})
		}
		if lc.Nullable != rc.Nullable {
			d.Nullability = append(d.Nullability, NullabilityChange{
				Column: lc.Name, LeftNullable: lc.Nullable, RightNullable: rc.Nullable,
			})
		}
		if comparable {
			d.Common = append(d.Common, lc.Name)
		}
	}
	for _, rc := range right.Columns {
		if !lSeen[rc.Name] {
			d.AddedColumns = append(d.AddedColumns, rc.Name)
		}
	}
	return d
}

func renderType(c source.Column) string {
	if c.PhysicalType != "" && c.PhysicalType != c.Type.String() {
		return c.Type.String() + " (" + c.PhysicalType + ")"
	}
	return c.Type.String()
}
