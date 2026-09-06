package schema

import (
	"reflect"
	"testing"

	"github.com/KonMam/venn/internal/source"
)

func mkSchema(cols ...source.Column) source.Schema {
	return source.Schema{Columns: cols}
}

func col(name string, t source.Type) source.Column {
	return source.Column{Name: name, Type: t}
}

func TestCompareIdentical(t *testing.T) {
	s := mkSchema(col("id", source.TypeInt64), col("name", source.TypeString))
	d := Compare(s, s)
	if !d.Same() {
		t.Fatalf("identical schemas not Same: %+v", d)
	}
	if !reflect.DeepEqual(d.Common, []string{"id", "name"}) {
		t.Errorf("Common = %v, want [id name]", d.Common)
	}
}

func TestCompareColumnOrderIrrelevant(t *testing.T) {
	left := mkSchema(col("a", source.TypeInt64), col("b", source.TypeString))
	right := mkSchema(col("b", source.TypeString), col("a", source.TypeInt64))
	d := Compare(left, right)
	if !d.Same() {
		t.Fatalf("reordered schemas not Same: %+v", d)
	}
	// Common follows left-schema order regardless of right order.
	if !reflect.DeepEqual(d.Common, []string{"a", "b"}) {
		t.Errorf("Common = %v, want [a b]", d.Common)
	}
}

func TestCompareAddedRemoved(t *testing.T) {
	left := mkSchema(col("id", source.TypeInt64), col("gone", source.TypeString))
	right := mkSchema(col("id", source.TypeInt64), col("fresh", source.TypeString))
	d := Compare(left, right)
	if d.Same() {
		t.Fatal("differing schemas reported Same")
	}
	if !reflect.DeepEqual(d.AddedColumns, []string{"fresh"}) {
		t.Errorf("AddedColumns = %v, want [fresh]", d.AddedColumns)
	}
	if !reflect.DeepEqual(d.RemovedColumns, []string{"gone"}) {
		t.Errorf("RemovedColumns = %v, want [gone]", d.RemovedColumns)
	}
	if !reflect.DeepEqual(d.Common, []string{"id"}) {
		t.Errorf("Common = %v, want [id]", d.Common)
	}
}

func TestCompareTypeChangeComparable(t *testing.T) {
	left := mkSchema(col("id", source.TypeInt64), col("v", source.TypeInt64))
	right := mkSchema(col("id", source.TypeInt64), col("v", source.TypeFloat64))
	d := Compare(left, right)
	if len(d.TypeChanges) != 1 {
		t.Fatalf("TypeChanges = %+v, want one entry", d.TypeChanges)
	}
	tc := d.TypeChanges[0]
	if tc.Column != "v" || !tc.Comparable {
		t.Errorf("int64→float64 change = %+v, want comparable", tc)
	}
	// Numerically coercible columns stay in the row diff.
	if !reflect.DeepEqual(d.Common, []string{"id", "v"}) {
		t.Errorf("Common = %v, want [id v]", d.Common)
	}
}

func TestCompareTypeChangeIncomparable(t *testing.T) {
	left := mkSchema(col("id", source.TypeInt64), col("ts", source.TypeString))
	right := mkSchema(col("id", source.TypeInt64), col("ts", source.TypeTimestamp))
	d := Compare(left, right)
	if len(d.TypeChanges) != 1 || d.TypeChanges[0].Comparable {
		t.Fatalf("string→timestamp change = %+v, want one incomparable entry", d.TypeChanges)
	}
	// Incomparable columns are excluded from the row diff.
	if !reflect.DeepEqual(d.Common, []string{"id"}) {
		t.Errorf("Common = %v, want [id]", d.Common)
	}
}

func TestCompareNullability(t *testing.T) {
	left := mkSchema(source.Column{Name: "id", Type: source.TypeInt64, Nullable: false})
	right := mkSchema(source.Column{Name: "id", Type: source.TypeInt64, Nullable: true})
	d := Compare(left, right)
	if len(d.Nullability) != 1 {
		t.Fatalf("Nullability = %+v, want one entry", d.Nullability)
	}
	nc := d.Nullability[0]
	if nc.Column != "id" || nc.LeftNullable || !nc.RightNullable {
		t.Errorf("nullability change = %+v", nc)
	}
	// Nullability alone does not remove the column from the row diff.
	if !reflect.DeepEqual(d.Common, []string{"id"}) {
		t.Errorf("Common = %v, want [id]", d.Common)
	}
}

func TestCompareCaseSensitive(t *testing.T) {
	// Column matching is case-sensitive: "ID" and "id" are distinct columns.
	left := mkSchema(col("ID", source.TypeInt64))
	right := mkSchema(col("id", source.TypeInt64))
	d := Compare(left, right)
	if !reflect.DeepEqual(d.RemovedColumns, []string{"ID"}) ||
		!reflect.DeepEqual(d.AddedColumns, []string{"id"}) {
		t.Errorf("case-differing columns: %+v", d)
	}
	if len(d.Common) != 0 {
		t.Errorf("Common = %v, want empty", d.Common)
	}
}

func TestComparable(t *testing.T) {
	cases := []struct {
		a, b source.Type
		want bool
	}{
		{source.TypeInt64, source.TypeInt64, true},
		{source.TypeInt64, source.TypeFloat64, true},
		{source.TypeFloat64, source.TypeInt64, true},
		{source.TypeString, source.TypeBytes, true},
		{source.TypeBytes, source.TypeString, true},
		{source.TypeString, source.TypeTimestamp, false},
		{source.TypeTimestamp, source.TypeString, false},
		{source.TypeDate, source.TypeTimestamp, false},
		{source.TypeBool, source.TypeInt64, false},
		{source.TypeString, source.TypeInt64, false},
	}
	for _, c := range cases {
		if got := Comparable(c.a, c.b); got != c.want {
			t.Errorf("Comparable(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestRenderTypePhysical(t *testing.T) {
	d := Compare(
		mkSchema(source.Column{Name: "v", Type: source.TypeInt64, PhysicalType: "INT32"}),
		mkSchema(source.Column{Name: "v", Type: source.TypeFloat64, PhysicalType: "DOUBLE"}),
	)
	if len(d.TypeChanges) != 1 {
		t.Fatalf("TypeChanges = %+v, want one entry", d.TypeChanges)
	}
	tc := d.TypeChanges[0]
	if tc.Left != "int64 (INT32)" {
		t.Errorf("Left rendering = %q, want %q", tc.Left, "int64 (INT32)")
	}
	if tc.Right != "float64 (DOUBLE)" {
		t.Errorf("Right rendering = %q, want %q", tc.Right, "float64 (DOUBLE)")
	}
}
