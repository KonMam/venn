package source

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestFastCursorDifferential compares the custom kernels against the
// parquet-go-based cursor column by column on a real multi-row-group file.
func TestFastCursorDifferential(t *testing.T) {
	path := "../../testdata/10m/left.parquet"
	if _, err := os.Stat(path); err != nil {
		t.Skip("10m fixture not generated")
	}
	collect := func(disable bool) []Col {
		os.Unsetenv("TDIFF_NO_FASTPQ")
		noFastPQ = disable
		src, err := OpenParquet(path)
		if err != nil {
			t.Fatal(err)
		}
		defer src.Close()
		ps := src.(*parquetSource)
		ncols := len(ps.schema.Columns)
		out := make([]Col, ncols)
		for i := range out {
			out[i].Type = ps.schema.Columns[i].Type
		}
		groups := ps.pf.RowGroups()
		st := &scanState{fast: make([]fastCursor, ncols)}
		// scan first 3 row groups serially, appending all values
		limit := 3
		if len(groups) < limit {
			limit = len(groups)
		}
		for gi := 0; gi < limit; gi++ {
			err := ps.scanRowGroup(groups[gi], gi, st, func(b *Batch) error {
				for ci := 0; ci < ncols; ci++ {
					for r := 0; r < b.N; r++ {
						v := b.Cols[ci].Value(r)
						out[ci].I64 = append(out[ci].I64, v.Int)
						out[ci].F64 = append(out[ci].F64, v.Float)
						out[ci].Str = append(out[ci].Str, strings.Clone(v.Str))
						out[ci].Nulls = append(out[ci].Nulls, v.Null)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("scan (disable=%v) rg %d: %v", disable, gi, err)
			}
		}
		return out
	}
	slow := collect(true)
	fast := collect(false)
	noFastPQ = false
	for ci := range slow {
		if len(slow[ci].I64) != len(fast[ci].I64) {
			t.Fatalf("col %d: row count %d vs %d", ci, len(slow[ci].I64), len(fast[ci].I64))
		}
		bad := 0
		for r := range slow[ci].I64 {
			if slow[ci].Nulls[r] != fast[ci].Nulls[r] || slow[ci].I64[r] != fast[ci].I64[r] ||
				slow[ci].F64[r] != fast[ci].F64[r] || slow[ci].Str[r] != fast[ci].Str[r] {
				if bad == 0 {
					t.Errorf("col %d first mismatch at row %d: generic={null:%v i:%d f:%g s:%q} fast={null:%v i:%d f:%g s:%q}",
						ci, r, slow[ci].Nulls[r], slow[ci].I64[r], slow[ci].F64[r], slow[ci].Str[r],
						fast[ci].Nulls[r], fast[ci].I64[r], fast[ci].F64[r], fast[ci].Str[r])
				}
				bad++
			}
		}
		if bad > 0 {
			t.Errorf("col %d: %d mismatching rows of %d %s", ci, bad, len(slow[ci].I64), fmt.Sprint(slow[ci].Type))
		}
	}
}
