package source

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestInteropDifferential compares fast kernels vs the parquet-go cursor on
// every interop corpus file, cell by cell.
func TestInteropDifferential(t *testing.T) {
	files, _ := filepath.Glob("../../testdata/interop/*.parquet")
	if len(files) == 0 {
		t.Skip("corpus not generated")
	}
	for _, path := range files {
		name := filepath.Base(path)
		t.Run(name, func(t *testing.T) {
			collect := func(disable bool) ([]Col, bool) {
				noFastPQ = disable
				defer func() { noFastPQ = false }()
				src, err := OpenParquet(path)
				if err != nil {
					t.Skipf("open: %v", err)
				}
				defer src.Close()
				ps := src.(*parquetSource)
				ncols := len(ps.schema.Columns)
				out := make([]Col, ncols)
				groups := ps.pf.RowGroups()
				st := &scanState{fast: make([]fastCursor, ncols)}
				for gi := range groups {
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
						t.Fatalf("scan(disable=%v) rg %d: %v", disable, gi, err)
					}
				}
				return out, true
			}
			slow, ok := collect(true)
			if !ok {
				return
			}
			fast, _ := collect(false)
			for ci := range slow {
				bad := 0
				for r := range slow[ci].I64 {
					if slow[ci].Nulls[r] != fast[ci].Nulls[r] || slow[ci].I64[r] != fast[ci].I64[r] ||
						slow[ci].F64[r] != fast[ci].F64[r] || slow[ci].Str[r] != fast[ci].Str[r] {
						if bad == 0 {
							t.Errorf("col %d row %d: generic={null:%v i:%d f:%g s:%q} fast={null:%v i:%d f:%g s:%q}",
								ci, r, slow[ci].Nulls[r], slow[ci].I64[r], slow[ci].F64[r], slow[ci].Str[r],
								fast[ci].Nulls[r], fast[ci].I64[r], fast[ci].F64[r], fast[ci].Str[r])
						}
						bad++
					}
				}
				if bad > 0 {
					t.Errorf("col %d: %d mismatches of %d", ci, bad, len(slow[ci].I64))
				}
			}
		})
	}
	_ = os.Getenv
}
