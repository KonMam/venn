package source

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// collectCursor scans the first few row groups of one file with the custom
// kernels either disabled or enabled, appending every value it sees.
func collectCursor(t *testing.T, path string, disable bool) []Col {
	t.Helper()
	os.Unsetenv("VENN_NO_FASTPQ")
	noFastPQ = disable
	src, err := OpenParquet(path)
	if err != nil {
		t.Fatalf("open (disable=%v): %v", disable, err)
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
	// a few row groups is enough to cover the cross-group cursor reset
	limit := min(len(groups), 4)
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

// compareCursors asserts the two scans agree value for value.
func compareCursors(t *testing.T, slow, fast []Col) {
	t.Helper()
	if len(slow) != len(fast) {
		t.Fatalf("column count %d vs %d", len(slow), len(fast))
	}
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

// TestFastCursorDifferential compares the custom decode kernels against the
// parquet-go-based cursor, column by column, over the committed interop
// corpus. That corpus is the point: it covers dictionary and plain encodings,
// data pages v1 and v2, snappy/gzip/zstd/uncompressed, INT96, int-backed
// DECIMAL, delta encodings and three different writers, and
// pyarrow-smallpages carries ten row groups so the cross-group cursor reset
// is exercised too. Every file is committed, so this runs everywhere rather
// than only on a machine that happens to have generated fixtures.
func TestFastCursorDifferential(t *testing.T) {
	defer func() { noFastPQ = false }()

	files, err := filepath.Glob("../../testdata/interop/*.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("interop corpus missing: testdata/interop/*.parquet is committed and must be present")
	}
	for _, path := range files {
		t.Run(strings.TrimSuffix(filepath.Base(path), ".parquet"), func(t *testing.T) {
			slow := collectCursor(t, path, true)
			fast := collectCursor(t, path, false)
			compareCursors(t, slow, fast)
		})
	}
}

// TestFastCursorDifferentialLarge repeats the comparison against the 10m
// benchmark fixture when it has been generated. It adds row-group counts and
// a column mix the interop corpus does not have, but it is gitignored, so it
// is a bonus rather than the coverage this file relies on.
func TestFastCursorDifferentialLarge(t *testing.T) {
	defer func() { noFastPQ = false }()

	const path = "../../testdata/10m/left.parquet"
	if _, err := os.Stat(path); err != nil {
		t.Skip("10m fixture not generated (go run ./bench/gen); the interop corpus covers the kernels")
	}
	slow := collectCursor(t, path, true)
	fast := collectCursor(t, path, false)
	compareCursors(t, slow, fast)
}
