package diff_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"venn/internal/diff"
	"venn/internal/source"
)

// TestInteropCorpus validates the readers against parquet produced by other
// writers (pyarrow, DuckDB, polars) with their real-world defaults:
// dictionary encoding, v1 data pages, gzip/zstd, INT96 timestamps, DECIMAL,
// delta encodings. Every corpus file must diff as identical against a
// canonical CSV of the same logical data, and produce exactly the planted
// diffs against a mutated CSV. Regenerate with testdata/interop/gen_corpus.py.
func TestInteropCorpus(t *testing.T) {
	dir := "../../testdata/interop"
	files, _ := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if len(files) == 0 {
		t.Skip("interop corpus not generated")
	}
	var want struct {
		Added, Removed, Changed int64
		ColumnChanges           map[string]int64 `json:"column_changes"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "diffs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}

	for _, pf := range files {
		name := strings.TrimSuffix(filepath.Base(pf), ".parquet")
		if name == "pyarrow-nested" {
			t.Run(name, func(t *testing.T) {
				// nested columns are skipped with a warning; the flat id
				// column must still diff cleanly against itself
				res := runDiff(t, pf, pf, diff.Options{Keys: []string{"id"}})
				if !res.RowsSame() {
					t.Errorf("nested self-diff not identical: %+v", res)
				}
			})
			continue
		}
		t.Run(name+"/identical", func(t *testing.T) {
			res := runDiff(t, pf, filepath.Join(dir, "canonical.csv"),
				diff.Options{Keys: []string{"id"}})
			if !res.RowsSame() {
				t.Errorf("vs canonical csv: +%d -%d ~%d (want identical); cols=%v ex=%v",
					res.Added, res.Removed, res.Changed, res.ColumnChanges, res.ChangedExamples)
			}
		})
		t.Run(name+"/mutated", func(t *testing.T) {
			res := runDiff(t, pf, filepath.Join(dir, "mutated.csv"),
				diff.Options{Keys: []string{"id"}})
			if res.Added != want.Added || res.Removed != want.Removed || res.Changed != want.Changed {
				t.Errorf("got +%d -%d ~%d, want +%d -%d ~%d",
					res.Added, res.Removed, res.Changed, want.Added, want.Removed, want.Changed)
			}
			for col, n := range want.ColumnChanges {
				if got := res.ColumnChanges[col]; got != n {
					t.Errorf("column %s: got %d changes, want %d", col, got, n)
				}
			}
		})
	}
	_ = source.TypeInt64
}
