package diff_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"tdiff/internal/diff"
	"tdiff/internal/fixture"
	"tdiff/internal/source"
)

func mustOpen(t *testing.T, path string) source.Source {
	t.Helper()
	s, err := source.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func runDiff(t *testing.T, left, right string, opts diff.Options) *diff.Result {
	t.Helper()
	res, err := diff.Run(mustOpen(t, left), mustOpen(t, right), opts)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestManifestOracle is the core correctness gate: for every variant,
// diff density, and format combination, the diff must find exactly the
// planted differences — nothing more, nothing less.
func TestManifestOracle(t *testing.T) {
	densities := []struct {
		name                    string
		changed, added, removed float64
	}{
		{"identical", 0, 0, 0},
		{"sparse", 0.001, 0.0005, 0.0005},
		{"dense", 0.10, 0.005, 0.005},
	}
	variants := []string{"standard", "wide", "stringy"}
	combos := [][2]string{
		{"parquet", "parquet"},
		{"csv", "csv"},
		{"parquet", "csv"},
		{"csv", "parquet"},
	}

	for _, variant := range variants {
		for _, d := range densities {
			dir := t.TempDir()
			rows := int64(5000)
			man, err := fixture.Generate(fixture.Config{
				Rows: rows, Seed: 42, Out: dir,
				PctChanged: d.changed, PctAdded: d.added, PctRemoved: d.removed,
				Variant: variant,
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, combo := range combos {
				for _, mode := range []string{"memory", "stream"} {
					name := fmt.Sprintf("%s/%s/%s-vs-%s/%s", variant, d.name, combo[0], combo[1], mode)
					t.Run(name, func(t *testing.T) {
						res := runDiff(t,
							filepath.Join(dir, "left."+combo[0]),
							filepath.Join(dir, "right."+combo[1]),
							diff.Options{Keys: []string{"id"}, Limit: 1 << 30, Mode: mode},
						)
						if !res.Schema.Same() {
							t.Errorf("schema diff not empty: %+v", res.Schema)
						}
						if res.LeftRows != man.RowsLeft || res.RightRows != man.RowsRight {
							t.Errorf("rows: got %d/%d want %d/%d", res.LeftRows, res.RightRows, man.RowsLeft, man.RowsRight)
						}
						if res.Added != man.Added || res.Removed != man.Removed || res.Changed != man.Changed {
							t.Errorf("counts: got +%d -%d ~%d, want +%d -%d ~%d",
								res.Added, res.Removed, res.Changed, man.Added, man.Removed, man.Changed)
						}
						for col, want := range man.ColumnChanges {
							if got := res.ColumnChanges[col]; got != want {
								t.Errorf("column %s: got %d changes, want %d", col, got, want)
							}
						}
						for col, got := range res.ColumnChanges {
							if man.ColumnChanges[col] == 0 {
								t.Errorf("column %s: reported %d changes, manifest has none", col, got)
							}
						}
						checkKeys(t, "added", res.AddedExamples, man.AddedKeys)
						checkKeys(t, "removed", res.RemovedExamples, man.RemovedKeys)
						var changedKeys []string
						for _, ex := range res.ChangedExamples {
							changedKeys = append(changedKeys, ex.Key)
						}
						checkKeys(t, "changed", changedKeys, man.ChangedKeys)
						if d.name == "identical" && !res.Same() {
							t.Error("identical fixture did not report Same()")
						}
					})
				}
			}
		}
	}
}

// checkKeys verifies the reported example keys are exactly the manifest set
// (tests run with an unbounded limit).
func checkKeys(t *testing.T, kind string, got []string, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s examples: got %d keys, want %d", kind, len(got), len(want))
		return
	}
	wantSet := make(map[string]bool, len(want))
	for _, k := range want {
		wantSet[strconv.FormatInt(k, 10)] = true
	}
	for _, k := range got {
		if !wantSet[k] {
			t.Errorf("%s examples: unexpected key %s", kind, k)
		}
	}
}

func writeFile(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSchemaDiffAcrossFormats(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a,b\n1,10,x\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a,c\n1,10.5,y\n")
	l, _ := source.Open(left)
	r, _ := source.Open(right)
	res, err := diff.Run(l, r, diff.Options{Keys: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	sd := res.Schema
	if len(sd.RemovedColumns) != 1 || sd.RemovedColumns[0] != "b" {
		t.Errorf("removed: %v", sd.RemovedColumns)
	}
	if len(sd.AddedColumns) != 1 || sd.AddedColumns[0] != "c" {
		t.Errorf("added: %v", sd.AddedColumns)
	}
	if len(sd.TypeChanges) != 1 || sd.TypeChanges[0].Column != "a" || !sd.TypeChanges[0].Comparable {
		t.Errorf("type changes: %+v", sd.TypeChanges)
	}
	// int 10 vs float 10.5 in column a → changed row
	if res.Changed != 1 || res.ColumnChanges["a"] != 1 {
		t.Errorf("changed=%d colchanges=%v", res.Changed, res.ColumnChanges)
	}
}

func TestIntFloatCoercionEquality(t *testing.T) {
	dir := t.TempDir()
	// column a is int on the left, float on the right, same numeric values
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a\n1,10.0\n2,20.0\n")
	res := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
	if !res.RowsSame() {
		t.Errorf("10 (int) should equal 10.0 (float): %+v", res)
	}
}

func TestFloatEdgeCases(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,f\n1,-0\n2,NaN\n3,1.5\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,f\n1,0\n2,NaN\n3,1.5\n")
	res := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
	if !res.RowsSame() {
		t.Errorf("-0 vs 0 and NaN vs NaN should be equal: %+v", res)
	}
}

func TestDuplicateKeyError(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a\n1,x\n1,y\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a\n1,x\n")
	l, _ := source.Open(left)
	r, _ := source.Open(right)
	_, err := diff.Run(l, r, diff.Options{Keys: []string{"id"}})
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
}

func TestMultiColumnKey(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "k1,k2,v\n1,a,10\n1,b,20\n2,a,30\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "k1,k2,v\n1,a,10\n1,b,21\n2,b,30\n")
	res := runDiff(t, left, right, diff.Options{Keys: []string{"k1", "k2"}})
	if res.Changed != 1 || res.Added != 1 || res.Removed != 1 {
		t.Errorf("got +%d -%d ~%d, want +1 -1 ~1", res.Added, res.Removed, res.Changed)
	}
}

func TestIgnoreColumns(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a,noise\n1,10,x\n2,20,y\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a,noise\n1,10,xx\n2,20,yy\n")
	res := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, IgnoreColumns: []string{"noise"}})
	if !res.RowsSame() {
		t.Errorf("ignored column still diffed: %+v", res)
	}
}

func TestMissingKeyColumn(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a\n1,10\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a\n1,10\n")
	l, _ := source.Open(left)
	r, _ := source.Open(right)
	if _, err := diff.Run(l, r, diff.Options{Keys: []string{"nope"}}); err == nil {
		t.Fatal("expected missing key error")
	}
}

func TestExampleLimit(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 2000, Seed: 7, Out: dir, PctChanged: 0.2, PctAdded: 0.1, PctRemoved: 0.1,
		Formats: []string{"csv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := runDiff(t, filepath.Join(dir, "left.csv"), filepath.Join(dir, "right.csv"),
		diff.Options{Keys: []string{"id"}, Limit: 3})
	if len(res.AddedExamples) != 3 || len(res.RemovedExamples) != 3 || len(res.ChangedExamples) != 3 {
		t.Errorf("limit not applied: %d/%d/%d", len(res.AddedExamples), len(res.RemovedExamples), len(res.ChangedExamples))
	}
	if res.Added != man.Added || res.Removed != man.Removed || res.Changed != man.Changed {
		t.Errorf("counts must be exact despite limit: got +%d -%d ~%d", res.Added, res.Removed, res.Changed)
	}
}

func TestQuotedCSVFallback(t *testing.T) {
	dir := t.TempDir()
	// quoted fields with embedded separators, quotes, and newlines
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,s\n1,\"a,b\"\n2,\"say \"\"hi\"\"\"\n3,\"line1\nline2\"\n4,plain\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,s\n1,\"a,b\"\n2,\"say \"\"hi\"\"\"\n3,\"line1\nline2\"\n4,changed\n")
	res := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
	if res.Changed != 1 || res.Added != 0 || res.Removed != 0 {
		t.Errorf("got +%d -%d ~%d, want ~1 only", res.Added, res.Removed, res.Changed)
	}
	same := runDiff(t, left, left, diff.Options{Keys: []string{"id"}})
	if !same.RowsSame() {
		t.Errorf("identical quoted files reported diffs: %+v", same)
	}
}

func TestCRLFAndTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a\r\n1,10\r\n2,20\r\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a\n1,10\n2,20") // no trailing newline
	res := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
	if !res.RowsSame() {
		t.Errorf("CRLF/no-trailing-newline mismatch: %+v", res)
	}
}

func TestSummaryMode(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 4000, Seed: 9, Out: dir, PctChanged: 0.05, PctAdded: 0.01, PctRemoved: 0.01,
		Formats: []string{"csv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := runDiff(t, filepath.Join(dir, "left.csv"), filepath.Join(dir, "right.csv"),
		diff.Options{Keys: []string{"id"}, Summary: true})
	if res.Added != man.Added || res.Removed != man.Removed || res.Changed != man.Changed {
		t.Errorf("summary counts: got +%d -%d ~%d, want +%d -%d ~%d",
			res.Added, res.Removed, res.Changed, man.Added, man.Removed, man.Changed)
	}
	if len(res.ChangedExamples) != 0 || len(res.ColumnChanges) != 0 {
		t.Errorf("summary mode must not attribute: %+v", res)
	}
}
