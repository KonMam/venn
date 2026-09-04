package diff_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

func TestOnDupWarn(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,10\n2,20\n2,21\n3,30\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,10\n2,20\n3,31\n3,32\n4,40\n")
	for _, mode := range []string{"memory", "stream"} {
		res := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, OnDup: "warn", Mode: mode})
		if res.Added != 1 || res.Removed != 0 || res.Changed != 1 {
			t.Errorf("%s: got +%d -%d ~%d, want +1 -0 ~1", mode, res.Added, res.Removed, res.Changed)
		}
		if res.DupsLeft != 1 || res.DupsRight != 1 {
			t.Errorf("%s: dups got %d/%d, want 1/1", mode, res.DupsLeft, res.DupsRight)
		}
	}
}

// exportCollector records sink rows for tests.
type exportCollector struct {
	mu     sync.Mutex
	counts map[byte]int
}

func (c *exportCollector) WriteDiffRow(status byte, key, left, right []source.Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[byte]int{}
	}
	c.counts[status]++
	if len(key) == 0 || key[0].Null {
		return fmt.Errorf("export row without key")
	}
	switch status {
	case 'a':
		if left != nil || right == nil {
			return fmt.Errorf("added row sides wrong")
		}
	case 'r':
		if left == nil || right != nil {
			return fmt.Errorf("removed row sides wrong")
		}
	default:
		if left == nil || right == nil {
			return fmt.Errorf("changed row sides wrong")
		}
	}
	return nil
}

func TestExportSink(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 4000, Seed: 21, Out: dir, PctChanged: 0.02, PctAdded: 0.01, PctRemoved: 0.01,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"memory", "stream"} {
		for _, combo := range [][2]string{{"parquet", "parquet"}, {"parquet", "csv"}} {
			c := &exportCollector{}
			res := runDiff(t, filepath.Join(dir, "left."+combo[0]), filepath.Join(dir, "right."+combo[1]),
				diff.Options{Keys: []string{"id"}, Mode: mode, Sink: c})
			if int64(c.counts['a']) != man.Added || int64(c.counts['r']) != man.Removed || int64(c.counts['c']) != man.Changed {
				t.Errorf("%s/%v: export rows a=%d r=%d c=%d, want %d/%d/%d",
					mode, combo, c.counts['a'], c.counts['r'], c.counts['c'], man.Added, man.Removed, man.Changed)
			}
			_ = res
		}
	}
}

func TestFloatPrecision(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,1.0001\n2,2.5\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,1.0002\n2,2.5000001\n")
	for _, mode := range []string{"memory", "stream"} {
		exact := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, Mode: mode})
		if exact.Changed != 2 {
			t.Errorf("%s exact: changed=%d want 2", mode, exact.Changed)
		}
		p3 := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, Mode: mode, FloatPrecision: 3})
		if !p3.RowsSame() {
			t.Errorf("%s precision 3: %+v want identical", mode, p3)
		}
		p4 := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, Mode: mode, FloatPrecision: 4})
		if p4.Changed != 1 {
			t.Errorf("%s precision 4: changed=%d want 1", mode, p4.Changed)
		}
	}
}

func TestInferKey(t *testing.T) {
	dir := t.TempDir()
	// v is also unique but id must win on naming; s repeats
	left := writeFile(t, filepath.Join(dir, "l.csv"), "s,v,id\nx,10,1\nx,20,2\ny,30,3\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "s,v,id\nx,10,1\nx,21,2\ny,30,3\n")
	l, _ := source.Open(left)
	r, _ := source.Open(right)
	k, err := diff.InferKey(l, r)
	if err != nil || k != "id" {
		t.Fatalf("inferred %q err=%v, want id", k, err)
	}
	// no unique column at all
	left2 := writeFile(t, filepath.Join(dir, "l2.csv"), "a,b\n1,1\n1,1\n")
	l2, _ := source.Open(left2)
	if _, err := diff.InferKey(l2, l2); err == nil {
		t.Fatal("expected inference failure")
	}
}

func TestSnapshot(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 3000, Seed: 33, Out: dir, PctChanged: 0.03, PctAdded: 0.01, PctRemoved: 0.01,
	})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := source.Open(filepath.Join(dir, "left.parquet"))
	defer l.Close()
	snap := filepath.Join(dir, "base.snap")
	rows, err := diff.WriteSnapshot(l, snap, diff.Options{Keys: []string{"id"}})
	if err != nil || rows != man.RowsLeft {
		t.Fatalf("snapshot: rows=%d err=%v", rows, err)
	}
	// cross-format: live CSV against a parquet-built snapshot
	r, _ := source.Open(filepath.Join(dir, "right.csv"))
	defer r.Close()
	res, err := diff.DiffAgainstSnapshot(snap, r, diff.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != man.Added || res.Removed != man.Removed || res.Changed != man.Changed {
		t.Errorf("got +%d -%d ~%d, want +%d -%d ~%d",
			res.Added, res.Removed, res.Changed, man.Added, man.Removed, man.Changed)
	}
	// identical file matches its own snapshot
	l2, _ := source.Open(filepath.Join(dir, "left.csv"))
	defer l2.Close()
	same, err := diff.DiffAgainstSnapshot(snap, l2, diff.Options{})
	if err != nil || !same.RowsSame() {
		t.Errorf("self-snapshot diff not identical: %+v err=%v", same, err)
	}
}

// TestMultiFileDataset splits a fixture into a partitioned directory layout
// and diffs directory-vs-file and directory-vs-directory.
func TestMultiFileDataset(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 6000, Seed: 44, Out: dir, PctChanged: 0.02, PctAdded: 0.01, PctRemoved: 0.01,
	})
	if err != nil {
		t.Fatal(err)
	}
	// split left.csv into a hive-partitioned dataset (3 shards by id%3),
	// dropping the id column into... keeping all columns, partition = shard
	lines, _ := os.ReadFile(filepath.Join(dir, "left.csv"))
	rows := strings.Split(strings.TrimSpace(string(lines)), "\n")
	header, body := rows[0], rows[1:]
	for shard := 0; shard < 3; shard++ {
		p := filepath.Join(dir, "leftds", fmt.Sprintf("shard=%d", shard))
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		b.WriteString(header + "\n")
		for i, r := range body {
			if i%3 == shard {
				b.WriteString(r + "\n")
			}
		}
		if err := os.WriteFile(filepath.Join(p, "part-0.csv"), []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// dir vs the original single right file: partition col only on the left
	// side is reported in the schema diff but must not break the row diff
	res := runDiff(t, filepath.Join(dir, "leftds"), filepath.Join(dir, "right.parquet"),
		diff.Options{Keys: []string{"id"}})
	if res.Added != man.Added || res.Removed != man.Removed || res.Changed != man.Changed {
		t.Errorf("dir vs file: got +%d -%d ~%d, want +%d -%d ~%d",
			res.Added, res.Removed, res.Changed, man.Added, man.Removed, man.Changed)
	}
	// dir vs identical dir
	same := runDiff(t, filepath.Join(dir, "leftds"), filepath.Join(dir, "leftds"),
		diff.Options{Keys: []string{"id"}})
	if !same.Same() {
		t.Errorf("dir self-diff not identical: %+v", same)
	}
	// glob
	resG := runDiff(t, filepath.Join(dir, "leftds", "shard=*", "*.csv"), filepath.Join(dir, "right.parquet"),
		diff.Options{Keys: []string{"id"}})
	if resG.Added != man.Added || resG.Removed != man.Removed || resG.Changed != man.Changed {
		t.Errorf("glob: got +%d -%d ~%d", resG.Added, resG.Removed, resG.Changed)
	}
}

// TestTableFormats diffs real Iceberg (pyiceberg-written) and Delta
// (delta-rs-written) tables across versions/snapshots. Regenerate fixtures
// with testdata/tables/gen_tables.py.
func TestTableFormats(t *testing.T) {
	base := "../../testdata/tables"
	if _, err := os.Stat(filepath.Join(base, "delta_orders", "_delta_log")); err != nil {
		t.Skip("table fixtures not generated")
	}
	check := func(name string, res *diff.Result) {
		t.Helper()
		if res.Added != 50 || res.Removed != 3 || res.Changed != 3 {
			t.Errorf("%s: got +%d -%d ~%d, want +50 -3 ~3", name, res.Added, res.Removed, res.Changed)
		}
		if res.ColumnChanges["amount"] != 3 {
			t.Errorf("%s: amount changes = %d, want 3", name, res.ColumnChanges["amount"])
		}
	}
	check("delta", runDiff(t, filepath.Join(base, "delta_orders")+"#0",
		filepath.Join(base, "delta_orders")+"#1", diff.Options{Keys: []string{"id"}}))

	snapsRaw, err := os.ReadFile(filepath.Join(base, "iceberg_snapshots.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// snapshots: [0]=v1 append, [1]+[2]=overwrite to v2, [3]/[4]=appends
	snaps := strings.Fields(string(snapsRaw))
	orders := filepath.Join(base, "iceberg_wh/db/orders")
	check("iceberg", runDiff(t, orders+"#"+snaps[0], orders+"#"+snaps[2],
		diff.Options{Keys: []string{"id"}}))

	// v2 against a plain CSV of the same logical content
	res := runDiff(t, filepath.Join(base, "delta_orders")+"#1", filepath.Join(base, "v2.csv"),
		diff.Options{Keys: []string{"id"}})
	if !res.RowsSame() {
		t.Errorf("delta v1 vs csv: %+v want identical rows", res)
	}
}

// TestMergeOnRead: tables whose snapshots carry delete files must diff on
// their live rows. Fixtures: real pyiceberg/delta-rs tables plus
// spec-conformant delete files validated by independent readers (pyiceberg
// for Iceberg position deletes, DuckDB delta_scan for deletion vectors).
func TestMergeOnRead(t *testing.T) {
	base := "../../testdata/tables"
	if _, err := os.Stat(filepath.Join(base, "iceberg_mor")); err != nil {
		t.Skip("merge-on-read fixtures not generated")
	}
	snaps := func(name string) []string {
		raw, err := os.ReadFile(filepath.Join(base, name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.Fields(string(raw))
	}

	t.Run("iceberg-position-deletes", func(t *testing.T) {
		s := snaps("iceberg_mor_snapshots.txt")
		orders := filepath.Join(base, "iceberg_mor/db/orders")
		res := runDiff(t, orders+"#"+s[0], orders+"#"+s[1], diff.Options{Keys: []string{"id"}})
		if res.Added != 0 || res.Removed != 3 || res.Changed != 0 {
			t.Errorf("got +%d -%d ~%d, want +0 -3 ~0", res.Added, res.Removed, res.Changed)
		}
		if res.RightRows != 997 {
			t.Errorf("right rows %d, want 997", res.RightRows)
		}
	})

	t.Run("iceberg-equality-deletes", func(t *testing.T) {
		s := snaps("iceberg_eqdel_snapshots.txt")
		orders := filepath.Join(base, "iceberg_eqdel/db/orders")
		res := runDiff(t, orders+"#"+s[0], orders+"#"+s[1], diff.Options{Keys: []string{"id"}})
		// eq-delete of {10,20}, re-add of 10 with new amount, add of 2000:
		// the delete must not touch the new data file (same sequence number)
		if res.Added != 1 || res.Removed != 1 || res.Changed != 1 {
			t.Errorf("got +%d -%d ~%d, want +1 -1 ~1", res.Added, res.Removed, res.Changed)
		}
		if res.ColumnChanges["amount"] != 1 {
			t.Errorf("amount changes = %d, want 1", res.ColumnChanges["amount"])
		}
	})

	t.Run("delta-deletion-vector", func(t *testing.T) {
		dv := filepath.Join(base, "delta_dv")
		res := runDiff(t, dv+"#0", dv+"#1", diff.Options{Keys: []string{"id"}})
		if res.Added != 0 || res.Removed != 3 || res.Changed != 0 {
			t.Errorf("got +%d -%d ~%d, want +0 -3 ~0", res.Added, res.Removed, res.Changed)
		}
		if res.RightRows != 997 {
			t.Errorf("right rows %d, want 997", res.RightRows)
		}
	})

	// same snapshot on both sides must report zero differences (delete
	// filtering must be deterministic across scans)
	t.Run("mor-self-diff", func(t *testing.T) {
		s := snaps("iceberg_eqdel_snapshots.txt")
		orders := filepath.Join(base, "iceberg_eqdel/db/orders")
		res := runDiff(t, orders+"#"+s[1], orders+"#"+s[1], diff.Options{Keys: []string{"id"}})
		if !res.RowsSame() || res.LeftRows != 1000 {
			t.Errorf("self diff: %+v (left rows %d, want 1000: 1000-2 deleted+2 added)", res, res.LeftRows)
		}
	})
}

// TestSnapshotPruning: diffing two snapshots of the same table must skip the
// data files live in both, and the pruned run must report exactly what a
// full scan reports once the skipped rows are folded back in.
func TestSnapshotPruning(t *testing.T) {
	base := "../../testdata/tables"
	if _, err := os.Stat(filepath.Join(base, "delta_orders", "_delta_log")); err != nil {
		t.Skip("table fixtures not generated")
	}
	snapsRaw, err := os.ReadFile(filepath.Join(base, "iceberg_snapshots.txt"))
	if err != nil {
		t.Fatal(err)
	}
	snaps := strings.Fields(string(snapsRaw))
	orders := filepath.Join(base, "iceberg_wh/db/orders")
	cases := []struct {
		name, left, right string
	}{
		// append → append: earlier files shared by both snapshots
		{"iceberg-append", orders + "#" + snaps[3], orders + "#" + snaps[4]},
		{"delta-append", filepath.Join(base, "delta_orders") + "#1", filepath.Join(base, "delta_orders") + "#2"},
		// identical snapshots: everything shared, one file kept per side
		{"iceberg-same", orders + "#" + snaps[len(snaps)-1], orders + "#" + snaps[len(snaps)-1]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left, right, pair, err := source.OpenPair(tc.left, tc.right, source.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer left.Close()
			defer right.Close()
			if pair.SharedFiles == 0 || pair.SharedRows == 0 {
				t.Fatalf("no files pruned: %+v", pair)
			}
			res, err := diff.Run(left, right, diff.Options{Keys: []string{"id"}})
			if err != nil {
				t.Fatal(err)
			}
			full := runDiff(t, tc.left, tc.right, diff.Options{Keys: []string{"id"}})
			if res.Added != full.Added || res.Removed != full.Removed || res.Changed != full.Changed {
				t.Errorf("pruned +%d -%d ~%d != full +%d -%d ~%d",
					res.Added, res.Removed, res.Changed, full.Added, full.Removed, full.Changed)
			}
			if got, want := res.Unchanged+pair.SharedRows, full.Unchanged; got != want {
				t.Errorf("unchanged: pruned %d + shared %d = %d, full scan %d",
					res.Unchanged, pair.SharedRows, got, want)
			}
			if got, want := res.LeftRows+pair.SharedRows, full.LeftRows; got != want {
				t.Errorf("left rows: %d != %d", got, want)
			}
			if got, want := res.RightRows+pair.SharedRows, full.RightRows; got != want {
				t.Errorf("right rows: %d != %d", got, want)
			}
		})
	}
}
