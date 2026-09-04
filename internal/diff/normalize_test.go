package diff_test

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/KonMam/venn/internal/diff"
	"github.com/KonMam/venn/internal/fixture"
)

// modes covers both join strategies: every value-normalization and tolerance
// rule has to hold in the streaming engine too.
var joinModes = []string{"memory", "stream"}

func TestTrimAndIgnoreCase(t *testing.T) {
	dir := t.TempDir()
	// row 1 differs only by case, row 2 only by padding, row 3 by both,
	// row 4 genuinely
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,s\n1,Alice\n2,bob\n3,Carl\n4,dave\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,s\n1,alice\n2,\"  bob \"\n3,\"  CARL\"\n4,erin\n")

	for _, mode := range joinModes {
		base := diff.Options{Keys: []string{"id"}, Mode: mode}
		exact := runDiff(t, left, right, base)
		if exact.Changed != 4 {
			t.Errorf("%s exact: changed=%d want 4", mode, exact.Changed)
		}

		fold := base
		fold.IgnoreCase = true
		if got := runDiff(t, left, right, fold); got.Changed != 3 {
			t.Errorf("%s ignore-case: changed=%d want 3", mode, got.Changed)
		}

		trim := base
		trim.Trim = true
		if got := runDiff(t, left, right, trim); got.Changed != 3 {
			t.Errorf("%s trim: changed=%d want 3", mode, got.Changed)
		}

		both := base
		both.IgnoreCase, both.Trim = true, true
		got := runDiff(t, left, right, both)
		if got.Changed != 1 || got.Unchanged != 3 {
			t.Errorf("%s trim+ignore-case: changed=%d unchanged=%d want 1/3", mode, got.Changed, got.Unchanged)
		}
		// the row that really differs must still be attributed to the column
		if got.ColumnChanges["s"] != 1 {
			t.Errorf("%s trim+ignore-case: column changes = %v want s:1", mode, got.ColumnChanges)
		}
	}
}

// TestNormalizedKeys pins that the normalizations apply to key columns too —
// they are hash-consistent, so they can join rows whose keys differ only by
// case or padding.
func TestNormalizedKeys(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "k,v\nAB,1\n cd,2\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "k,v\nab,1\ncd ,2\n")
	for _, mode := range joinModes {
		opts := diff.Options{Keys: []string{"k"}, Mode: mode}
		if got := runDiff(t, left, right, opts); got.Added != 2 || got.Removed != 2 {
			t.Errorf("%s exact: added=%d removed=%d want 2/2", mode, got.Added, got.Removed)
		}
		opts.IgnoreCase, opts.Trim = true, true
		if got := runDiff(t, left, right, opts); !got.RowsSame() || got.Unchanged != 2 {
			t.Errorf("%s normalized: %+v want 2 unchanged", mode, got)
		}
	}
}

// TestIgnoreCaseNonASCII covers the allocating fold path (the hot path folds
// ASCII in a stack buffer; both must agree).
func TestIgnoreCaseNonASCII(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,s\n1,ÄPFEL\n2,Öl\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,s\n1,äpfel\n2,öl\n")
	for _, mode := range joinModes {
		opts := diff.Options{Keys: []string{"id"}, Mode: mode, IgnoreCase: true}
		if got := runDiff(t, left, right, opts); !got.RowsSame() {
			t.Errorf("%s: %+v want identical", mode, got)
		}
	}
}

func TestTimestampPrecision(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,t\n1,2024-01-01T00:00:00.123456Z\n2,2024-01-01T00:00:01.500000Z\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,t\n1,2024-01-01T00:00:00.123999Z\n2,2024-01-01T00:00:01.900000Z\n")
	for _, mode := range joinModes {
		opts := diff.Options{Keys: []string{"id"}, Mode: mode}
		if got := runDiff(t, left, right, opts); got.Changed != 2 {
			t.Errorf("%s exact: changed=%d want 2", mode, got.Changed)
		}
		opts.TimestampPrecision = "ms"
		if got := runDiff(t, left, right, opts); got.Changed != 1 {
			t.Errorf("%s ms: changed=%d want 1", mode, got.Changed)
		}
		opts.TimestampPrecision = "s"
		if got := runDiff(t, left, right, opts); !got.RowsSame() {
			t.Errorf("%s s: %+v want identical", mode, got)
		}
		opts.TimestampPrecision = "ns"
		if _, err := diff.Run(mustOpen(t, left), mustOpen(t, right), opts); err == nil {
			t.Errorf("%s: expected an error for --timestamp-precision ns", mode)
		}
	}
}

// TestTimestampPrecisionPreEpoch pins that truncation floors rather than
// truncating toward zero, so instants either side of the epoch bucket
// consistently.
func TestTimestampPrecisionPreEpoch(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,t\n1,1969-12-31T23:59:59.100000Z\n2,1969-12-31T23:59:58.100000Z\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,t\n1,1969-12-31T23:59:59.900000Z\n2,1969-12-31T23:59:58.900000Z\n")
	opts := diff.Options{Keys: []string{"id"}, TimestampPrecision: "s"}
	if got := runDiff(t, left, right, opts); !got.RowsSame() {
		t.Fatalf("%+v want identical (both pairs share their second)", got)
	}
	// a pair straddling a second boundary must still differ
	left2 := writeFile(t, filepath.Join(dir, "l2.csv"), "id,t\n1,1969-12-31T23:59:58.900000Z\n")
	right2 := writeFile(t, filepath.Join(dir, "r2.csv"), "id,t\n1,1969-12-31T23:59:59.100000Z\n")
	if got := runDiff(t, left2, right2, opts); got.Changed != 1 {
		t.Fatalf("changed=%d want 1", got.Changed)
	}
}

func TestParseTolerance(t *testing.T) {
	cases := []struct {
		spec    string
		col     string
		abs     float64
		rel     float64
		wantErr bool
	}{
		{spec: "0.01", abs: 0.01},
		{spec: "0.01,rel=1e-6", abs: 0.01, rel: 1e-6},
		{spec: "rel=0.001", rel: 0.001},
		{spec: "abs=2", abs: 2},
		{spec: "price=0.01", col: "price", abs: 0.01},
		{spec: "price=0.01,rel=1e-9", col: "price", abs: 0.01, rel: 1e-9},
		{spec: "price=rel=1e-9", col: "price", rel: 1e-9},
		{spec: "", wantErr: true},
		{spec: "0", wantErr: true},
		{spec: "-1", wantErr: true},
		{spec: "nope", wantErr: true},
		{spec: "price=nope", wantErr: true},
		{spec: "0.1,pct=2", wantErr: true},
		{spec: "=0.1", wantErr: true},
	}
	for _, c := range cases {
		col, tol, err := diff.ParseTolerance(c.spec)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseTolerance(%q) = %v, want an error", c.spec, tol)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseTolerance(%q): %v", c.spec, err)
			continue
		}
		if col != c.col || tol.Abs != c.abs || tol.Rel != c.rel {
			t.Errorf("ParseTolerance(%q) = %q %+v, want %q abs=%v rel=%v",
				c.spec, col, tol, c.col, c.abs, c.rel)
		}
	}
}

func TestToleranceReclassifies(t *testing.T) {
	dir := t.TempDir()
	// row 1: a small absolute difference, relatively large (5e-4)
	// row 2: large either way · row 3: relatively tiny (5e-5) but 0.05 wide
	// row 4: a non-numeric difference, never tolerable
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,price,name\n1,10.000,a\n2,20.00,b\n3,1000.00,c\n4,40.00,d\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,price,name\n1,10.005,a\n2,25.00,b\n3,1000.05,c\n4,40.00,dd\n")

	for _, mode := range joinModes {
		// an absolute bound catches row 1 and not row 3
		abs := diff.Options{Keys: []string{"id"}, Mode: mode, Tolerance: &diff.Tolerance{Abs: 0.01}}
		got := runDiff(t, left, right, abs)
		if got.Changed != 3 || got.WithinTolerance != 1 {
			t.Errorf("%s abs: changed=%d within=%d want 3/1", mode, got.Changed, got.WithinTolerance)
		}
		if got.ColumnChanges["price"] != 2 {
			t.Errorf("%s abs: price changes=%d want 2 (the tolerable one is not a change)",
				mode, got.ColumnChanges["price"])
		}
		// a reclassified row is not an example and not exported
		for _, ex := range got.ChangedExamples {
			if ex.Key == "1" {
				t.Errorf("%s abs: row 1 is within tolerance but appears as an example", mode)
			}
		}

		// a relative bound catches the other one: row 3, not row 1
		rel := diff.Options{Keys: []string{"id"}, Mode: mode, Tolerance: &diff.Tolerance{Rel: 1e-4}}
		got = runDiff(t, left, right, rel)
		if got.Changed != 3 || got.WithinTolerance != 1 {
			t.Errorf("%s rel: changed=%d within=%d want 3/1", mode, got.Changed, got.WithinTolerance)
		}
		for _, ex := range got.ChangedExamples {
			if ex.Key == "3" {
				t.Errorf("%s rel: row 3 is within tolerance but appears as an example", mode)
			}
		}

		// either bound alone is enough, so both together catch both rows
		both := diff.Options{Keys: []string{"id"}, Mode: mode,
			Tolerance: &diff.Tolerance{Abs: 0.01, Rel: 1e-4}}
		got = runDiff(t, left, right, both)
		if got.Changed != 2 || got.WithinTolerance != 2 {
			t.Errorf("%s abs+rel: changed=%d within=%d want 2/2", mode, got.Changed, got.WithinTolerance)
		}
		if got.ColumnChanges["price"] != 1 {
			t.Errorf("%s abs+rel: price changes=%d want 1", mode, got.ColumnChanges["price"])
		}
		// tolerance never turns a difference into "unchanged"
		if got.Unchanged != 0 {
			t.Errorf("%s abs+rel: unchanged=%d want 0", mode, got.Unchanged)
		}
		if got.RowsSame() {
			t.Errorf("%s abs+rel: RowsSame() is true but 2 rows still differ", mode)
		}
	}
}

// TestToleranceAllRowsTolerable pins the exit-code-relevant case: when every
// difference is inside tolerance the rows count as same.
func TestToleranceAllRowsTolerable(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,1.000\n2,2.000\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,1.004\n2,1.996\n")
	for _, mode := range joinModes {
		got := runDiff(t, left, right, diff.Options{
			Keys: []string{"id"}, Mode: mode, Tolerance: &diff.Tolerance{Abs: 0.01},
		})
		if !got.RowsSame() || got.WithinTolerance != 2 {
			t.Errorf("%s: %+v want rows same with 2 within tolerance", mode, got)
		}
		if len(got.ColumnChanges) != 0 {
			t.Errorf("%s: column changes = %v want none", mode, got.ColumnChanges)
		}
	}
}

func TestPerColumnTolerance(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,price,qty\n1,10.000,5.0\n2,20.00,5.0\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,price,qty\n1,10.005,5.0\n2,20.00,5.005\n")
	opts := diff.Options{Keys: []string{"id"},
		ColumnTolerance: map[string]diff.Tolerance{"price": {Abs: 0.01}}}
	got := runDiff(t, left, right, opts)
	if got.Changed != 1 || got.WithinTolerance != 1 {
		t.Fatalf("changed=%d within=%d want 1/1", got.Changed, got.WithinTolerance)
	}
	if got.ColumnChanges["qty"] != 1 || got.ColumnChanges["price"] != 0 {
		t.Fatalf("column changes = %v want qty:1 only", got.ColumnChanges)
	}
}

func TestToleranceRejections(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v,s,t\n1,1.0,a,2024-01-01T00:00:00Z\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v,s,t\n1,1.5,b,2024-01-01T00:00:01Z\n")
	cases := []struct {
		name string
		opts diff.Options
		want string
	}{
		{"summary", diff.Options{Keys: []string{"id"}, Summary: true,
			Tolerance: &diff.Tolerance{Abs: 1}}, "full diff mode"},
		{"unknown column", diff.Options{Keys: []string{"id"},
			ColumnTolerance: map[string]diff.Tolerance{"nope": {Abs: 1}}}, "not compared"},
		{"string column", diff.Options{Keys: []string{"id"},
			ColumnTolerance: map[string]diff.Tolerance{"s": {Abs: 1}}}, "not numeric"},
		{"timestamp column", diff.Options{Keys: []string{"id"},
			ColumnTolerance: map[string]diff.Tolerance{"t": {Abs: 1}}}, "not numeric"},
		{"ignored column", diff.Options{Keys: []string{"id"}, IgnoreColumns: []string{"v"},
			ColumnTolerance: map[string]diff.Tolerance{"v": {Abs: 1}}}, "not compared"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := diff.Run(mustOpen(t, left), mustOpen(t, right), c.opts)
			if err == nil {
				t.Fatalf("expected an error mentioning %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

// TestGlobalToleranceSkipsNonNumeric pins that a bare --tolerance applies to
// the numeric columns and leaves the rest exact, rather than erroring.
func TestGlobalToleranceSkipsNonNumeric(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v,s\n1,1.0,a\n2,1.0,a\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v,s\n1,1.005,a\n2,1.0,b\n")
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, Tolerance: &diff.Tolerance{Abs: 0.01},
	})
	if got.Changed != 1 || got.WithinTolerance != 1 {
		t.Fatalf("changed=%d within=%d want 1/1", got.Changed, got.WithinTolerance)
	}
	if got.ColumnChanges["s"] != 1 {
		t.Fatalf("column changes = %v want s:1", got.ColumnChanges)
	}
}

func TestColumnStats(t *testing.T) {
	dir := t.TempDir()
	// v: 2 of 4 compared rows differ, by 1 and 3 (max 3, mean 2)
	// s: 1 of 4 differs, non-numeric
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,v,s\n1,10,a\n2,10,a\n3,10,a\n4,10,a\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,v,s\n1,11,a\n2,13,a\n3,10,z\n4,10,a\n")
	for _, mode := range joinModes {
		got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, Mode: mode})
		if got.ComparedRows() != 4 {
			t.Fatalf("%s: compared=%d want 4", mode, got.ComparedRows())
		}
		v := got.ColumnStats["v"]
		if v == nil {
			t.Fatalf("%s: no stats for v (%v)", mode, got.ColumnStats)
		}
		if v.Changed != 2 || !v.Numeric || v.MaxAbsDiff != 3 || v.MeanAbsDiff != 2 {
			t.Errorf("%s: v stats = %+v want changed=2 max=3 mean=2 numeric", mode, v)
		}
		if v.MatchRate != 0.5 {
			t.Errorf("%s: v match rate = %v want 0.5", mode, v.MatchRate)
		}
		s := got.ColumnStats["s"]
		if s == nil || s.Changed != 1 || s.Numeric || s.MatchRate != 0.75 {
			t.Errorf("%s: s stats = %+v want changed=1 rate=0.75 non-numeric", mode, s)
		}
	}
}

// TestColumnStatsSkipNulls pins that a NULL on either side counts as a
// change but contributes no magnitude.
func TestColumnStatsSkipNulls(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,10\n2,\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,14\n2,5\n")
	got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
	v := got.ColumnStats["v"]
	if v == nil || v.Changed != 2 {
		t.Fatalf("v stats = %+v want changed=2", v)
	}
	if v.MaxAbsDiff != 4 || v.MeanAbsDiff != 4 {
		t.Fatalf("v stats = %+v want max=mean=4 (the NULL pair has no magnitude)", v)
	}
}

// TestFinishStatsIdempotent pins that the CLI can fold rows skipped as shared
// into the counts and re-derive the rates.
func TestFinishStatsIdempotent(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,10\n2,10\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,11\n2,10\n")
	got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
	if r := got.ColumnStats["v"].MatchRate; r != 0.5 {
		t.Fatalf("match rate = %v want 0.5", r)
	}
	got.Unchanged += 2 // as if two rows had been skipped as shared
	got.FinishStats()
	if r := got.ColumnStats["v"].MatchRate; r != 0.75 {
		t.Fatalf("match rate after folding = %v want 0.75", r)
	}
	got.FinishStats() // twice must not drift
	if r := got.ColumnStats["v"].MatchRate; r != 0.75 {
		t.Fatalf("match rate re-derived = %v want 0.75", r)
	}
}

func TestSnapshotNormalization(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,s\n1,Alice\n2,bob\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,s\n1,alice\n2,bob\n")
	snap := filepath.Join(dir, "base.snap")
	if _, err := diff.WriteSnapshot(mustOpen(t, left), snap,
		diff.Options{Keys: []string{"id"}, IgnoreCase: true}); err != nil {
		t.Fatal(err)
	}
	// the baseline's own normalization is honored
	got, err := diff.DiffAgainstSnapshot(snap, mustOpen(t, right),
		diff.Options{IgnoreCase: true})
	if err != nil {
		t.Fatal(err)
	}
	if !got.RowsSame() {
		t.Errorf("%+v want identical under the snapshot's normalization", got)
	}
	// asking for a different set is refused rather than silently compared
	for _, opts := range []diff.Options{
		{Trim: true},
		{IgnoreCase: true, Trim: true},
		{Tolerance: &diff.Tolerance{Abs: 1}},
	} {
		if _, err := diff.DiffAgainstSnapshot(snap, mustOpen(t, right), opts); err == nil {
			t.Errorf("expected a refusal for %+v", opts)
		}
	}
}

// TestToleranceOracle is the tolerance counterpart of TestManifestOracle:
// the generator plants rows whose only differences are inside
// fixture.ToleranceAbs, and the diff must move exactly those rows — no more,
// no fewer — out of Changed and into WithinTolerance.
func TestToleranceOracle(t *testing.T) {
	for _, variant := range []string{"standard", "wide"} {
		dir := t.TempDir()
		man, err := fixture.Generate(fixture.Config{
			Rows: 5000, Seed: 7, Out: dir, Variant: variant,
			PctChanged: 0.05, PctAdded: 0.01, PctRemoved: 0.01, PctTolerable: 0.05,
		})
		if err != nil {
			t.Fatal(err)
		}
		if man.Tolerable == 0 {
			t.Fatalf("%s: fixture planted no tolerable rows", variant)
		}
		tol := &diff.Tolerance{Abs: man.ToleranceAbs}
		for _, combo := range [][2]string{{"parquet", "parquet"}, {"csv", "csv"}, {"parquet", "csv"}} {
			for _, mode := range joinModes {
				name := fmt.Sprintf("%s/%s-vs-%s/%s", variant, combo[0], combo[1], mode)
				t.Run(name, func(t *testing.T) {
					left := filepath.Join(dir, "left."+combo[0])
					right := filepath.Join(dir, "right."+combo[1])
					base := diff.Options{Keys: []string{"id"}, Limit: 1 << 30, Mode: mode}

					// exact: the tolerable rows are ordinary changed rows
					exact := runDiff(t, left, right, base)
					if exact.Changed != man.Changed+man.Tolerable {
						t.Fatalf("exact: changed=%d want %d", exact.Changed, man.Changed+man.Tolerable)
					}
					if exact.WithinTolerance != 0 {
						t.Fatalf("exact: within=%d want 0", exact.WithinTolerance)
					}

					withTol := base
					withTol.Tolerance = tol
					got := runDiff(t, left, right, withTol)
					if got.Changed != man.Changed || got.WithinTolerance != man.Tolerable {
						t.Fatalf("changed=%d within=%d want %d/%d",
							got.Changed, got.WithinTolerance, man.Changed, man.Tolerable)
					}
					if got.Added != man.Added || got.Removed != man.Removed {
						t.Fatalf("added=%d removed=%d want %d/%d",
							got.Added, got.Removed, man.Added, man.Removed)
					}
					// the reclassified rows contribute no column changes
					for col, want := range man.ColumnChanges {
						if n := got.ColumnChanges[col]; n != want {
							t.Errorf("column %s: got %d changes, want %d", col, n, want)
						}
					}
					for col, n := range got.ColumnChanges {
						if man.ColumnChanges[col] == 0 {
							t.Errorf("column %s: reported %d changes, manifest has none", col, n)
						}
					}
					// and no examples
					tolerable := make(map[string]bool, len(man.TolerableKeys))
					for _, k := range man.TolerableKeys {
						tolerable[strconv.FormatInt(k, 10)] = true
					}
					for _, ex := range got.ChangedExamples {
						if tolerable[ex.Key] {
							t.Errorf("key %s is within tolerance but appears as a changed example", ex.Key)
						}
					}
					if len(got.ChangedExamples) != int(man.Changed) {
						t.Errorf("changed examples: got %d, want %d", len(got.ChangedExamples), man.Changed)
					}
				})
			}
		}
	}
}

// TestComparisonReported pins that a loosened comparison says so: an
// "identical" verdict reached under a tolerance or a normalization must carry
// the settings that produced it.
func TestComparisonReported(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,price,qty,s\n1,1.0,2.0,a\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,price,qty,s\n1,1.0,2.0,a\n")
	cases := []struct {
		name string
		opts diff.Options
		want string
	}{
		{"exact", diff.Options{}, ""},
		{"trim", diff.Options{Trim: true}, "trim"},
		{"fold", diff.Options{IgnoreCase: true}, "ignore-case"},
		{"float", diff.Options{FloatPrecision: 3}, "float precision 3"},
		{"ts", diff.Options{TimestampPrecision: "ms"}, "timestamp precision ms"},
		{"global tolerance", diff.Options{Tolerance: &diff.Tolerance{Abs: 0.01}},
			"tolerance ±0.01"},
		{"relative tolerance", diff.Options{Tolerance: &diff.Tolerance{Rel: 1e-6}},
			"tolerance ±1e-06 relative"},
		{"column tolerance",
			diff.Options{ColumnTolerance: map[string]diff.Tolerance{"price": {Abs: 0.01}}},
			"tolerance ±0.01 on price"},
		{"combined",
			diff.Options{Trim: true, IgnoreCase: true, Tolerance: &diff.Tolerance{Abs: 1}},
			"trim, ignore-case, tolerance ±1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := c.opts
			opts.Keys = []string{"id"}
			got := runDiff(t, left, right, opts)
			if got.Comparison != c.want {
				t.Fatalf("Comparison = %q, want %q", got.Comparison, c.want)
			}
		})
	}
}
