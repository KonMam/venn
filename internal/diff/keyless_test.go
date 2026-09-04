package diff_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KonMam/tdiff/internal/diff"
	"github.com/KonMam/tdiff/internal/fixture"
)

func TestKeyless(t *testing.T) {
	dir := t.TempDir()
	// left  {(1,A)x2, (2,B), (3,C)}
	// right {(1,A)x3, (2,B), (4,D)}
	left := writeFile(t, filepath.Join(dir, "l.csv"), "a,b\n1,A\n1,A\n2,B\n3,C\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "a,b\n1,A\n1,A\n1,A\n2,B\n4,D\n")
	got := runDiff(t, left, right, diff.Options{Keyless: true, Limit: 100})
	if got.Unchanged != 3 || got.Added != 2 || got.Removed != 1 {
		t.Fatalf("%+v want 3 unchanged, 2 added, 1 removed", got)
	}
	if got.Changed != 0 {
		t.Fatalf("changed=%d — a keyless diff has no pairing to call changed", got.Changed)
	}
	if !got.Keyless {
		t.Error("result is not marked keyless")
	}
	if len(got.ColumnChanges) != 0 || len(got.ColumnStats) != 0 {
		t.Errorf("keyless diff reported attribution: %v / %v", got.ColumnChanges, got.ColumnStats)
	}
	// examples name whole rows, with the right multiplicity
	assertKeyBag(t, "added", got.AddedExamples, []string{"1|A", "4|D"})
	assertKeyBag(t, "removed", got.RemovedExamples, []string{"3|C"})
}

// TestKeylessIdentical pins the CI hot path.
func TestKeylessIdentical(t *testing.T) {
	dir := t.TempDir()
	f := writeFile(t, filepath.Join(dir, "a.csv"), "a,b\n1,A\n1,A\n2,B\n")
	got := runDiff(t, f, f, diff.Options{Keyless: true})
	if !got.Same() || got.Unchanged != 3 {
		t.Fatalf("%+v want identical with 3 unchanged", got)
	}
}

// TestKeylessOrderIndependent pins that a keyless diff ignores row order —
// the whole point of matching as a multiset.
func TestKeylessOrderIndependent(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "a,b\n1,A\n2,B\n3,C\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "a,b\n3,C\n1,A\n2,B\n")
	got := runDiff(t, left, right, diff.Options{Keyless: true})
	if !got.RowsSame() {
		t.Fatalf("%+v want identical: the same rows in a different order", got)
	}
}

// TestKeylessMultiplicity pins that repeated rows are counted, not
// deduplicated: three copies on the left and one on the right is two removed.
func TestKeylessMultiplicity(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "a\nX\nX\nX\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "a\nX\n")
	got := runDiff(t, left, right, diff.Options{Keyless: true, Limit: 100})
	if got.Unchanged != 1 || got.Removed != 2 || got.Added != 0 {
		t.Fatalf("%+v want 1 unchanged, 2 removed", got)
	}
	if len(got.RemovedExamples) != 2 {
		t.Fatalf("%d removed examples, want 2", len(got.RemovedExamples))
	}
}

func TestKeylessExport(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "a,b\n1,A\n2,B\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "a,b\n1,A\n3,C\n")
	sink := &statusCollector{}
	if _, err := diff.Run(mustOpen(t, left), mustOpen(t, right),
		diff.Options{Keyless: true, Sink: sink}); err != nil {
		t.Fatal(err)
	}
	if sink.counts['a'] != 1 || sink.counts['r'] != 1 || sink.counts['c'] != 0 {
		t.Fatalf("exported a=%d r=%d c=%d want 1/1/0", sink.counts['a'], sink.counts['r'], sink.counts['c'])
	}
}

func TestKeylessIgnoreColumns(t *testing.T) {
	dir := t.TempDir()
	// the rows differ only in the ignored column
	left := writeFile(t, filepath.Join(dir, "l.csv"), "a,ts\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "a,ts\n1,11\n2,21\n")
	if got := runDiff(t, left, right, diff.Options{Keyless: true}); got.Added != 2 {
		t.Fatalf("%+v want 2 added without the ignore", got)
	}
	got := runDiff(t, left, right, diff.Options{Keyless: true, IgnoreColumns: []string{"ts"}})
	if !got.RowsSame() {
		t.Fatalf("%+v want identical once ts is ignored", got)
	}
}

// TestKeylessNormalization pins that the hash-consistent knobs work here
// too: they are per-value canonicalizations, so they need no pairing.
func TestKeylessNormalization(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "a,v\nAlice,1.0001\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "a,v\n alice ,1.0002\n")
	if got := runDiff(t, left, right, diff.Options{Keyless: true}); got.Added != 1 {
		t.Fatalf("%+v want 1 added exactly", got)
	}
	got := runDiff(t, left, right, diff.Options{
		Keyless: true, Trim: true, IgnoreCase: true, FloatPrecision: 3,
	})
	if !got.RowsSame() {
		t.Fatalf("%+v want identical under normalization", got)
	}
}

func TestKeylessRejections(t *testing.T) {
	dir := t.TempDir()
	f := writeFile(t, filepath.Join(dir, "a.csv"), "a\n1\n")
	cases := []struct {
		name string
		opts diff.Options
		want string
	}{
		{"with key", diff.Options{Keyless: true, Keys: []string{"a"}}, "mutually exclusive"},
		{"with tolerance", diff.Options{Keyless: true, Tolerance: &diff.Tolerance{Abs: 1}}, "pairing"},
		{"with on-dup", diff.Options{Keyless: true, OnDup: "match"}, "no meaning"},
		{"with stream", diff.Options{Keyless: true, Mode: "stream"}, "in-memory join"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := diff.Run(mustOpen(t, f), mustOpen(t, f), c.opts)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want one mentioning %q", err, c.want)
			}
		})
	}
	// no comparable column at all
	g := writeFile(t, filepath.Join(dir, "b.csv"), "z\n1\n")
	if _, err := diff.Run(mustOpen(t, f), mustOpen(t, g), diff.Options{Keyless: true}); err == nil {
		t.Fatal("expected an error when the inputs share no comparable column")
	}
}

// TestKeylessAgainstReference checks random datasets against the obvious
// multiset computation, the same way the duplicate-key rule is checked.
func TestKeylessAgainstReference(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			lRows, rRows := randomDupRows(seed)
			left := writeFile(t, filepath.Join(dir, "l.csv"), csvOf(lRows))
			right := writeFile(t, filepath.Join(dir, "r.csv"), csvOf(rRows))

			// the reference: cancel identical whole rows, count the rest
			count := func(rows []dupRow) map[dupRow]int {
				m := map[dupRow]int{}
				for _, r := range rows {
					m[r]++
				}
				return m
			}
			lc, rc := count(lRows), count(rRows)
			var unchanged, added, removed int64
			for row, n := range lc {
				c := min(n, rc[row])
				unchanged += int64(c)
				removed += int64(n - c)
			}
			for row, n := range rc {
				added += int64(n - min(n, lc[row]))
			}

			got := runDiff(t, left, right, diff.Options{Keyless: true, Limit: 1 << 20})
			if got.Unchanged != unchanged || got.Added != added || got.Removed != removed {
				t.Fatalf("+%d -%d =%d, want +%d -%d =%d\nleft=%v\nright=%v",
					got.Added, got.Removed, got.Unchanged, added, removed, unchanged, lRows, rRows)
			}
			if got.Changed != 0 {
				t.Fatalf("changed=%d, want 0", got.Changed)
			}
			if int64(len(got.AddedExamples)) != added || int64(len(got.RemovedExamples)) != removed {
				t.Fatalf("examples %d added / %d removed for counts %d / %d",
					len(got.AddedExamples), len(got.RemovedExamples), added, removed)
			}
		})
	}
}

// TestKeylessOnFixtures runs the keyless diff over the generated fixture
// pairs. Every planted change rewrites a row, so a changed row shows up as
// one added and one removed — which is the whole semantic difference from a
// keyed diff, stated as an assertion.
func TestKeylessOnFixtures(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 5000, Seed: 11, Out: dir,
		PctChanged: 0.05, PctAdded: 0.01, PctRemoved: 0.01,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, combo := range [][2]string{{"parquet", "parquet"}, {"csv", "csv"}, {"parquet", "csv"}} {
		t.Run(combo[0]+"-vs-"+combo[1], func(t *testing.T) {
			got := runDiff(t,
				filepath.Join(dir, "left."+combo[0]),
				filepath.Join(dir, "right."+combo[1]),
				diff.Options{Keyless: true, Limit: 1 << 20})
			if got.Changed != 0 {
				t.Fatalf("changed=%d, want 0", got.Changed)
			}
			if want := man.Added + man.Changed; got.Added != want {
				t.Errorf("added=%d want %d (added rows plus the rewritten ones)", got.Added, want)
			}
			if want := man.Removed + man.Changed; got.Removed != want {
				t.Errorf("removed=%d want %d (removed rows plus the originals)", got.Removed, want)
			}
			if want := man.RowsLeft - man.Removed - man.Changed; got.Unchanged != want {
				t.Errorf("unchanged=%d want %d", got.Unchanged, want)
			}
		})
	}
}
