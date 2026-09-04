package diff_test

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/KonMam/venn/internal/diff"
	"github.com/KonMam/venn/internal/fixture"
)

func TestParseBudget(t *testing.T) {
	cases := []struct {
		spec    string
		rows    int64
		want    int64
		wantErr bool
	}{
		{spec: "0", want: 0},
		{spec: "1000", want: 1000},
		{spec: "0.5%", rows: 1000, want: 5},
		{spec: "100%", rows: 1000, want: 1000},
		{spec: "0%", rows: 1000, want: 0},
		{spec: "-1", wantErr: true},
		{spec: "-1%", wantErr: true},
		{spec: "abc", wantErr: true},
		{spec: "abc%", wantErr: true},
		{spec: "", wantErr: true},
	}
	for _, c := range cases {
		got, err := diff.ParseBudget(c.spec, c.rows)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseBudget(%q, %d) = %d, want an error", c.spec, c.rows, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("ParseBudget(%q, %d) = %d, %v; want %d", c.spec, c.rows, got, err, c.want)
		}
	}
}

// TestMaxDiffAbort pins the early-abort contract: a blown budget stops the
// run, the result says so, and the counts are marked as lower bounds rather
// than passed off as totals.
func TestMaxDiffAbort(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 20000, Seed: 3, Out: dir, Formats: []string{"parquet"},
		PctChanged: 0.2, PctAdded: 0.05, PctRemoved: 0.05,
	})
	if err != nil {
		t.Fatal(err)
	}
	left := filepath.Join(dir, "left.parquet")
	right := filepath.Join(dir, "right.parquet")
	total := man.Added + man.Removed + man.Changed

	t.Run("count budget aborts", func(t *testing.T) {
		got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, MaxDiff: "10"})
		if !got.Aborted || !got.PartialCounts {
			t.Fatalf("aborted=%v partial=%v want both true", got.Aborted, got.PartialCounts)
		}
		if got.Same() || got.RowsSame() {
			t.Fatal("an aborted run must never report the inputs as same")
		}
		// the counts are lower bounds — never above the truth
		if got.Added > man.Added || got.Changed > man.Changed {
			t.Fatalf("partial counts exceed the truth: %d/%d vs %d/%d",
				got.Added, got.Changed, man.Added, man.Changed)
		}
		if got.Added+got.Changed < 10 {
			t.Fatalf("aborted at %d differing rows, below the budget of 10", got.Added+got.Changed)
		}
		// attribution and examples are not presented for a partial run
		if len(got.ColumnChanges) != 0 || len(got.ChangedExamples) != 0 {
			t.Fatalf("partial run reported attribution: %v / %d examples",
				got.ColumnChanges, len(got.ChangedExamples))
		}
		if !strings.Contains(got.AbortReason, "budget of 10") {
			t.Fatalf("abort reason = %q", got.AbortReason)
		}
	})

	t.Run("percent budget aborts with a known row count", func(t *testing.T) {
		// parquet carries its row count, so the denominator is final up front
		got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, MaxDiff: "0.1%"})
		if !got.Aborted {
			t.Fatalf("%+v want an abort", got)
		}
	})

	t.Run("budget within reach runs to completion", func(t *testing.T) {
		got := runDiff(t, left, right, diff.Options{
			Keys: []string{"id"}, MaxDiff: strconv.FormatInt(total, 10),
		})
		if got.Aborted {
			t.Fatalf("aborted on a budget that was never exceeded: %s", got.AbortReason)
		}
		if got.Added != man.Added || got.Removed != man.Removed || got.Changed != man.Changed {
			t.Fatalf("counts +%d -%d ~%d want +%d -%d ~%d",
				got.Added, got.Removed, got.Changed, man.Added, man.Removed, man.Changed)
		}
	})

	t.Run("stream mode stops exactly", func(t *testing.T) {
		got := runDiff(t, left, right, diff.Options{
			Keys: []string{"id"}, Mode: "stream", MaxDiff: "10",
		})
		if !got.Aborted || got.PartialCounts {
			t.Fatalf("aborted=%v partial=%v want aborted with exact counts",
				got.Aborted, got.PartialCounts)
		}
		// pass A finished, so the counts are the real ones
		if got.Added != man.Added || got.Removed != man.Removed || got.Changed != man.Changed {
			t.Fatalf("counts +%d -%d ~%d want +%d -%d ~%d",
				got.Added, got.Removed, got.Changed, man.Added, man.Removed, man.Changed)
		}
	})

	t.Run("no budget never aborts", func(t *testing.T) {
		got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}})
		if got.Aborted {
			t.Fatalf("aborted without a budget: %s", got.AbortReason)
		}
	})
}

// TestMaxDiffPercentUnknownRowsRunsFully pins the precision rule: a CSV right
// side has no up-front row count, so the percentage denominator is not final
// and no abort is provable — the run stays exact.
func TestMaxDiffPercentUnknownRowsRunsFully(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 5000, Seed: 4, Out: dir, Formats: []string{"csv"},
		PctChanged: 0.5, PctAdded: 0.05, PctRemoved: 0.05,
	})
	if err != nil {
		t.Fatal(err)
	}
	left := filepath.Join(dir, "left.csv")
	right := filepath.Join(dir, "right.csv")
	got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, MaxDiff: "0.1%"})
	if got.Aborted {
		t.Fatalf("aborted on an unknown-denominator percentage: %s", got.AbortReason)
	}
	if got.Changed != man.Changed || got.Added != man.Added || got.Removed != man.Removed {
		t.Fatalf("counts +%d -%d ~%d want +%d -%d ~%d",
			got.Added, got.Removed, got.Changed, man.Added, man.Removed, man.Changed)
	}
	// a count budget on the same inputs does abort
	if got := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, MaxDiff: "5"}); !got.Aborted {
		t.Fatalf("%+v want an abort on the count budget", got)
	}
}

// TestMaxDiffDoesNotAbortAnExport pins that --output wins: half an export
// file is worse than a slower run.
func TestMaxDiffDoesNotAbortAnExport(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 5000, Seed: 5, Out: dir, Formats: []string{"parquet"},
		PctChanged: 0.3, PctAdded: 0.05, PctRemoved: 0.05,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &exportCollector{}
	res, err := diff.Run(
		mustOpen(t, filepath.Join(dir, "left.parquet")),
		mustOpen(t, filepath.Join(dir, "right.parquet")),
		diff.Options{Keys: []string{"id"}, MaxDiff: "1", Sink: sink},
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.Aborted {
		t.Fatalf("aborted while exporting: %s", res.AbortReason)
	}
	got := int64(sink.counts['a'] + sink.counts['r'] + sink.counts['c'])
	if want := man.Added + man.Removed + man.Changed; got != want {
		t.Fatalf("exported %d rows, want the complete %d", got, want)
	}
}
