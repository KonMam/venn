package tdiff

import (
	"path/filepath"
	"testing"

	"github.com/KonMam/tdiff/internal/fixture"
)

// TestDiffFacade checks that the public API reports what the engine found,
// including the parts a caller cannot reconstruct from the counts alone
// (column attribution and example rows). The inputs are deliberately in
// different formats, since comparing across formats is the point of the tool.
func TestDiffFacade(t *testing.T) {
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: 10000, Seed: 1, Out: dir, Formats: []string{"parquet", "csv"},
		PctChanged: 0.01, PctAdded: 0.005, PctRemoved: 0.005,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := Diff(filepath.Join(dir, "left.parquet"), filepath.Join(dir, "right.csv"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != man.Added || res.Removed != man.Removed || res.Changed != man.Changed {
		t.Errorf("got +%d -%d ~%d, want +%d -%d ~%d",
			res.Added, res.Removed, res.Changed, man.Added, man.Removed, man.Changed)
	}
	if res.Same() || !res.Schema.Same() {
		t.Errorf("Same()=%v Schema.Same()=%v, want false/true", res.Same(), res.Schema.Same())
	}
	if len(res.ColumnChanges) == 0 || len(res.ChangedExamples) == 0 {
		t.Errorf("expected column attribution and changed examples in the public result")
	}
}
