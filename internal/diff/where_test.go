package diff_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/KonMam/tdiff/internal/diff"
	"github.com/KonMam/tdiff/internal/source"
)

// filtered opens a path with a --where filter applied, the way the CLI does.
func filtered(t *testing.T, path, spec string) source.Source {
	t.Helper()
	src := mustOpen(t, path)
	preds, err := source.ParseWhere(spec)
	if err != nil {
		t.Fatal(err)
	}
	f, err := source.Filter(src, preds)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestWhereEndToEnd pins the semantics that matters most: the filter is
// applied to both sides before the diff, so the counts are of the filtered
// universe, not of the whole input.
func TestWhereEndToEnd(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,region,price\n1,eu,10\n2,us,20\n3,eu,30\n4,ap,40\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,region,price\n1,eu,10\n2,us,25\n3,eu,31\n4,ap,40\n")

	for _, mode := range joinModes {
		unfiltered := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, Mode: mode})
		if unfiltered.Changed != 2 {
			t.Fatalf("%s unfiltered: changed=%d want 2", mode, unfiltered.Changed)
		}
		res, err := diff.Run(filtered(t, left, "region = eu"), filtered(t, right, "region = eu"),
			diff.Options{Keys: []string{"id"}, Mode: mode, Where: "region = eu", Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if res.LeftRows != 2 || res.RightRows != 2 {
			t.Errorf("%s: rows %d/%d want 2/2; the filter runs before the diff",
				mode, res.LeftRows, res.RightRows)
		}
		if res.Changed != 1 || res.Unchanged != 1 {
			t.Errorf("%s: changed=%d unchanged=%d want 1/1", mode, res.Changed, res.Unchanged)
		}
		if res.Filter != "region = eu" {
			t.Errorf("%s: filter = %q, want it reported", mode, res.Filter)
		}
	}
}

// TestWhereEmptyResult pins that filtering everything out is an ordinary
// "identical" answer, not an error.
func TestWhereEmptyResult(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,11\n2,21\n")
	res, err := diff.Run(filtered(t, left, "v > 1000"), filtered(t, right, "v > 1000"),
		diff.Options{Keys: []string{"id"}, Where: "v > 1000"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.RowsSame() || res.LeftRows != 0 || res.RightRows != 0 {
		t.Fatalf("%+v want an empty identical result", res)
	}
}

// TestWhereAsymmetricSelection pins that a row filtered out on one side only
// is reported as added/removed, not silently dropped.
func TestWhereAsymmetricSelection(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\n1,10\n2,5\n")
	// v >= 10 keeps both left rows but only one right row
	res, err := diff.Run(filtered(t, left, "v >= 10"), filtered(t, right, "v >= 10"),
		diff.Options{Keys: []string{"id"}, Where: "v >= 10", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 1 || res.Unchanged != 1 || res.Added != 0 {
		t.Fatalf("%+v want 1 removed, 1 unchanged", res)
	}
	if len(res.RemovedExamples) != 1 || res.RemovedExamples[0] != "2" {
		t.Fatalf("removed examples = %v want [2]", res.RemovedExamples)
	}
}

// TestWhereWithKeyInference pins that inference judges uniqueness over the
// rows that will actually be compared.
func TestWhereWithKeyInference(t *testing.T) {
	dir := t.TempDir()
	// id repeats overall, but is unique among the rows where keep = 1
	f := writeFile(t, filepath.Join(dir, "a.csv"),
		"id,keep\n1,1\n1,0\n2,1\n2,0\n")
	if _, err := diff.InferKey(mustOpen(t, f), mustOpen(t, f), nil); err == nil {
		t.Fatal("expected inference to fail on the unfiltered input")
	}
	k, err := diff.InferKey(filtered(t, f, "keep = 1"), filtered(t, f, "keep = 1"), nil)
	if err != nil || k != "id" {
		t.Fatalf("inferred %q err=%v, want id", k, err)
	}
}

// TestWhereSnapshotMustMatch pins that a baseline records its filter: a
// filtered snapshot compared against an unfiltered file would report the
// difference in filters as removed rows.
func TestWhereSnapshotMustMatch(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,region,v\n1,eu,10\n2,us,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,region,v\n1,eu,11\n2,us,20\n")
	snap := filepath.Join(dir, "base.snap")
	if _, err := diff.WriteSnapshot(filtered(t, left, "region = eu"), snap,
		diff.Options{Keys: []string{"id"}, Where: "region = eu"}); err != nil {
		t.Fatal(err)
	}
	// same filter: a clean comparison
	got, err := diff.DiffAgainstSnapshot(snap, filtered(t, right, "region = eu"),
		diff.Options{Where: "region = eu"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Changed != 1 || got.LeftRows != 1 {
		t.Fatalf("%+v want 1 changed row out of 1", got)
	}
	// no filter, or a different one: refused
	for _, where := range []string{"", "region = us"} {
		src := mustOpen(t, right)
		if where != "" {
			src = filtered(t, right, where)
		}
		if _, err := diff.DiffAgainstSnapshot(snap, src, diff.Options{Where: where}); err == nil {
			t.Errorf("--where %q against a snapshot filtered by region = eu was accepted", where)
		} else if !strings.Contains(err.Error(), "different rows") {
			t.Errorf("err = %v, want one about covering different rows", err)
		}
	}
}

// TestWhereWithKeyless and the other modes: the filter is a source-layer
// concern, so it composes with every join strategy.
func TestWhereWithKeylessAndDups(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,region,v\nk,eu,1\nk,eu,2\nk,us,9\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,region,v\nk,eu,1\nk,eu,3\nk,us,9\n")

	keyless, err := diff.Run(filtered(t, left, "region = eu"), filtered(t, right, "region = eu"),
		diff.Options{Keyless: true, Where: "region = eu", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if keyless.Unchanged != 1 || keyless.Added != 1 || keyless.Removed != 1 {
		t.Fatalf("keyless: %+v want 1 unchanged, 1 added, 1 removed", keyless)
	}

	dup, err := diff.Run(filtered(t, left, "region = eu"), filtered(t, right, "region = eu"),
		diff.Options{Keys: []string{"id"}, OnDup: "match", Mode: "memory",
			Where: "region = eu", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Unchanged != 1 || dup.Changed != 1 {
		t.Fatalf("on-dup match: %+v want 1 unchanged, 1 changed", dup)
	}
}
