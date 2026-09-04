package diff_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/KonMam/venn/internal/diff"
)

// TestRename pins the point of --rename: the pair becomes one compared
// column instead of one added and one removed column, and the rename is
// reported.
func TestRename(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"customer_id,amount\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"cust_id,amount\n1,10\n2,25\n")

	for _, mode := range joinModes {
		opts := diff.Options{
			Keys: []string{"customer_id"}, Mode: mode,
			Rename: map[string]string{"cust_id": "customer_id"},
		}
		got := runDiff(t, left, right, opts)
		if got.Changed != 1 || got.Unchanged != 1 || got.Added != 0 || got.Removed != 0 {
			t.Fatalf("%s: %+v want 1 changed, 1 unchanged", mode, got)
		}
		if len(got.Schema.Renames) != 1 {
			t.Fatalf("%s: renames = %+v want one", mode, got.Schema.Renames)
		}
		if rn := got.Schema.Renames[0]; rn.Left != "customer_id" || rn.Right != "cust_id" {
			t.Fatalf("%s: rename = %+v", mode, rn)
		}
		// a rename is not a schema difference — the caller declared the
		// columns equivalent
		if !got.Schema.Same() {
			t.Fatalf("%s: schema reported as differing: %+v", mode, got.Schema)
		}
		if len(got.Schema.AddedColumns) != 0 || len(got.Schema.RemovedColumns) != 0 {
			t.Fatalf("%s: renamed pair still reported as added/removed: %+v", mode, got.Schema)
		}
	}

	// without the rename the columns are an added/removed pair, and the key
	// is not even resolvable
	_, err := diff.Run(mustOpen(t, left), mustOpen(t, right),
		diff.Options{Keys: []string{"customer_id"}})
	if err == nil {
		t.Fatal("expected the un-renamed diff to fail on the missing key column")
	}
}

// TestRenameKeyColumnOnly pins that renaming only the key still works, and
// that a renamed non-key column is compared by value.
func TestRenameValueColumn(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,total\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,sum\n1,10\n2,25\n")
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, Rename: map[string]string{"sum": "total"},
	})
	if got.Changed != 1 || got.ColumnChanges["total"] != 1 {
		t.Fatalf("%+v / %v want 1 changed row in total", got, got.ColumnChanges)
	}
}

func TestRenameValidation(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a,b\n1,1,1\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,x,b\n1,1,1\n")
	cases := []struct {
		name   string
		rename map[string]string
		want   string
	}{
		{"unknown right column", map[string]string{"nope": "a"}, "right input has no column"},
		{"unknown left column", map[string]string{"x": "nope"}, "left input has no column"},
		{"collision", map[string]string{"x": "b"}, "already has a column"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := diff.Run(mustOpen(t, left), mustOpen(t, right),
				diff.Options{Keys: []string{"id"}, Rename: c.rename})
			if err == nil {
				t.Fatalf("expected an error mentioning %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not mention %q", err, c.want)
			}
		})
	}
}

// TestRenameIdentityIsANoOp pins that renaming a column to its own name is
// accepted and reported as nothing.
func TestRenameIdentityIsANoOp(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a\n1,1\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a\n1,1\n")
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, Rename: map[string]string{"a": "a"},
	})
	if !got.Same() || len(got.Schema.Renames) != 0 {
		t.Fatalf("%+v want identical with no renames reported", got)
	}
}

// TestRenameInferredKey pins that key inference sees the renamed columns.
func TestRenameInferredKey(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "customer_id,v\n1,1\n2,2\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "cust_id,v\n1,1\n2,2\n")
	k, err := diff.InferKey(mustOpen(t, left), mustOpen(t, right),
		map[string]string{"cust_id": "customer_id"})
	if err != nil || k != "customer_id" {
		t.Fatalf("inferred %q err=%v, want customer_id", k, err)
	}
	// without the rename there is no common id-ish column to infer from
	if k, err := diff.InferKey(mustOpen(t, left), mustOpen(t, right), nil); err == nil && k == "customer_id" {
		t.Fatalf("inferred %q without the rename", k)
	}
}

// TestRenameAgainstSnapshot pins that a live file whose columns were renamed
// can still be compared against a baseline taken under the old names.
func TestRenameAgainstSnapshot(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "customer_id,amount\n1,10\n2,20\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "cust_id,amount\n1,10\n2,25\n")
	snap := filepath.Join(dir, "base.snap")
	if _, err := diff.WriteSnapshot(mustOpen(t, left), snap,
		diff.Options{Keys: []string{"customer_id"}}); err != nil {
		t.Fatal(err)
	}
	// the un-renamed live file no longer matches the baseline's layout
	if _, err := diff.DiffAgainstSnapshot(snap, mustOpen(t, right), diff.Options{}); err == nil {
		t.Fatal("expected a layout mismatch without the rename")
	}
	got, err := diff.DiffAgainstSnapshot(snap, mustOpen(t, right), diff.Options{
		Rename: map[string]string{"cust_id": "customer_id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Changed != 1 || got.Unchanged != 1 {
		t.Fatalf("%+v want 1 changed, 1 unchanged", got)
	}
	if len(got.Schema.Renames) != 1 {
		t.Fatalf("renames = %+v want one", got.Schema.Renames)
	}
}
