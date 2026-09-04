package diff_test

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KonMam/tdiff/internal/diff"
	"github.com/KonMam/tdiff/internal/source"
)

// maskedSink records the exported rows so a test can prove no real value
// reached the writer. WriteDiffRow is called from every scan worker, so the
// sink locks like a real one.
type maskedSink struct {
	mu   sync.Mutex
	rows []string
}

func (m *maskedSink) WriteDiffRow(status byte, key, left, right []source.Value) error {
	var sb strings.Builder
	sb.WriteByte(status)
	for _, group := range [][]source.Value{key, left, right} {
		for i := range group {
			sb.WriteString("|" + group[i].Display())
		}
	}
	m.mu.Lock()
	m.rows = append(m.rows, sb.String())
	m.mu.Unlock()
	return nil
}

func TestMask(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,email,amount\n1,alice@x.com,10\n2,bob@x.com,20\n3,carl@x.com,30\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,email,amount\n1,alice@x.com,10\n2,bob2@x.com,25\n4,dana@x.com,40\n")
	secrets := []string{"alice@x.com", "bob@x.com", "bob2@x.com", "carl@x.com", "dana@x.com"}

	for _, mode := range joinModes {
		got := runDiff(t, left, right, diff.Options{
			Keys: []string{"id"}, Mode: mode, Mask: []string{"email"}, Limit: 100,
		})
		// masking is display-only: the counts must be identical to a plain run
		plain := runDiff(t, left, right, diff.Options{Keys: []string{"id"}, Mode: mode, Limit: 100})
		if got.Added != plain.Added || got.Removed != plain.Removed || got.Changed != plain.Changed {
			t.Fatalf("%s: masking changed the counts: %+v vs %+v", mode, got, plain)
		}
		if got.ColumnChanges["email"] != plain.ColumnChanges["email"] {
			t.Fatalf("%s: masking changed the attribution: %v vs %v",
				mode, got.ColumnChanges, plain.ColumnChanges)
		}
		if len(got.Masked) != 1 || got.Masked[0] != "email" {
			t.Fatalf("%s: masked = %v want [email]", mode, got.Masked)
		}
		// no example may carry a real value
		var shown []string
		for _, ex := range got.ChangedExamples {
			shown = append(shown, ex.Key)
			for _, c := range ex.Columns {
				shown = append(shown, c.Left, c.Right)
			}
		}
		shown = append(shown, got.AddedExamples...)
		shown = append(shown, got.RemovedExamples...)
		joined := strings.Join(shown, " ")
		for _, secret := range secrets {
			if strings.Contains(joined, secret) {
				t.Fatalf("%s: masked value %q leaked into the examples: %s", mode, secret, joined)
			}
		}
		// and the token is stable: the same value gives the same token
		if !strings.Contains(joined, "xxh:") {
			t.Fatalf("%s: no masked token in the examples: %s", mode, joined)
		}
	}
}

func TestMaskExport(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,email,amount\n1,alice@x.com,10\n2,bob@x.com,20\n3,carl@x.com,30\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,email,amount\n1,alice@x.com,10\n2,bob2@x.com,25\n4,dana@x.com,40\n")
	for _, mode := range joinModes {
		sink := &maskedSink{}
		if _, err := diff.Run(mustOpen(t, left), mustOpen(t, right), diff.Options{
			Keys: []string{"id"}, Mode: mode, Mask: []string{"email"}, Sink: sink,
		}); err != nil {
			t.Fatal(err)
		}
		if len(sink.rows) != 3 {
			t.Fatalf("%s: exported %d rows, want 3", mode, len(sink.rows))
		}
		all := strings.Join(sink.rows, "\n")
		for _, secret := range []string{"alice@x.com", "bob@x.com", "bob2@x.com", "carl@x.com", "dana@x.com"} {
			if strings.Contains(all, secret) {
				t.Fatalf("%s: masked value %q reached the export:\n%s", mode, secret, all)
			}
		}
		// the unmasked column still exports its real values
		if !strings.Contains(all, "|25") {
			t.Fatalf("%s: the unmasked amount column is missing from the export:\n%s", mode, all)
		}
	}
}

// TestMaskExportSchema pins that a masked column exports as a string, so the
// token has somewhere to go in a typed writer.
func TestMaskExportSchema(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,amount\n1,10\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,amount\n1,11\n")
	l, r := mustOpen(t, left), mustOpen(t, right)
	_, kt, vn, vt, err := diff.ResolveColumns(l.Schema(), r.Schema(),
		diff.Options{Keys: []string{"id"}, Mask: []string{"amount", "id"}})
	if err != nil {
		t.Fatal(err)
	}
	if kt[0] != source.TypeString {
		t.Fatalf("masked key type = %v, want string", kt[0])
	}
	for i, n := range vn {
		if n == "amount" && vt[i] != source.TypeString {
			t.Fatalf("masked amount type = %v, want string", vt[i])
		}
	}
}

func TestMaskValidation(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,a\n1,1\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,a\n1,1\n")
	for _, mask := range [][]string{{"nope"}, {"a", "nope"}} {
		_, err := diff.Run(mustOpen(t, left), mustOpen(t, right),
			diff.Options{Keys: []string{"id"}, Mask: mask})
		if err == nil || !strings.Contains(err.Error(), "not compared") {
			t.Fatalf("--mask %v: err = %v, want a not-compared error", mask, err)
		}
	}
	// masking an ignored column is a typo, not a no-op
	_, err := diff.Run(mustOpen(t, left), mustOpen(t, right),
		diff.Options{Keys: []string{"id"}, IgnoreColumns: []string{"a"}, Mask: []string{"a"}})
	if err == nil {
		t.Fatal("expected an error for masking an ignored column")
	}
}

// TestMaskTokenStability pins that equal values mask to equal tokens (so
// rows stay correlatable) and different values to different ones.
func TestMaskTokenStability(t *testing.T) {
	dir := t.TempDir()
	// two rows share the same masked value; a third differs
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,s,v\n1,same,1\n2,same,1\n3,other,1\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,s,v\n1,same,2\n2,same,2\n3,other,2\n")
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, Mask: []string{"s"}, Limit: 100,
	})
	if got.Changed != 3 {
		t.Fatalf("changed=%d want 3", got.Changed)
	}
	// s did not change, so it is not in the examples; mask the key instead
	got = runDiff(t, left, right, diff.Options{
		Keys: []string{"s", "id"}, Mask: []string{"s"}, Limit: 100,
	})
	tokens := map[string]bool{}
	for _, ex := range got.ChangedExamples {
		tokens[strings.Split(ex.Key, "|")[0]] = true
	}
	if len(tokens) != 2 {
		t.Fatalf("distinct masked key tokens = %d (%v), want 2", len(tokens), tokens)
	}
}
