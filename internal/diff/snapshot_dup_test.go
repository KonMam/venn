package diff

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KonMam/tdiff/internal/source"
)

// Loading a snapshot dense with duplicate key hashes used to deadlock: the
// workers exited on the duplicate error while the reader blocked on the
// buffer/work channels. The load must return the error promptly instead.
func TestAgainstSnapshotDuplicateKeysErrorsPromptly(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "dups.csv")
	var sb strings.Builder
	sb.WriteString("id,v\n")
	// Unique keys with one duplicate planted mid-file: the loader's reader
	// must already be parked on a full channel (pool of 1MB pair chunks far
	// smaller than the file) when the worker dies on the duplicate — that is
	// the interleaving that used to deadlock. An all-duplicate file errors
	// too early to catch it.
	for i := 0; i < 2_000_000; i++ {
		fmt.Fprintf(&sb, "%d,%d\n", i, i)
	}
	sb.WriteString("600000,-1\n") // the duplicate key
	if err := os.WriteFile(csvPath, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	src, err := source.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	snapPath := filepath.Join(dir, "dups.snap")
	if _, err := WriteSnapshot(src, snapPath, Options{Keys: []string{"id"}}); err != nil {
		t.Fatal(err)
	}

	live, err := source.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	done := make(chan error, 1)
	go func() {
		// Threads:1 pins the failure mode: the lone worker dies on the first
		// chunk while the reader is blocked handing over the next one.
		_, derr := DiffAgainstSnapshot(snapPath, live, Options{OnDup: "error", Threads: 1})
		done <- derr
	}()
	select {
	case derr := <-done:
		if derr == nil || !strings.Contains(derr.Error(), "duplicate key hash") {
			t.Fatalf("want duplicate-key error, got %v", derr)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("snapshot load deadlocked on duplicate keys")
	}
}
