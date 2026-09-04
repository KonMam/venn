package source

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// scanRowCount runs the parallel batch scan and returns the total row count.
func scanRowCount(t *testing.T, src Source) int64 {
	t.Helper()
	bs, ok := src.(BatchScanner)
	if !ok {
		t.Fatalf("%T does not implement BatchScanner", src)
	}
	var total atomic.Int64
	err := bs.ScanBatches(4, func() (BatchFunc, error) {
		return func(b *Batch) error {
			total.Add(int64(b.N))
			return nil
		}, nil
	})
	if err != nil {
		t.Fatalf("ScanBatches: %v", err)
	}
	return total.Load()
}

// A line longer than the pooled buffer slack (4096 bytes) that straddles a
// block boundary used to panic the parallel feeders: the leftover carried
// between blocks could exceed the slack, and the next refill sliced past the
// buffer's capacity. Regression for that overrun in both text fast paths.
func TestCSVLongLineAcrossBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "long.csv")

	var buf bytes.Buffer
	buf.WriteString("id,payload\n")
	rows := 0
	// Fill to ~6KB before the 1MB boundary with short rows.
	for buf.Len() < csvBlockSize-6*1024 {
		fmt.Fprintf(&buf, "%d,x\n", rows)
		rows++
	}
	// One 8KB row straddling the boundary.
	fmt.Fprintf(&buf, "%d,%s\n", rows, strings.Repeat("y", 8*1024))
	rows++
	// And enough rows after it to force several more blocks.
	for buf.Len() < 3*csvBlockSize {
		fmt.Fprintf(&buf, "%d,x\n", rows)
		rows++
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	src, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if got := scanRowCount(t, src); got != int64(rows) {
		t.Fatalf("row count = %d, want %d", got, rows)
	}
}

func TestNDJSONLongLineAcrossBlockBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "long.ndjson")

	var buf bytes.Buffer
	rows := 0
	for buf.Len() < ndjsonBlockSize-6*1024 {
		fmt.Fprintf(&buf, `{"id":%d,"payload":"x"}`+"\n", rows)
		rows++
	}
	fmt.Fprintf(&buf, `{"id":%d,"payload":"%s"}`+"\n", rows, strings.Repeat("y", 8*1024))
	rows++
	for buf.Len() < 3*ndjsonBlockSize {
		fmt.Fprintf(&buf, `{"id":%d,"payload":"x"}`+"\n", rows)
		rows++
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	src, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if got := scanRowCount(t, src); got != int64(rows) {
		t.Fatalf("row count = %d, want %d", got, rows)
	}
}
