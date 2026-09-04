package source

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// writeDeltaWithCheckpoint builds a minimal Delta table whose live file set
// comes entirely from a version-0 checkpoint parquet: an empty commit JSON
// (so the log has a latest version) plus a checkpoint of nAdds add actions.
// Returns the checkpoint path.
func writeDeltaWithCheckpoint(t *testing.T, dir string, nAdds int) string {
	t.Helper()
	logDir := filepath.Join(dir, "_delta_log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "00000000000000000000.json"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	cpPath := filepath.Join(logDir, "00000000000000000000.checkpoint.parquet")
	f, err := os.Create(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[checkpointRow](f, parquet.Compression(&parquet.Snappy))
	rows := make([]checkpointRow, nAdds)
	for i := range rows {
		rows[i] = checkpointRow{Add: &deltaAdd{
			Path:  fmt.Sprintf("part-%05d.parquet", i),
			Stats: `{"numRecords":3}`,
		}}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return cpPath
}

func TestListDeltaFilesFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	writeDeltaWithCheckpoint(t, dir, 50)
	files, label, err := ListDeltaFiles(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 50 {
		t.Fatalf("got %d files, want 50", len(files))
	}
	if files[0].Rows != 3 {
		t.Errorf("rows = %d, want 3 (from add stats)", files[0].Rows)
	}
	if !strings.HasSuffix(label, "#0") {
		t.Errorf("label = %q, want version 0", label)
	}
}

// A decode error mid-checkpoint must fail the listing: treating it as EOF
// would silently yield a partial live-file set. The corruption below leaves
// the footer (and page index) intact so parquet.OpenFile succeeds and the
// failure happens while reading rows.
func TestListDeltaFilesCorruptCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cpPath := writeDeltaWithCheckpoint(t, dir, 50)

	raw, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	// Overwrite the first column chunk's pages (header and data) with 0xFF.
	col := pf.Metadata().RowGroups[0].Columns[0].MetaData
	off := col.DataPageOffset
	if col.DictionaryPageOffset > 0 && col.DictionaryPageOffset < off {
		off = col.DictionaryPageOffset
	}
	end := off + col.TotalCompressedSize
	if off < 4 || end >= int64(len(raw))-8 {
		t.Fatalf("column chunk [%d, %d) not strictly inside the file (%d bytes)", off, end, len(raw))
	}
	for i := off; i < end; i++ {
		raw[i] = 0xFF
	}
	// Sanity: the footer must still open.
	if _, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatalf("corruption broke the footer, not the pages: %v", err)
	}
	if err := os.WriteFile(cpPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err = ListDeltaFiles(dir, "")
	if err == nil {
		t.Fatal("corrupt checkpoint listed successfully (partial live-file set)")
	}
	// The message pins the mid-read path in readCheckpoint: OpenFile passed
	// the sanity check above, so only the row-reading loop can report this.
	if !strings.Contains(err.Error(), "reading checkpoint") {
		t.Errorf("unexpected error: %v", err)
	}
}
