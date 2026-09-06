package output

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/KonMam/venn/internal/source"
)

// The export writers share one contract: constructor, concurrent-safe
// WriteDiffRow (sync.Mutex; the CSV writer additionally formats outside the
// lock via a sync.Pool of record buffers), then closer. These tests exercise
// the real writers end-to-end and read the files back.

func exportCols() (keyNames []string, keyTypes []source.Type, valNames []string, valTypes []source.Type) {
	return []string{"id"}, []source.Type{source.TypeInt64},
		[]string{"v", "s"}, []source.Type{source.TypeInt64, source.TypeString}
}

func intVal(n int64) source.Value  { return source.Value{Type: source.TypeInt64, Int: n} }
func strVal(s string) source.Value { return source.Value{Type: source.TypeString, Str: s} }
func nullVal(t source.Type) source.Value {
	return source.Value{Type: t, Null: true}
}

func TestExportRejectsUnknownExtension(t *testing.T) {
	kn, kt, vn, vt := exportCols()
	if _, _, err := NewExport(filepath.Join(t.TempDir(), "out.txt"), kn, kt, vn, vt); err == nil {
		t.Fatal("NewExport accepted .txt")
	}
}

func TestCSVExport(t *testing.T) {
	kn, kt, vn, vt := exportCols()
	path := filepath.Join(t.TempDir(), "out.csv")
	sink, closer, err := NewExport(path, kn, kt, vn, vt)
	if err != nil {
		t.Fatal(err)
	}

	// added: no left row; removed: no right row; changed: NULL string on the left
	if err := sink.WriteDiffRow('a', []source.Value{intVal(1)}, nil,
		[]source.Value{intVal(10), strVal("x")}); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteDiffRow('r', []source.Value{intVal(2)},
		[]source.Value{intVal(20), strVal("y")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteDiffRow('c', []source.Value{intVal(3)},
		[]source.Value{intVal(30), nullVal(source.TypeString)},
		[]source.Value{intVal(31), strVal("z")}); err != nil {
		t.Fatal(err)
	}
	if err := closer(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	wantHeader := []string{"id", "diff_status", "v__left", "v__right", "s__left", "s__right"}
	if len(recs) != 4 {
		t.Fatalf("got %d records, want 4", len(recs))
	}
	for i, h := range wantHeader {
		if recs[0][i] != h {
			t.Errorf("header[%d] = %q, want %q", i, recs[0][i], h)
		}
	}
	want := [][]string{
		{"1", "added", "", "10", "", "x"},
		{"2", "removed", "20", "", "y", ""},
		{"3", "changed", "30", "31", "", "z"},
	}
	for i, w := range want {
		for j, cell := range w {
			if recs[i+1][j] != cell {
				t.Errorf("row %d col %d = %q, want %q", i, j, recs[i+1][j], cell)
			}
		}
	}
}

func TestCSVExportConcurrent(t *testing.T) {
	kn, kt, vn, vt := exportCols()
	path := filepath.Join(t.TempDir(), "out.csv")
	sink, closer, err := NewExport(path, kn, kt, vn, vt)
	if err != nil {
		t.Fatal(err)
	}

	const workers, perWorker = 8, 200
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := int64(g*perWorker + i)
				err := sink.WriteDiffRow('c', []source.Value{intVal(id)},
					[]source.Value{intVal(id), strVal("l" + strconv.FormatInt(id, 10))},
					[]source.Value{intVal(-id), strVal("r" + strconv.FormatInt(id, 10))})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := closer(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != workers*perWorker+1 {
		t.Fatalf("got %d records, want %d", len(recs), workers*perWorker+1)
	}
	seen := make(map[string]bool, workers*perWorker)
	for _, rec := range recs[1:] {
		id := rec[0]
		if seen[id] {
			t.Fatalf("duplicate row for key %s", id)
		}
		seen[id] = true
		if rec[1] != "changed" || rec[2] != id || rec[4] != "l"+id || rec[5] != "r"+id {
			t.Fatalf("row for key %s scrambled: %v", id, rec)
		}
	}
}

// pqExportRow mirrors the export schema for reading the file back.
type pqExportRow struct {
	ID     *int64  `parquet:"id,optional"`
	Status string  `parquet:"diff_status"`
	VLeft  *int64  `parquet:"v__left,optional"`
	VRight *int64  `parquet:"v__right,optional"`
	SLeft  *string `parquet:"s__left,optional"`
	SRight *string `parquet:"s__right,optional"`
}

func TestParquetExport(t *testing.T) {
	kn, kt, vn, vt := exportCols()
	path := filepath.Join(t.TempDir(), "out.parquet")
	sink, closer, err := NewExport(path, kn, kt, vn, vt)
	if err != nil {
		t.Fatal(err)
	}

	if err := sink.WriteDiffRow('a', []source.Value{intVal(1)}, nil,
		[]source.Value{intVal(10), strVal("x")}); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteDiffRow('r', []source.Value{intVal(2)},
		[]source.Value{intVal(20), strVal("y")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteDiffRow('c', []source.Value{intVal(3)},
		[]source.Value{intVal(30), nullVal(source.TypeString)},
		[]source.Value{intVal(31), strVal("z")}); err != nil {
		t.Fatal(err)
	}
	if err := closer(); err != nil {
		t.Fatal(err)
	}

	rows := readParquetExport(t, path)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	sort.Slice(rows, func(i, j int) bool { return *rows[i].ID < *rows[j].ID })

	r := rows[0]
	if *r.ID != 1 || r.Status != "added" || r.VLeft != nil || *r.VRight != 10 ||
		r.SLeft != nil || *r.SRight != "x" {
		t.Errorf("added row wrong: %+v", r)
	}
	r = rows[1]
	if *r.ID != 2 || r.Status != "removed" || *r.VLeft != 20 || r.VRight != nil ||
		*r.SLeft != "y" || r.SRight != nil {
		t.Errorf("removed row wrong: %+v", r)
	}
	r = rows[2]
	if *r.ID != 3 || r.Status != "changed" || *r.VLeft != 30 || *r.VRight != 31 ||
		r.SLeft != nil || *r.SRight != "z" {
		t.Errorf("changed row wrong: %+v", r)
	}
}

func TestParquetExportConcurrent(t *testing.T) {
	kn, kt, vn, vt := exportCols()
	path := filepath.Join(t.TempDir(), "out.parquet")
	sink, closer, err := NewExport(path, kn, kt, vn, vt)
	if err != nil {
		t.Fatal(err)
	}

	const workers, perWorker = 4, 100
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id := int64(g*perWorker + i)
				err := sink.WriteDiffRow('c', []source.Value{intVal(id)},
					[]source.Value{intVal(id * 2), strVal(fmt.Sprintf("s%d", id))},
					[]source.Value{intVal(id*2 + 1), nullVal(source.TypeString)})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := closer(); err != nil {
		t.Fatal(err)
	}

	rows := readParquetExport(t, path)
	if len(rows) != workers*perWorker {
		t.Fatalf("got %d rows, want %d", len(rows), workers*perWorker)
	}
	seen := make(map[int64]bool, len(rows))
	for _, r := range rows {
		id := *r.ID
		if seen[id] {
			t.Fatalf("duplicate row for key %d", id)
		}
		seen[id] = true
		if r.Status != "changed" || *r.VLeft != id*2 || *r.VRight != id*2+1 ||
			*r.SLeft != fmt.Sprintf("s%d", id) || r.SRight != nil {
			t.Fatalf("row for key %d scrambled: %+v", id, r)
		}
	}
}

func readParquetExport(t *testing.T, path string) []pqExportRow {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	reader := parquet.NewGenericReader[pqExportRow](pf)
	defer reader.Close()
	out := make([]pqExportRow, pf.NumRows())
	n := 0
	for n < len(out) {
		m, err := reader.Read(out[n:])
		n += m
		if err != nil {
			break
		}
	}
	return out[:n]
}
