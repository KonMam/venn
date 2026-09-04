package source

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var modTime = time.Now()

func newReadSeeker(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// TestHTTPParquet serves a real corpus file over a local HTTP server and
// diffs... well, reads it through the range-request path.
func TestHTTPParquet(t *testing.T) {
	data, err := os.ReadFile("../../testdata/interop/pyarrow-default.parquet")
	if err != nil {
		t.Skip("corpus not generated")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "f.parquet", modTime, newReadSeeker(data))
	}))
	defer srv.Close()

	src, err := openRemote(srv.URL+"/f.parquet", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var rows atomic.Int64
	err = src.(*parquetSource).ScanBatches(4, func() (BatchFunc, error) {
		return func(b *Batch) error { rows.Add(int64(b.N)); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rows.Load() != 50000 {
		t.Fatalf("rows=%d want 50000", rows.Load())
	}
}

func TestHTTPCSV(t *testing.T) {
	data, err := os.ReadFile("../../testdata/interop/canonical.csv")
	if err != nil {
		t.Skip("corpus not generated")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "c.csv", modTime, newReadSeeker(data))
	}))
	defer srv.Close()
	src, err := openRemote(srv.URL+"/c.csv", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var rows atomic.Int64
	err = src.(interface {
		ScanBatches(int, func() (BatchFunc, error)) error
	}).ScanBatches(4, func() (BatchFunc, error) {
		return func(b *Batch) error { rows.Add(int64(b.N)); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rows.Load() != 50000 {
		t.Fatalf("rows=%d want 50000", rows.Load())
	}
}
