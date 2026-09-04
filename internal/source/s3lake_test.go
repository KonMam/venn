package source_test

// End-to-end lake-tables-on-S3 test: the real table fixtures are served
// through a minimal in-process S3 API (ListObjectsV2, GET with ranges,
// HEAD), and the tables are opened via s3:// URLs, so metadata, manifests,
// and parquet data files all travel through the S3 client.

import (
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KonMam/tdiff/internal/diff"
	"github.com/KonMam/tdiff/internal/source"
)

// fakeS3 serves root as bucket "lake".
func fakeS3(t *testing.T, root string) *httptest.Server {
	t.Helper()
	// key → size, in walk order
	sizes := map[string]int64{}
	var keys []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		k := filepath.ToSlash(rel)
		keys = append(keys, k)
		sizes[k] = info.Size()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if bucket != "lake" {
			http.Error(w, "NoSuchBucket", http.StatusNotFound)
			return
		}
		if key == "" { // ListObjectsV2
			q := r.URL.Query()
			prefix, delim := q.Get("prefix"), q.Get("delimiter")
			var b strings.Builder
			b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>`)
			seen := map[string]bool{}
			for _, k := range keys {
				if !strings.HasPrefix(k, prefix) {
					continue
				}
				rest := k[len(prefix):]
				if delim != "" {
					if i := strings.Index(rest, delim); i >= 0 {
						cp := prefix + rest[:i+1]
						if !seen[cp] {
							seen[cp] = true
							fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", cp)
						}
						continue
					}
				}
				fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size></Contents>", k, sizes[k])
			}
			b.WriteString("</ListBucketResult>")
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, b.String())
			return
		}
		fp := filepath.Join(root, filepath.FromSlash(key))
		f, err := os.Open(fp)
		if err != nil {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		defer f.Close()
		st, _ := f.Stat()
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(st.Size()))
			return
		}
		http.ServeContent(w, r, key, st.ModTime(), f)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestS3LakeTables(t *testing.T) {
	base := "../../testdata/tables"
	if _, err := os.Stat(filepath.Join(base, "delta_orders", "_delta_log")); err != nil {
		t.Skip("table fixtures not generated")
	}
	srv := fakeS3(t, base)
	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	check := func(name string, res *diff.Result, shared int64) {
		t.Helper()
		if res.Added != 50 || res.Removed != 3 || res.Changed != 3 {
			t.Errorf("%s: got +%d -%d ~%d, want +50 -3 ~3", name, res.Added, res.Removed, res.Changed)
		}
		if got := res.Unchanged + shared; got != 4994 {
			t.Errorf("%s: unchanged %d (incl. %d shared), want 4994", name, got, shared)
		}
	}

	t.Run("delta", func(t *testing.T) {
		left, right, pair, err := source.OpenPair("s3://lake/delta_orders#0", "s3://lake/delta_orders#1", source.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer left.Close()
		defer right.Close()
		res, err := diff.Run(left, right, diff.Options{Keys: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		check("delta", res, pair.SharedRows)
	})

	t.Run("iceberg", func(t *testing.T) {
		snapsRaw, err := os.ReadFile(filepath.Join(base, "iceberg_snapshots.txt"))
		if err != nil {
			t.Fatal(err)
		}
		snaps := strings.Fields(string(snapsRaw))
		orders := "s3://lake/iceberg_wh/db/orders"
		left, right, pair, err := source.OpenPair(orders+"#"+snaps[0], orders+"#"+snaps[2], source.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer left.Close()
		defer right.Close()
		res, err := diff.Run(left, right, diff.Options{Keys: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		check("iceberg", res, pair.SharedRows)
	})

	t.Run("iceberg-pruned-append", func(t *testing.T) {
		snapsRaw, _ := os.ReadFile(filepath.Join(base, "iceberg_snapshots.txt"))
		snaps := strings.Fields(string(snapsRaw))
		orders := "s3://lake/iceberg_wh/db/orders"
		left, right, pair, err := source.OpenPair(orders+"#"+snaps[3], orders+"#"+snaps[4], source.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer left.Close()
		defer right.Close()
		if pair.SharedFiles == 0 {
			t.Fatalf("no shared files pruned over s3: %+v", pair)
		}
		res, err := diff.Run(left, right, diff.Options{Keys: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		if res.Added != 10 || res.Removed != 0 || res.Changed != 0 {
			t.Errorf("append diff: got +%d -%d ~%d, want +10 -0 ~0", res.Added, res.Removed, res.Changed)
		}
	})
}
