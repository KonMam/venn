package source

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTmp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDelimiterOption(t *testing.T) {
	// enough rows to clear the vectorized scanner's block path as well as
	// the encoding/csv fallback
	var sb strings.Builder
	sb.WriteString("id;name;v\n")
	for i := 0; i < 5000; i++ {
		sb.WriteString("1;alice;2.5\n")
	}
	path := writeTmp(t, "semi.csv", sb.String())

	src, err := OpenWith(path, Options{Delimiter: ";"})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	cols := src.Schema().Columns
	if len(cols) != 3 || cols[0].Name != "id" || cols[2].Name != "v" {
		t.Fatalf("schema = %+v, want id/name/v", cols)
	}
	if cols[0].Type != TypeInt64 || cols[2].Type != TypeFloat64 {
		t.Fatalf("types = %v/%v, want int64/float64", cols[0].Type, cols[2].Type)
	}
	if n := scanRowCount(t, src); n != 5000 {
		t.Fatalf("scanned %d rows, want 5000", n)
	}

	// without the option the whole line is one column
	plain, err := OpenWith(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if got := len(plain.Schema().Columns); got != 1 {
		t.Fatalf("default delimiter gave %d columns, want 1", got)
	}
}

// TestDelimiterOverridesTSV pins that the option beats the extension's
// default, and that .tsv still defaults to tab.
func TestDelimiterOverridesTSV(t *testing.T) {
	path := writeTmp(t, "odd.tsv", "id|v\n1|2\n")
	src, err := OpenWith(path, Options{Delimiter: "|"})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if got := len(src.Schema().Columns); got != 2 {
		t.Fatalf("got %d columns, want 2", got)
	}

	tabbed := writeTmp(t, "tabs.tsv", "id\tv\n1\t2\n")
	src2, err := OpenWith(tabbed, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	if got := len(src2.Schema().Columns); got != 2 {
		t.Fatalf("default .tsv gave %d columns, want 2", got)
	}
}

func TestDelimiterParsing(t *testing.T) {
	cases := []struct {
		in      string
		want    rune
		wantErr bool
	}{
		{in: "", want: ','},
		{in: ";", want: ';'},
		{in: "|", want: '|'},
		{in: `\t`, want: '\t'},
		{in: "\t", want: '\t'},
		{in: "tab", want: '\t'},
		{in: ";;", wantErr: true},
		{in: "…", wantErr: true},
	}
	for _, c := range cases {
		got, err := Options{Delimiter: c.in}.delimiter(',')
		if c.wantErr {
			if err == nil {
				t.Errorf("delimiter(%q) = %q, want an error", c.in, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("delimiter(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
}

// TestDelimiterReachesLakeTables pins the Options threading: a delimiter (and
// whole-file inference) must survive the directory/multi-file open path.
func TestDelimiterReachesMultiFile(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.csv", "b.csv"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("id;v\n1;2\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	src, err := OpenWith(dir, Options{Delimiter: ";"})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if got := len(src.Schema().Columns); got != 2 {
		t.Fatalf("got %d columns, want 2", got)
	}
}
