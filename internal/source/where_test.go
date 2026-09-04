package source

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseWhere(t *testing.T) {
	cases := []struct {
		spec    string
		want    string
		wantErr bool
	}{
		{spec: "price > 10", want: "price > 10"},
		{spec: "price>10", want: "price > 10"},
		{spec: "price >= 10.5", want: "price >= 10.5"},
		{spec: "price <= -3", want: "price <= -3"},
		{spec: "n != 1", want: "n != 1"},
		{spec: "n <> 1", want: "n != 1"},
		{spec: "n == 1", want: "n = 1"},
		{spec: "region = 'eu'", want: "region = eu"},
		{spec: `region = "eu west"`, want: "region = eu west"},
		{spec: "note IS NULL", want: "note IS NULL"},
		{spec: "note is not null", want: "note IS NOT NULL"},
		{spec: "a = 1 and b = 2", want: "a = 1 AND b = 2"},
		{spec: "a = 1 AND b = 2 and c = 3", want: "a = 1 AND b = 2 AND c = 3"},
		// " and " inside a quoted literal is part of the literal
		{spec: "s = 'x and y'", want: "s = x and y"},
		// an operator inside a quoted literal is not the clause's operator
		{spec: `s = '>=5'`, want: "s = >=5"},
		{spec: "", wantErr: true},
		{spec: "price", wantErr: true},
		{spec: "= 5", wantErr: true},
		{spec: "price >", wantErr: true},
		{spec: "IS NULL", wantErr: true},
	}
	for _, c := range cases {
		preds, err := ParseWhere(c.spec)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseWhere(%q) = %v, want an error", c.spec, PredicateString(preds))
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseWhere(%q): %v", c.spec, err)
			continue
		}
		if got := PredicateString(preds); got != c.want {
			t.Errorf("ParseWhere(%q) = %q, want %q", c.spec, got, c.want)
		}
	}
}

// filterRows scans a filtered source and returns the surviving rows as text.
func filterRows(t *testing.T, path, spec string) []string {
	t.Helper()
	src, err := OpenWith(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	preds, err := ParseWhere(spec)
	if err != nil {
		t.Fatal(err)
	}
	f, err := Filter(src, preds)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	err = Scan(f, 1, func() (BatchFunc, error) {
		return func(b *Batch) error {
			for r := 0; r < b.N; r++ {
				parts := make([]string, len(b.Cols))
				for ci := range b.Cols {
					v := b.Cols[ci].Value(r)
					parts[ci] = v.Display()
				}
				out = append(out, strings.Join(parts, "|"))
			}
			return nil
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFilterBatches(t *testing.T) {
	path := writeTmp(t, "w.csv",
		"id,region,price,note,ts,flag\n"+
			"1,eu,10.5,x,2026-01-01T00:00:00Z,true\n"+
			"2,us,20,,2026-02-01T00:00:00Z,false\n"+
			"3,eu,30,y,2026-03-01T00:00:00Z,true\n"+
			"4,ap,40,z,2026-04-01T00:00:00Z,false\n")
	cases := []struct {
		spec string
		want []string
	}{
		{"price > 25", []string{"3", "4"}},
		{"price >= 30", []string{"3", "4"}},
		{"price < 20", []string{"1"}},
		{"price = 10.5", []string{"1"}},
		{"region = eu", []string{"1", "3"}},
		{"region != eu", []string{"2", "4"}},
		{"region > 'ب'", nil}, // ordinary byte order, nothing sorts above that
		{"flag = true", []string{"1", "3"}},
		{"flag = false", []string{"2", "4"}},
		{"ts >= 2026-03-01T00:00:00Z", []string{"3", "4"}},
		{"id > 2 and region = ap", []string{"4"}},
		{"price > 100", nil},
		{"price > 0", []string{"1", "2", "3", "4"}},
	}
	for _, c := range cases {
		t.Run(c.spec, func(t *testing.T) {
			rows := filterRows(t, path, c.spec)
			var ids []string
			for _, r := range rows {
				ids = append(ids, strings.SplitN(r, "|", 2)[0])
			}
			if strings.Join(ids, ",") != strings.Join(c.want, ",") {
				t.Errorf("kept %v, want %v", ids, c.want)
			}
		})
	}
}

// TestFilterKeepsColumnValues pins that compaction copies the right cells,
// not just the right count.
func TestFilterKeepsColumnValues(t *testing.T) {
	path := writeTmp(t, "w.csv", "id,s,f\n1,a,1.5\n2,b,2.5\n3,c,3.5\n4,d,4.5\n")
	rows := filterRows(t, path, "id != 2 and id != 4")
	want := []string{"1|a|1.5", "3|c|3.5"}
	if strings.Join(rows, " ") != strings.Join(want, " ") {
		t.Fatalf("kept %v, want %v", rows, want)
	}
}

// TestFilterNulls pins the SQL-ish rule: a NULL satisfies no comparison, and
// IS NULL is the way to select one.
func TestFilterNulls(t *testing.T) {
	path := writeTmp(t, "w.csv", "id,n\n1,10\n2,\n3,30\n")
	for _, c := range []struct {
		spec string
		want string
	}{
		{"n > 5", "1,3"},
		{"n < 100", "1,3"},
		{"n != 10", "3"},
		{"n IS NULL", "2"},
		{"n IS NOT NULL", "1,3"},
	} {
		rows := filterRows(t, path, c.spec)
		var ids []string
		for _, r := range rows {
			ids = append(ids, strings.SplitN(r, "|", 2)[0])
		}
		if got := strings.Join(ids, ","); got != c.want {
			t.Errorf("%q kept %q, want %q", c.spec, got, c.want)
		}
	}
}

// TestFilterSerialPath covers Rows(), which key inference and the
// duplicate-key error lookup use.
func TestFilterSerialPath(t *testing.T) {
	path := writeTmp(t, "w.csv", "id,v\n1,10\n2,20\n3,30\n")
	src, err := OpenWith(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	preds, _ := ParseWhere("v > 15")
	f, err := Filter(src, preds)
	if err != nil {
		t.Fatal(err)
	}
	it, err := f.Rows()
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	row := make([]Value, len(f.Schema().Columns))
	var ids []string
	for {
		ok, err := it.Next(row)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		ids = append(ids, row[0].Display())
	}
	if strings.Join(ids, ",") != "2,3" {
		t.Fatalf("kept %v, want 2,3", ids)
	}
}

func TestFilterBindErrors(t *testing.T) {
	path := writeTmp(t, "w.csv", "id,s,f\n1,a,1.5\n")
	src, err := OpenWith(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, c := range []struct{ spec, want string }{
		{"nope = 1", "does not have"},
		{"f > abc", "not a number"},
		{"id > abc", "not a number"},
	} {
		preds, perr := ParseWhere(c.spec)
		if perr != nil {
			t.Fatalf("ParseWhere(%q): %v", c.spec, perr)
		}
		if _, err := Filter(src, preds); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("Filter(%q) err = %v, want one mentioning %q", c.spec, err, c.want)
		}
	}
}

// TestFilterPrunesPartitions pins layer 2: a predicate on a hive partition
// column skips whole files, and the answer is the same either way.
func TestFilterPrunesPartitions(t *testing.T) {
	dir := t.TempDir()
	for _, region := range []string{"eu", "us", "ap"} {
		sub := filepath.Join(dir, "region="+region)
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, "p.csv"),
			[]byte("id,v\n1,10\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	open := func(spec string) (Source, int) {
		src, err := OpenWith(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		preds, err := ParseWhere(spec)
		if err != nil {
			t.Fatal(err)
		}
		f, err := Filter(src, preds)
		if err != nil {
			t.Fatal(err)
		}
		pruned := 0
		if p, ok := f.(interface{ PrunedFiles() int }); ok {
			pruned = p.PrunedFiles()
		}
		return f, pruned
	}
	f, pruned := open("region = eu")
	defer f.Close()
	if pruned != 2 {
		t.Errorf("pruned %d files, want 2", pruned)
	}
	if n := scanRowCount(t, f); n != 1 {
		t.Errorf("scanned %d rows, want 1", n)
	}

	// a predicate on a data column prunes nothing but filters the same rows
	g, pruned := open("v > 100")
	defer g.Close()
	if pruned != 0 {
		t.Errorf("pruned %d files on a data-column predicate, want 0", pruned)
	}
	if n := scanRowCount(t, g); n != 0 {
		t.Errorf("scanned %d rows, want 0", n)
	}
}

// TestFilterUpperBoundRows pins that a filtered source advertises its row
// count as a ceiling, so the engine does not size a table for rows that will
// never arrive.
func TestFilterUpperBoundRows(t *testing.T) {
	path := writeTmp(t, "w.csv", "id,v\n1,10\n2,20\n")
	src, err := OpenWith(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	preds, _ := ParseWhere("v > 15")
	f, err := Filter(src, preds)
	if err != nil {
		t.Fatal(err)
	}
	ub, ok := f.(interface{ RowsAreUpperBound() bool })
	if !ok || !ub.RowsAreUpperBound() {
		t.Fatal("a filtered source must mark its row count as an upper bound")
	}
}
