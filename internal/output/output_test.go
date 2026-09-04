package output

import (
	"strings"
	"testing"

	"github.com/KonMam/venn/internal/diff"
	"github.com/KonMam/venn/internal/schema"
	"github.com/KonMam/venn/internal/source"
)

// diffResult builds a small differing Result exercising every markdown/human
// section: schema changes, counts, dups, column counts, and examples.
func diffResult() *diff.Result {
	return &diff.Result{
		Schema: schema.Diff{
			AddedColumns:   []string{"new_col"},
			RemovedColumns: []string{"old|col"},
			TypeChanges: []schema.TypeChange{{
				Column: "amount", LeftType: source.TypeInt64, RightType: source.TypeString,
				Left: "int64", Right: "string", Comparable: false,
			}},
		},
		LeftRows: 1000, RightRows: 1002,
		Added: 3, Removed: 1, Changed: 2, Unchanged: 996,
		DupsLeft: 1, DupsRight: 0,
		ColumnChanges:   map[string]int64{"price": 2, "qty": 1, "aaa": 1},
		AddedExamples:   []string{"k1", "k2"},
		RemovedExamples: []string{"k3"},
		ChangedExamples: []diff.RowExample{{
			Key:     "k|4",
			Columns: []diff.ColumnChange{{Column: "price", Left: "a`b", Right: "c\nd"}},
		}},
	}
}

func TestMdEscape(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a|b", "a\\|b"},
		{"a\nb", "a b"},
		{"a`b", "a\\`b"},
		{"a<b>c", "a&lt;b&gt;c"},
		{"plain", "plain"},
	}
	for _, c := range cases {
		if got := mdEscape(c.in); got != c.want {
			t.Errorf("mdEscape(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestMarkdownIdentical(t *testing.T) {
	var b strings.Builder
	Markdown(&b, &diff.Result{Unchanged: 1234567}, "a.csv", "b.csv")
	out := b.String()
	for _, want := range []string{"### venn:", "`a.csv`", "`b.csv`", "**Identical**", "1,234,567"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "<details>") {
		t.Errorf("identical report should carry no examples:\n%s", out)
	}
}

func TestMarkdownDiffering(t *testing.T) {
	var b strings.Builder
	Markdown(&b, diffResult(), "left|v1", "right")
	out := b.String()
	for _, want := range []string{
		"`left\\|v1`",          // escaped label
		"**6 differing rows**", // 3+1+2
		"added column `new_col`",
		"removed column `old\\|col`", // escaped schema column
		"`amount`: int64 → string",
		"| 3 | 1 | 2 | 996 | 1,000 | 1,002 |", // count table row
		"Duplicate keys set aside: 1 left, 0 right",
		"**Changed columns:** `price` (2), `aaa` (1), `qty` (1)", // count desc, then name
		"<details><summary>Example rows (4)</summary>",
		"| `k\\|4` | `price` | a\\`b | c d |", // escaped example cells
		"**Added keys:** k1, k2",
		"**Removed keys:** k3",
		"</details>",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// 4 examples for 6 differing rows: truncation note expected.
	if !strings.Contains(out, "examples truncated") {
		t.Errorf("missing truncation note in:\n%s", out)
	}
}

func TestMarkdownSchemaOnlyDiff(t *testing.T) {
	res := &diff.Result{
		Schema:    schema.Diff{AddedColumns: []string{"c"}},
		Unchanged: 10,
	}
	var b strings.Builder
	Markdown(&b, res, "l", "r")
	if !strings.Contains(b.String(), "schema differs") {
		t.Errorf("missing schema-differs verdict:\n%s", b.String())
	}
}

func TestHumanIdentical(t *testing.T) {
	var b strings.Builder
	Human(&b, &diff.Result{Unchanged: 42}, false)
	out := b.String()
	if !strings.Contains(out, "schema: identical") || !strings.Contains(out, "rows:   identical (42 compared)") {
		t.Errorf("unexpected identical output:\n%s", out)
	}
}

func TestHumanDiffering(t *testing.T) {
	var b strings.Builder
	Human(&b, diffResult(), true)
	out := b.String()
	for _, want := range []string{
		"schema: + column new_col (right only)",
		"schema: - column old|col (left only)",
		"schema: ~ column amount: int64 → string  [not comparable — column excluded from row diff]",
		"rows:   +3 added   -1 removed   ~2 changed   =996 unchanged   (left 1,000, right 1,002)",
		"dups:   1 left, 0 right rows set aside",
		"changed columns: price(2) aaa(1) qty(1)",
		"+ key=k1",
		"- key=k3",
		"~ key=k|4: price: a`b → c\nd;",
		"… (examples truncated; raise --limit)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestHumanNonVerboseOmitsExamples(t *testing.T) {
	var b strings.Builder
	Human(&b, diffResult(), false)
	if strings.Contains(b.String(), "key=") {
		t.Errorf("non-verbose output leaked examples:\n%s", b.String())
	}
}

func TestComma(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"}, {1, "1"}, {999, "999"}, {1000, "1,000"},
		{1234567, "1,234,567"}, {-1234, "-1,234"}, {-999, "-999"},
	}
	for _, c := range cases {
		if got := comma(c.in); got != c.want {
			t.Errorf("comma(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestSortedColumnChanges(t *testing.T) {
	got := sortedColumnChanges(&diff.Result{
		ColumnChanges: map[string]int64{"b": 5, "a": 5, "c": 9},
	})
	want := []colCount{{name: "c", n: 9}, {name: "a", n: 5}, {name: "b", n: 5}}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// statsResult is a differing Result carrying per-column statistics and a
// tolerance verdict — the Phase A additions to every renderer.
func statsResult() *diff.Result {
	res := &diff.Result{
		LeftRows: 100, RightRows: 100,
		Changed: 3, Unchanged: 96, WithinTolerance: 1,
		ColumnChanges: map[string]int64{"price": 3, "name": 1},
		ColumnStats: map[string]*diff.ColumnStat{
			"price": {Changed: 3, MaxAbsDiff: 0.5, MeanAbsDiff: 0.25, Numeric: true},
			"name":  {Changed: 1},
		},
	}
	res.FinishStats()
	return res
}

func TestHumanColumnStats(t *testing.T) {
	var sb strings.Builder
	Human(&sb, statsResult(), false)
	out := sb.String()
	for _, want := range []string{
		"1 rows differ only within tolerance",
		"price(3, 97.0% match, max Δ 0.5, mean Δ 0.25)",
		"name(1, 99.0% match)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestMarkdownColumnStats(t *testing.T) {
	var sb strings.Builder
	Markdown(&sb, statsResult(), "l", "r")
	out := sb.String()
	for _, want := range []string{
		"| Column | Changed rows | Match rate | Max Δ | Mean Δ |",
		"| `price` | 3 | 97.0% | 0.5 | 0.25 |",
		"| `name` | 1 | 99.0% | — | — |",
		"1 rows differ only within tolerance",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestMarkdownColumnStatsWithoutNumeric drops the magnitude columns when no
// compared column has a measurable one.
func TestMarkdownColumnStatsWithoutNumeric(t *testing.T) {
	res := &diff.Result{
		Changed: 1, Unchanged: 9,
		ColumnChanges: map[string]int64{"name": 1},
		ColumnStats:   map[string]*diff.ColumnStat{"name": {Changed: 1}},
	}
	res.FinishStats()
	var sb strings.Builder
	Markdown(&sb, res, "l", "r")
	out := sb.String()
	if !strings.Contains(out, "| Column | Changed rows | Match rate |") {
		t.Errorf("want the three-column table in:\n%s", out)
	}
	if strings.Contains(out, "Max Δ") {
		t.Errorf("unexpected magnitude columns in:\n%s", out)
	}
}

// TestMatchRatePrecision pins that a nearly-perfect column does not round to
// a flat 100%.
func TestMatchRatePrecision(t *testing.T) {
	for _, c := range []struct {
		rate float64
		want string
	}{
		{1, "100%"},
		{0.99999, "99.99%"},
		{0.9995, "99.950%"},
		{0.97, "97.0%"},
		{0, "0.0%"},
	} {
		if got := pct(c.rate); got != c.want {
			t.Errorf("pct(%v) = %q, want %q", c.rate, got, c.want)
		}
	}
}

// TestComparisonLineRendered pins that a loosened comparison is stated in
// both renderers — a report that hides it overstates what was checked.
func TestComparisonLineRendered(t *testing.T) {
	res := statsResult()
	res.Comparison = "trim, tolerance ±0.01"

	var human strings.Builder
	Human(&human, res, false)
	if !strings.Contains(human.String(), "compare: trim, tolerance ±0.01") {
		t.Errorf("human output lacks the compare line:\n%s", human.String())
	}

	var md strings.Builder
	Markdown(&md, res, "l", "r")
	if !strings.Contains(md.String(), "_Compared with trim, tolerance ±0.01._") {
		t.Errorf("markdown output lacks the compare note:\n%s", md.String())
	}
}

// TestRenamesRendered pins that a rename is reported in both renderers even
// when the schemas are otherwise identical — the reader has to know two
// differently-named columns were matched by instruction.
func TestRenamesRendered(t *testing.T) {
	sd := schema.Diff{Renames: []schema.Rename{{Left: "customer_id", Right: "cust_id"}}}
	var human strings.Builder
	SchemaHuman(&human, &sd)
	if got := human.String(); !strings.Contains(got, "customer_id ⇐ cust_id (renamed)") {
		t.Errorf("human schema output = %q", got)
	}
	if strings.Contains(human.String(), "identical") {
		t.Errorf("a rename must not be reported as a plain identical schema:\n%s", human.String())
	}

	// identical result: the rename still shows up
	same := &diff.Result{Unchanged: 10, Schema: sd}
	var md strings.Builder
	Markdown(&md, same, "l", "r")
	if !strings.Contains(md.String(), "`customer_id` ⇐ `cust_id`") {
		t.Errorf("markdown (identical) lacks the rename:\n%s", md.String())
	}

	// differing result: same
	differing := diffResult()
	differing.Schema.Renames = sd.Renames
	var md2 strings.Builder
	Markdown(&md2, differing, "l", "r")
	if !strings.Contains(md2.String(), "`customer_id` ⇐ `cust_id`") {
		t.Errorf("markdown (differing) lacks the rename:\n%s", md2.String())
	}
}
