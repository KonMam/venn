package output

import (
	"strings"
	"testing"

	"github.com/KonMam/venn/internal/diff"
	"github.com/KonMam/venn/internal/schema"
)

// TestHTMLSelfContained is the whole point of the HTML report: it has to open
// from a CI artifact with no network. Nothing may reference an external
// origin, and the CSS and JS must be inline.
func TestHTMLSelfContained(t *testing.T) {
	var sb strings.Builder
	HTML(&sb, diffResult(), "left.parquet", "right.csv")
	out := sb.String()
	for _, forbidden := range []string{"http://", "https://", "//cdn", "src=", "@import", "url("} {
		if strings.Contains(out, forbidden) {
			t.Errorf("report references %q — it must be self-contained", forbidden)
		}
	}
	for _, want := range []string{"<!doctype html>", "<style>", "<script>", "</html>"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in the report", want)
		}
	}
}

func TestHTMLEscapesValues(t *testing.T) {
	res := &diff.Result{
		LeftRows: 1, RightRows: 1, Changed: 1,
		ColumnChanges: map[string]int64{"<script>": 1},
		ColumnStats:   map[string]*diff.ColumnStat{"<script>": {Changed: 1}},
		ChangedExamples: []diff.RowExample{{
			Key: `"><img onerror=alert(1)>`,
			Columns: []diff.ColumnChange{{
				Column: "<b>", Left: "<script>alert(1)</script>", Right: "a & b",
			}},
		}},
		AddedExamples: []string{"<i>"},
		Added:         1,
	}
	res.FinishStats()
	var sb strings.Builder
	HTML(&sb, res, "<left>", "<right>")
	out := sb.String()
	// the only tags in the document must be the ones the renderer wrote
	for _, injected := range []string{"<script>alert(1)</script>", "<img onerror", "<b>", "<i>", "<left>"} {
		if strings.Contains(out, injected) {
			t.Errorf("unescaped %q reached the report", injected)
		}
	}
	for _, want := range []string{"&lt;script&gt;alert(1)&lt;/script&gt;", "a &amp; b", "&lt;left&gt;"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing escaped %q in the report", want)
		}
	}
	// exactly one <script> element: the sort handler
	if n := strings.Count(out, "<script"); n != 1 {
		t.Errorf("found %d <script tags, want 1", n)
	}
}

func TestHTMLVerdicts(t *testing.T) {
	cases := []struct {
		name string
		res  *diff.Result
		want string
	}{
		{"identical", &diff.Result{Unchanged: 100}, "verdict ok\">Identical"},
		{"schema only", &diff.Result{
			Unchanged: 100,
			Schema:    schema.Diff{AddedColumns: []string{"c"}},
		}, "verdict warn\">Rows identical, schema differs"},
		{"differing", &diff.Result{Changed: 3, Unchanged: 7}, "verdict bad\">3 differing rows"},
		{"aborted partial", &diff.Result{
			Changed: 3, Aborted: true, PartialCounts: true,
			AbortReason: "budget blown",
		}, "verdict bad\">Stopped early"},
		{"aborted exact", &diff.Result{
			Changed: 3, Aborted: true, AbortReason: "budget blown",
		}, "stopped before column attribution"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var sb strings.Builder
			HTML(&sb, c.res, "l", "r")
			if !strings.Contains(sb.String(), c.want) {
				t.Errorf("missing %q in:\n%s", c.want, sb.String())
			}
		})
	}
}

// TestHTMLPartialCountsMarked pins that a cancelled run's counts are not
// presented as totals.
func TestHTMLPartialCountsMarked(t *testing.T) {
	res := &diff.Result{
		Added: 5, Changed: 7, LeftRows: 100, RightRows: 40,
		Aborted: true, PartialCounts: true, AbortReason: "budget of 1 exceeded",
	}
	var sb strings.Builder
	HTML(&sb, res, "l", "r")
	out := sb.String()
	for _, want := range []string{"≥5", "≥7", "right rows scanned", "budget of 1 exceeded"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestHTMLSortKeys pins that every numeric cell carries a machine-sortable
// data-v: the displayed text has thousands separators, which a numeric sort
// cannot parse.
func TestHTMLSortKeys(t *testing.T) {
	res := &diff.Result{
		Changed: 2000, Unchanged: 8000,
		ColumnChanges: map[string]int64{"a": 1500},
		ColumnStats:   map[string]*diff.ColumnStat{"a": {Changed: 1500, MaxAbsDiff: 2, MeanAbsDiff: 1, Numeric: true}},
	}
	res.FinishStats()
	var sb strings.Builder
	HTML(&sb, res, "l", "r")
	out := sb.String()
	if !strings.Contains(out, `data-v="1500">1,500`) {
		t.Errorf("the changed-rows cell lacks a raw sort value:\n%s", out)
	}
	if !strings.Contains(out, `data-sort="num"`) {
		t.Errorf("no numeric sort columns declared:\n%s", out)
	}
}

func TestHTMLRenamesAndMasking(t *testing.T) {
	res := &diff.Result{
		Unchanged: 10,
		Masked:    []string{"email"},
		Schema:    schema.Diff{Renames: []schema.Rename{{Left: "customer_id", Right: "cust_id"}}},
	}
	var sb strings.Builder
	HTML(&sb, res, "l", "r")
	out := sb.String()
	for _, want := range []string{"Masked columns", "email", "renamed", "cust_id", "customer_id"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
