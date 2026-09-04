// Package output renders diff results for humans and machines.
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/KonMam/tdiff/internal/diff"
	"github.com/KonMam/tdiff/internal/schema"
)

// JSON writes the machine-readable result.
func JSON(w io.Writer, res *diff.Result) error {
	type envelope struct {
		Equal bool `json:"equal"`
		*diff.Result
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(envelope{Equal: res.Same(), Result: res})
}

// SchemaJSON writes just a schema diff.
func SchemaJSON(w io.Writer, sd *schema.Diff) error {
	type envelope struct {
		Equal bool `json:"equal"`
		*schema.Diff
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(envelope{Equal: sd.Same(), Diff: sd})
}

func comma(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg, s = true, s[1:]
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// SchemaHuman renders a schema diff for terminals.
func SchemaHuman(w io.Writer, sd *schema.Diff) {
	// renames are reported even when the schemas are otherwise identical:
	// the reader has to know the columns were matched by instruction
	for _, rn := range sd.Renames {
		fmt.Fprintf(w, "schema: ~ column %s ⇐ %s (renamed)\n", rn.Left, rn.Right)
	}
	if sd.Same() {
		if len(sd.Renames) == 0 {
			fmt.Fprintln(w, "schema: identical")
		}
		return
	}
	for _, c := range sd.AddedColumns {
		fmt.Fprintf(w, "schema: + column %s (right only)\n", c)
	}
	for _, c := range sd.RemovedColumns {
		fmt.Fprintf(w, "schema: - column %s (left only)\n", c)
	}
	for _, tc := range sd.TypeChanges {
		note := ""
		if !tc.Comparable {
			note = "  [not comparable — column excluded from row diff]"
		}
		fmt.Fprintf(w, "schema: ~ column %s: %s → %s%s\n", tc.Column, tc.Left, tc.Right, note)
	}
	for _, nc := range sd.Nullability {
		fmt.Fprintf(w, "schema: ~ column %s: nullable %v → %v\n", nc.Column, nc.LeftNullable, nc.RightNullable)
	}
}

// colCount is one column's changed-row count plus its statistics, for
// display ordering.
type colCount struct {
	name string
	n    int64
	stat *diff.ColumnStat // nil when the diff kept no statistics
}

// sortedColumnChanges orders per-column change counts by count descending,
// then name — the display order shared by every renderer.
func sortedColumnChanges(res *diff.Result) []colCount {
	cols := make([]colCount, 0, len(res.ColumnChanges))
	for name, n := range res.ColumnChanges {
		cols = append(cols, colCount{name, n, res.ColumnStats[name]})
	}
	sort.Slice(cols, func(i, j int) bool {
		if cols[i].n != cols[j].n {
			return cols[i].n > cols[j].n
		}
		return cols[i].name < cols[j].name
	})
	return cols
}

// pct renders a match rate as a percentage, keeping enough digits to show
// that a nearly-perfect column is not actually perfect.
func pct(rate float64) string {
	switch {
	case rate >= 1:
		return "100%"
	case rate > 0.9999:
		return "99.99%"
	case rate > 0.999:
		return fmt.Sprintf("%.3f%%", rate*100)
	default:
		return fmt.Sprintf("%.1f%%", rate*100)
	}
}

// statSuffix renders the optional match-rate and magnitude detail of one
// column.
func statSuffix(c colCount) string {
	if c.stat == nil {
		return ""
	}
	out := ", " + pct(c.stat.MatchRate) + " match"
	if c.stat.Numeric {
		out += fmt.Sprintf(", max Δ %s, mean Δ %s",
			trimFloat(c.stat.MaxAbsDiff), trimFloat(c.stat.MeanAbsDiff))
	}
	return out
}

// rateCell renders one column's match rate for a table cell.
func rateCell(c colCount) string {
	if c.stat == nil {
		return "—"
	}
	return pct(c.stat.MatchRate)
}

// trimFloat renders a difference magnitude compactly.
func trimFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', 4, 64)
}

// Human renders the full diff result for terminals.
func Human(w io.Writer, res *diff.Result, verbose bool) {
	SchemaHuman(w, &res.Schema)
	if res.Comparison != "" {
		fmt.Fprintf(w, "compare: %s\n", res.Comparison)
	}
	if res.Filter != "" {
		note := ""
		if res.FilesPruned > 0 {
			note = fmt.Sprintf(" (%d files skipped by partition)", res.FilesPruned)
		}
		fmt.Fprintf(w, "filter: %s%s — counts are of the matching rows\n", res.Filter, note)
	}
	if len(res.Masked) > 0 {
		fmt.Fprintf(w, "masked: %s (values shown as xxh: tokens)\n", strings.Join(res.Masked, ", "))
	}
	if res.Aborted && res.PartialCounts {
		// the scan was cancelled mid-flight: every count is a lower bound,
		// so mark them rather than presenting them as totals
		fmt.Fprintf(w, "rows:   ≥+%s added   ≥-%s removed   ≥~%s changed   (scanned left %s, right %s)\n",
			comma(res.Added), comma(res.Removed), comma(res.Changed),
			comma(res.LeftRows), comma(res.RightRows))
		fmt.Fprintf(w, "abort:  %s\n", res.AbortReason)
		return
	}
	if res.Aborted {
		fmt.Fprintf(w, "abort:  %s\n", res.AbortReason)
	}
	if res.DupKeys > 0 {
		fmt.Fprintf(w, "dups:   %s keys duplicated on the left (%s rows), matched as multisets\n",
			comma(res.DupKeys), comma(res.DupRows))
	}
	if res.RowsSame() {
		fmt.Fprintf(w, "rows:   identical (%s compared)\n", comma(res.Unchanged))
		if res.WithinTolerance > 0 {
			fmt.Fprintf(w, "tol:    %s rows differ only within tolerance\n", comma(res.WithinTolerance))
		}
		if res.DupsLeft > 0 || res.DupsRight > 0 {
			fmt.Fprintf(w, "dups:   %s left, %s right rows set aside (first occurrence per key kept)\n",
				comma(res.DupsLeft), comma(res.DupsRight))
		}
		return
	}
	fmt.Fprintf(w, "rows:   +%s added   -%s removed   ~%s changed   =%s unchanged   (left %s, right %s)\n",
		comma(res.Added), comma(res.Removed), comma(res.Changed), comma(res.Unchanged),
		comma(res.LeftRows), comma(res.RightRows))

	if res.WithinTolerance > 0 {
		fmt.Fprintf(w, "tol:    %s rows differ only within tolerance (not counted as changed)\n",
			comma(res.WithinTolerance))
	}
	if res.DupsLeft > 0 || res.DupsRight > 0 {
		fmt.Fprintf(w, "dups:   %s left, %s right rows set aside (first occurrence per key kept)\n",
			comma(res.DupsLeft), comma(res.DupsRight))
	}
	if len(res.ColumnChanges) > 0 {
		fmt.Fprintf(w, "changed columns:")
		for _, c := range sortedColumnChanges(res) {
			fmt.Fprintf(w, " %s(%s%s)", c.name, comma(c.n), statSuffix(c))
		}
		fmt.Fprintln(w)
	}

	if !verbose {
		return
	}
	label := "key"
	if res.Keyless {
		label = "row" // there is no key to name a row by
	}
	for _, k := range res.AddedExamples {
		fmt.Fprintf(w, "+ %s=%s\n", label, k)
	}
	for _, k := range res.RemovedExamples {
		fmt.Fprintf(w, "- %s=%s\n", label, k)
	}
	for _, ex := range res.ChangedExamples {
		fmt.Fprintf(w, "~ key=%s:", ex.Key)
		for _, c := range ex.Columns {
			fmt.Fprintf(w, " %s: %s → %s;", c.Column, c.Left, c.Right)
		}
		fmt.Fprintln(w)
	}
	more := int64(len(res.AddedExamples))+int64(len(res.RemovedExamples))+int64(len(res.ChangedExamples)) < res.Added+res.Removed+res.Changed
	if more {
		fmt.Fprintln(w, "… (examples truncated; raise --limit)")
	}
}
