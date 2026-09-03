// Package output renders diff results for humans and machines.
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"

	"tdiff/internal/diff"
	"tdiff/internal/schema"
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
	if sd.Same() {
		fmt.Fprintln(w, "schema: identical")
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

// Human renders the full diff result for terminals.
func Human(w io.Writer, res *diff.Result, verbose bool) {
	SchemaHuman(w, &res.Schema)
	if res.RowsSame() {
		fmt.Fprintf(w, "rows:   identical (%s compared)\n", comma(res.Unchanged))
		return
	}
	fmt.Fprintf(w, "rows:   +%s added   -%s removed   ~%s changed   =%s unchanged   (left %s, right %s)\n",
		comma(res.Added), comma(res.Removed), comma(res.Changed), comma(res.Unchanged),
		comma(res.LeftRows), comma(res.RightRows))

	if len(res.ColumnChanges) > 0 {
		type cc struct {
			name string
			n    int64
		}
		cols := make([]cc, 0, len(res.ColumnChanges))
		for name, n := range res.ColumnChanges {
			cols = append(cols, cc{name, n})
		}
		sort.Slice(cols, func(i, j int) bool {
			if cols[i].n != cols[j].n {
				return cols[i].n > cols[j].n
			}
			return cols[i].name < cols[j].name
		})
		fmt.Fprintf(w, "changed columns:")
		for _, c := range cols {
			fmt.Fprintf(w, " %s(%s)", c.name, comma(c.n))
		}
		fmt.Fprintln(w)
	}

	if !verbose {
		return
	}
	for _, k := range res.AddedExamples {
		fmt.Fprintf(w, "+ key=%s\n", k)
	}
	for _, k := range res.RemovedExamples {
		fmt.Fprintf(w, "- key=%s\n", k)
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
