package output

// Markdown renders a diff result as a GitHub-flavored summary, sized for
// $GITHUB_STEP_SUMMARY / PR comments: verdict first, count table, changed
// columns, then examples folded into a <details> block.

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"tdiff/internal/diff"
)

// mdEscape neutralizes table/HTML-significant characters in cell values.
func mdEscape(s string) string {
	r := strings.NewReplacer("|", "\\|", "<", "&lt;", ">", "&gt;", "\n", " ", "`", "\\`")
	return r.Replace(s)
}

// Markdown writes the report. left/right label the inputs.
func Markdown(w io.Writer, res *diff.Result, left, right string) {
	fmt.Fprintf(w, "### tdiff: `%s` vs `%s`\n\n", mdEscape(left), mdEscape(right))

	if res.Same() {
		fmt.Fprintf(w, "✅ **Identical** — %s rows compared, schema matches.\n", comma(res.Unchanged))
		return
	}
	if res.RowsSame() {
		fmt.Fprintf(w, "⚠️ Rows identical (%s compared), but the **schema differs**.\n\n", comma(res.Unchanged))
	} else {
		fmt.Fprintf(w, "❌ **%s differing rows**\n\n", comma(res.Added+res.Removed+res.Changed))
	}

	sd := &res.Schema
	if !sd.Same() {
		fmt.Fprintf(w, "**Schema changes:**\n")
		for _, c := range sd.AddedColumns {
			fmt.Fprintf(w, "- added column `%s`\n", mdEscape(c))
		}
		for _, c := range sd.RemovedColumns {
			fmt.Fprintf(w, "- removed column `%s`\n", mdEscape(c))
		}
		for _, tc := range sd.TypeChanges {
			fmt.Fprintf(w, "- `%s`: %s → %s\n", mdEscape(tc.Column), tc.LeftType, tc.RightType)
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "| Added | Removed | Changed | Unchanged | Left rows | Right rows |\n")
	fmt.Fprintf(w, "|------:|--------:|--------:|----------:|----------:|-----------:|\n")
	fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s |\n\n",
		comma(res.Added), comma(res.Removed), comma(res.Changed),
		comma(res.Unchanged), comma(res.LeftRows), comma(res.RightRows))

	if res.DupsLeft > 0 || res.DupsRight > 0 {
		fmt.Fprintf(w, "⚠️ Duplicate keys set aside: %s left, %s right (first occurrence kept).\n\n",
			comma(res.DupsLeft), comma(res.DupsRight))
	}

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
		fmt.Fprintf(w, "**Changed columns:** ")
		parts := make([]string, len(cols))
		for i, c := range cols {
			parts[i] = fmt.Sprintf("`%s` (%s)", mdEscape(c.name), comma(c.n))
		}
		fmt.Fprintf(w, "%s\n\n", strings.Join(parts, ", "))
	}

	nex := len(res.AddedExamples) + len(res.RemovedExamples) + len(res.ChangedExamples)
	if nex == 0 {
		return
	}
	fmt.Fprintf(w, "<details><summary>Example rows (%d)</summary>\n\n", nex)
	if len(res.ChangedExamples) > 0 {
		fmt.Fprintf(w, "| Key | Column | Left | Right |\n|---|---|---|---|\n")
		for _, ex := range res.ChangedExamples {
			for _, c := range ex.Columns {
				fmt.Fprintf(w, "| `%s` | `%s` | %s | %s |\n",
					mdEscape(ex.Key), mdEscape(c.Column), mdEscape(c.Left), mdEscape(c.Right))
			}
		}
		fmt.Fprintln(w)
	}
	if len(res.AddedExamples) > 0 {
		fmt.Fprintf(w, "**Added keys:** %s\n\n", mdEscape(strings.Join(res.AddedExamples, ", ")))
	}
	if len(res.RemovedExamples) > 0 {
		fmt.Fprintf(w, "**Removed keys:** %s\n\n", mdEscape(strings.Join(res.RemovedExamples, ", ")))
	}
	if int64(nex) < res.Added+res.Removed+res.Changed {
		fmt.Fprintf(w, "…examples truncated (raise `--limit`).\n")
	}
	fmt.Fprintf(w, "</details>\n")
}
