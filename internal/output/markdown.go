package output

// Markdown renders a diff result as a GitHub-flavored summary, sized for
// $GITHUB_STEP_SUMMARY / PR comments: verdict first, count table, changed
// columns, then examples folded into a <details> block.

import (
	"fmt"
	"io"
	"strings"

	"github.com/KonMam/tdiff/internal/diff"
)

// mdEscape neutralizes table/HTML-significant characters in cell values.
func mdEscape(s string) string {
	r := strings.NewReplacer("|", "\\|", "<", "&lt;", ">", "&gt;", "\n", " ", "`", "\\`")
	return r.Replace(s)
}

// Markdown writes the report. left/right label the inputs.
func Markdown(w io.Writer, res *diff.Result, left, right string) {
	fmt.Fprintf(w, "### tdiff: `%s` vs `%s`\n\n", mdEscape(left), mdEscape(right))
	if res.Filter != "" {
		fmt.Fprintf(w, "_Filtered to `%s` on both sides — every count below is of the matching rows",
			mdEscape(res.Filter))
		if res.FilesPruned > 0 {
			fmt.Fprintf(w, " (%d data files skipped by partition value)", res.FilesPruned)
		}
		fmt.Fprintf(w, "._\n\n")
	}
	if res.Comparison != "" {
		fmt.Fprintf(w, "_Compared with %s._\n\n", mdEscape(res.Comparison))
	}

	if res.Same() {
		fmt.Fprintf(w, "✅ **Identical** — %s rows compared, schema matches.\n", comma(res.Unchanged))
		for _, rn := range res.Schema.Renames {
			fmt.Fprintf(w, "\n- renamed column `%s` ⇐ `%s` (compared as one column)\n",
				mdEscape(rn.Left), mdEscape(rn.Right))
		}
		return
	}
	switch {
	case res.Aborted && res.PartialCounts:
		fmt.Fprintf(w, "❌ **At least %s differing rows** (the run stopped early)\n\n",
			comma(res.Added+res.Removed+res.Changed))
	case res.RowsSame():
		fmt.Fprintf(w, "⚠️ Rows identical (%s compared), but the **schema differs**.\n\n", comma(res.Unchanged))
	default:
		fmt.Fprintf(w, "❌ **%s differing rows**\n\n", comma(res.Added+res.Removed+res.Changed))
	}

	sd := &res.Schema
	if len(sd.Renames) > 0 {
		fmt.Fprintf(w, "**Renamed columns** (compared, not reported as added/removed):\n")
		for _, rn := range sd.Renames {
			fmt.Fprintf(w, "- `%s` ⇐ `%s`\n", mdEscape(rn.Left), mdEscape(rn.Right))
		}
		fmt.Fprintln(w)
	}
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

	if res.PartialCounts {
		fmt.Fprintf(w, "| ≥ Added | ≥ Removed | ≥ Changed | Unchanged | Left scanned | Right scanned |\n")
	} else {
		fmt.Fprintf(w, "| Added | Removed | Changed | Unchanged | Left rows | Right rows |\n")
	}
	fmt.Fprintf(w, "|------:|--------:|--------:|----------:|----------:|-----------:|\n")
	fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s |\n\n",
		comma(res.Added), comma(res.Removed), comma(res.Changed),
		comma(res.Unchanged), comma(res.LeftRows), comma(res.RightRows))

	if res.Aborted {
		fmt.Fprintf(w, "⏹️ **Stopped early:** %s.\n\n", mdEscape(res.AbortReason))
	}
	if len(res.Masked) > 0 {
		fmt.Fprintf(w, "🔒 Masked columns (values shown as `xxh:` tokens): %s\n\n",
			"`"+mdEscape(strings.Join(res.Masked, "`, `"))+"`")
	}
	if res.WithinTolerance > 0 {
		fmt.Fprintf(w, "ℹ️ %s rows differ only within tolerance (not counted as changed).\n\n",
			comma(res.WithinTolerance))
	}
	if res.DupKeys > 0 {
		fmt.Fprintf(w, "⚠️ %s keys duplicated on the left (%s rows), matched as multisets — identical rows cancel, leftovers count as added/removed, and no change attribution is attempted inside a duplicate group.\n\n",
			comma(res.DupKeys), comma(res.DupRows))
	}
	if res.DupsLeft > 0 || res.DupsRight > 0 {
		fmt.Fprintf(w, "⚠️ Duplicate keys set aside: %s left, %s right (first occurrence kept).\n\n",
			comma(res.DupsLeft), comma(res.DupsRight))
	}

	if cols := sortedColumnChanges(res); len(cols) > 0 {
		haveStats, numeric := false, false
		for _, c := range cols {
			if c.stat == nil {
				continue
			}
			haveStats = true
			numeric = numeric || c.stat.Numeric
		}
		switch {
		case !haveStats:
			// nothing to tabulate beyond the counts (a summary-mode result)
			parts := make([]string, len(cols))
			for i, c := range cols {
				parts[i] = fmt.Sprintf("`%s` (%s)", mdEscape(c.name), comma(c.n))
			}
			fmt.Fprintf(w, "**Changed columns:** %s\n\n", strings.Join(parts, ", "))
		case numeric:
			fmt.Fprintf(w, "**Changed columns:**\n\n")
			fmt.Fprintf(w, "| Column | Changed rows | Match rate | Max Δ | Mean Δ |\n")
			fmt.Fprintf(w, "|---|---:|---:|---:|---:|\n")
			for _, c := range cols {
				maxΔ, meanΔ := "—", "—"
				if c.stat != nil && c.stat.Numeric {
					maxΔ, meanΔ = trimFloat(c.stat.MaxAbsDiff), trimFloat(c.stat.MeanAbsDiff)
				}
				fmt.Fprintf(w, "| `%s` | %s | %s | %s | %s |\n",
					mdEscape(c.name), comma(c.n), rateCell(c), maxΔ, meanΔ)
			}
			fmt.Fprintln(w)
		default:
			fmt.Fprintf(w, "**Changed columns:**\n\n")
			fmt.Fprintf(w, "| Column | Changed rows | Match rate |\n|---|---:|---:|\n")
			for _, c := range cols {
				fmt.Fprintf(w, "| `%s` | %s | %s |\n", mdEscape(c.name), comma(c.n), rateCell(c))
			}
			fmt.Fprintln(w)
		}
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
	noun := "keys"
	if res.Keyless {
		noun = "rows" // there is no key to name a row by
	}
	if len(res.AddedExamples) > 0 {
		fmt.Fprintf(w, "**Added %s:** %s\n\n", noun, mdEscape(strings.Join(res.AddedExamples, ", ")))
	}
	if len(res.RemovedExamples) > 0 {
		fmt.Fprintf(w, "**Removed %s:** %s\n\n", noun, mdEscape(strings.Join(res.RemovedExamples, ", ")))
	}
	if int64(nex) < res.Added+res.Removed+res.Changed {
		fmt.Fprintf(w, "…examples truncated (raise `--limit`).\n")
	}
	fmt.Fprintf(w, "</details>\n")
}
