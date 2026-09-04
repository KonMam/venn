package output

// HTML renders a diff result as one self-contained page: inline CSS and JS,
// no network fetches at all. That is the point — the report has to open from
// a CI artifact zip, on an air-gapped box, out of an email attachment, with
// no CDN reachable and no build step.
//
// It takes the same *diff.Result the markdown renderer takes, so it stays
// decoupled from the engine.

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/KonMam/venn/internal/diff"
)

// htmlStyle is the whole stylesheet. It follows the viewer's colour scheme
// rather than picking one, so the report does not glare in either.
const htmlStyle = `
:root {
  --bg: #fbfbfa; --fg: #1a1a19; --muted: #6b6b68; --line: #e2e2df;
  --card: #ffffff; --accent: #2a5db0;
  --ok: #1a7f4b; --ok-bg: #e8f5ee;
  --bad: #b3261e; --bad-bg: #fdecea;
  --warn: #8a5a00; --warn-bg: #fdf3e0;
  --add: #1a7f4b; --del: #b3261e; --chg: #8a5a00;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #16171a; --fg: #e8e8e6; --muted: #9a9a97; --line: #2e3034;
    --card: #1d1f23; --accent: #7ea6ee;
    --ok: #63c98d; --ok-bg: #14301f;
    --bad: #f2857c; --bad-bg: #331815;
    --warn: #e0b25e; --warn-bg: #302512;
    --add: #63c98d; --del: #f2857c; --chg: #e0b25e;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 2rem 1.25rem 4rem; background: var(--bg); color: var(--fg);
  font: 15px/1.55 ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
}
main { max-width: 68rem; margin: 0 auto; }
h1 { font-size: 1.35rem; margin: 0 0 .35rem; font-weight: 650; }
h2 { font-size: 1rem; margin: 2rem 0 .6rem; font-weight: 650; letter-spacing: .01em; }
.paths { color: var(--muted); font-size: .85rem; margin: 0 0 1.25rem; word-break: break-all; }
.paths code { background: var(--card); border: 1px solid var(--line); border-radius: 4px; padding: .1rem .3rem; }
.verdict {
  display: flex; align-items: baseline; gap: .6rem; flex-wrap: wrap;
  padding: .85rem 1rem; border-radius: 8px; border: 1px solid var(--line);
  font-weight: 600; margin-bottom: 1rem;
}
.verdict.ok { background: var(--ok-bg); color: var(--ok); border-color: transparent; }
.verdict.bad { background: var(--bad-bg); color: var(--bad); border-color: transparent; }
.verdict.warn { background: var(--warn-bg); color: var(--warn); border-color: transparent; }
.verdict small { font-weight: 400; opacity: .85; }
.notes { margin: 0 0 1.25rem; padding: 0; list-style: none; font-size: .88rem; color: var(--muted); }
.notes li { margin: .2rem 0; }
.notes b { color: var(--fg); font-weight: 600; }
.tiles {
  display: grid; gap: .6rem; margin: 0 0 .5rem;
  grid-template-columns: repeat(auto-fit, minmax(8rem, 1fr));
}
.tile { background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: .7rem .85rem; }
.tile .n { font-size: 1.3rem; font-weight: 650; font-variant-numeric: tabular-nums; }
.tile .k { font-size: .75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
.tile.add .n { color: var(--add); }
.tile.del .n { color: var(--del); }
.tile.chg .n { color: var(--chg); }
.scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--card); }
table { border-collapse: collapse; width: 100%; font-size: .88rem; }
th, td { text-align: left; padding: .5rem .7rem; border-bottom: 1px solid var(--line); white-space: nowrap; }
th { font-weight: 600; color: var(--muted); font-size: .78rem; text-transform: uppercase; letter-spacing: .04em; }
tbody tr:last-child td { border-bottom: none; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .85em; }
th[data-sort] { cursor: pointer; user-select: none; }
th[data-sort]:hover { color: var(--fg); }
th[data-sort]::after { content: " \2195"; opacity: .35; }
th[data-sort].asc::after { content: " \2191"; opacity: 1; }
th[data-sort].desc::after { content: " \2193"; opacity: 1; }
.bar { display: flex; align-items: center; gap: .5rem; min-width: 9rem; }
.bar .track { flex: 1; height: 6px; background: var(--line); border-radius: 3px; overflow: hidden; }
.bar .fill { height: 100%; background: var(--accent); border-radius: 3px; }
.bar .pct { font-variant-numeric: tabular-nums; font-size: .8rem; color: var(--muted); min-width: 3.6rem; text-align: right; }
.left { color: var(--del); }
.right { color: var(--add); }
.keys { font-size: .85rem; word-break: break-all; }
.keys span { display: inline-block; background: var(--card); border: 1px solid var(--line); border-radius: 4px; padding: .05rem .35rem; margin: .1rem .2rem .1rem 0; }
.empty { color: var(--muted); font-size: .88rem; }
footer { margin-top: 2.5rem; color: var(--muted); font-size: .78rem; }
`

// htmlScript makes the tables sortable. Rows carry a data-sort-<n> attribute
// with the sortable form of each cell, so sorting never has to re-parse the
// rendered text.
const htmlScript = `
// Sort keys come from a cell's data-v when it has one, else its text. A
// column not declared numeric is still compared numerically when both cells
// happen to hold numbers, so a numeric key column sorts 90 < 100 rather
// than lexicographically.
function vennKey(cell) {
  var v = cell.getAttribute("data-v");
  return (v !== null ? v : cell.textContent).trim();
}
function vennNum(s) { return Number(String(s).replace(/,/g, "")); }
function vennCmp(x, y, numeric) {
  var nx = vennNum(x), ny = vennNum(y);
  var bothNum = x !== "" && y !== "" && !isNaN(nx) && !isNaN(ny);
  if (numeric || bothNum) {
    if (!bothNum) return numeric ? 0 : String(x).localeCompare(String(y));
    return nx - ny;
  }
  return String(x).localeCompare(String(y));
}
document.querySelectorAll("table").forEach(function (table) {
  table.querySelectorAll("th").forEach(function (th, i) {
    if (!th.hasAttribute("data-sort")) return;
    th.addEventListener("click", function () {
      var body = table.tBodies[0];
      var rows = Array.prototype.slice.call(body.rows);
      var desc = th.classList.contains("asc");
      table.querySelectorAll("th").forEach(function (o) { o.classList.remove("asc", "desc"); });
      th.classList.add(desc ? "desc" : "asc");
      var numeric = th.getAttribute("data-sort") === "num";
      rows.sort(function (a, b) {
        var c = vennCmp(vennKey(a.cells[i]), vennKey(b.cells[i]), numeric);
        return desc ? -c : c;
      });
      rows.forEach(function (r) { body.appendChild(r); });
    });
  });
});
`

func esc(s string) string { return html.EscapeString(s) }

// HTML writes the self-contained report. left/right label the inputs.
func HTML(w io.Writer, res *diff.Result, left, right string) {
	fmt.Fprintf(w, "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n")
	fmt.Fprintf(w, "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n")
	fmt.Fprintf(w, "<title>venn: %s vs %s</title>\n<style>%s</style>\n</head>\n<body>\n<main>\n",
		esc(left), esc(right), htmlStyle)

	fmt.Fprintf(w, "<h1>venn report</h1>\n")
	fmt.Fprintf(w, "<p class=\"paths\"><code>%s</code> &nbsp;vs&nbsp; <code>%s</code></p>\n",
		esc(left), esc(right))

	htmlVerdict(w, res)
	htmlNotes(w, res)
	htmlCounts(w, res)
	htmlSchema(w, res)
	htmlColumns(w, res)
	htmlExamples(w, res)

	fmt.Fprintf(w, "</main>\n<script>%s</script>\n</body>\n</html>\n", htmlScript)
}

func htmlVerdict(w io.Writer, res *diff.Result) {
	total := res.Added + res.Removed + res.Changed
	switch {
	case res.Aborted && res.PartialCounts:
		fmt.Fprintf(w, "<div class=\"verdict bad\">Stopped early <small>at least %s differing rows</small></div>\n",
			comma(total))
	case res.Aborted:
		fmt.Fprintf(w, "<div class=\"verdict bad\">%s differing rows <small>stopped before column attribution</small></div>\n",
			comma(total))
	case res.Same():
		fmt.Fprintf(w, "<div class=\"verdict ok\">Identical <small>%s rows compared, schema matches</small></div>\n",
			comma(res.Unchanged))
	case res.RowsSame():
		fmt.Fprintf(w, "<div class=\"verdict warn\">Rows identical, schema differs <small>%s rows compared</small></div>\n",
			comma(res.Unchanged))
	default:
		// the denominator is the union of both sides' keys: added and removed
		// rows differ without having been compared to anything
		fmt.Fprintf(w, "<div class=\"verdict bad\">%s differing rows <small>across %s rows</small></div>\n",
			comma(total), comma(res.ComparedRows()+res.Added+res.Removed))
	}
}

func htmlNotes(w io.Writer, res *diff.Result) {
	var notes []string
	if res.Filter != "" {
		note := "<b>Filtered to</b> <code>" + esc(res.Filter) +
			"</code> on both sides — every count is of the matching rows"
		if res.FilesPruned > 0 {
			note += fmt.Sprintf(" (%d data files skipped by partition value)", res.FilesPruned)
		}
		notes = append(notes, note)
	}
	if res.Comparison != "" {
		notes = append(notes, "<b>Compared with</b> "+esc(res.Comparison))
	}
	if len(res.Masked) > 0 {
		notes = append(notes, "<b>Masked columns</b> "+esc(strings.Join(res.Masked, ", "))+
			" — values shown as <code>xxh:</code> tokens")
	}
	if res.WithinTolerance > 0 {
		notes = append(notes, fmt.Sprintf("<b>%s rows</b> differ only within tolerance (not counted as changed)",
			comma(res.WithinTolerance)))
	}
	if res.DupKeys > 0 {
		notes = append(notes, fmt.Sprintf("<b>%s keys duplicated on the left</b> (%s rows), matched as multisets — "+
			"identical rows cancel, leftovers count as added/removed, and no change attribution is "+
			"attempted inside a group", comma(res.DupKeys), comma(res.DupRows)))
	}
	if res.DupsLeft > 0 || res.DupsRight > 0 {
		notes = append(notes, fmt.Sprintf("<b>Duplicate keys set aside</b> %s left, %s right (first occurrence kept)",
			comma(res.DupsLeft), comma(res.DupsRight)))
	}
	if res.Aborted {
		notes = append(notes, "<b>Stopped early</b> "+esc(res.AbortReason))
	}
	if len(notes) == 0 {
		return
	}
	fmt.Fprintf(w, "<ul class=\"notes\">\n")
	for _, n := range notes {
		fmt.Fprintf(w, "<li>%s</li>\n", n)
	}
	fmt.Fprintf(w, "</ul>\n")
}

func htmlCounts(w io.Writer, res *diff.Result) {
	ge := ""
	if res.PartialCounts {
		ge = "≥" // the scan was cancelled: these are lower bounds
	}
	tiles := []struct{ class, key, val string }{
		{"add", "added", ge + comma(res.Added)},
		{"del", "removed", ge + comma(res.Removed)},
		{"chg", "changed", ge + comma(res.Changed)},
		{"", "unchanged", comma(res.Unchanged)},
	}
	if res.WithinTolerance > 0 {
		tiles = append(tiles, struct{ class, key, val string }{"", "within tolerance", comma(res.WithinTolerance)})
	}
	scanned := ""
	if res.PartialCounts {
		scanned = " scanned"
	}
	tiles = append(tiles,
		struct{ class, key, val string }{"", "left rows" + scanned, comma(res.LeftRows)},
		struct{ class, key, val string }{"", "right rows" + scanned, comma(res.RightRows)})

	fmt.Fprintf(w, "<div class=\"tiles\">\n")
	for _, t := range tiles {
		cls := "tile"
		if t.class != "" {
			cls += " " + t.class
		}
		fmt.Fprintf(w, "<div class=\"%s\"><div class=\"n\">%s</div><div class=\"k\">%s</div></div>\n",
			cls, t.val, esc(t.key))
	}
	fmt.Fprintf(w, "</div>\n")
}

func htmlSchema(w io.Writer, res *diff.Result) {
	sd := &res.Schema
	if sd.Same() && len(sd.Renames) == 0 {
		return
	}
	fmt.Fprintf(w, "<h2>Schema</h2>\n<div class=\"scroll\"><table>\n")
	fmt.Fprintf(w, "<thead><tr><th data-sort=\"str\">Change</th><th data-sort=\"str\">Column</th><th>Detail</th></tr></thead>\n<tbody>\n")
	row := func(kind, col, detail string) {
		fmt.Fprintf(w, "<tr><td>%s</td><td><code>%s</code></td><td>%s</td></tr>\n",
			esc(kind), esc(col), detail)
	}
	for _, rn := range sd.Renames {
		row("renamed", rn.Left, "right input calls it <code>"+esc(rn.Right)+"</code>")
	}
	for _, c := range sd.AddedColumns {
		row("added", c, "right only")
	}
	for _, c := range sd.RemovedColumns {
		row("removed", c, "left only")
	}
	for _, tc := range sd.TypeChanges {
		detail := esc(tc.Left) + " &rarr; " + esc(tc.Right)
		if !tc.Comparable {
			detail += " <em>(not comparable — excluded from the row diff)</em>"
		}
		row("type", tc.Column, detail)
	}
	for _, nc := range sd.Nullability {
		row("nullability", nc.Column,
			fmt.Sprintf("nullable %v &rarr; %v", nc.LeftNullable, nc.RightNullable))
	}
	fmt.Fprintf(w, "</tbody>\n</table></div>\n")
}

func htmlColumns(w io.Writer, res *diff.Result) {
	cols := sortedColumnChanges(res)
	if len(cols) == 0 {
		return
	}
	numeric := false
	for _, c := range cols {
		if c.stat != nil && c.stat.Numeric {
			numeric = true
		}
	}
	fmt.Fprintf(w, "<h2>Columns</h2>\n<div class=\"scroll\"><table>\n<thead><tr>")
	fmt.Fprintf(w, "<th data-sort=\"str\">Column</th><th class=\"num\" data-sort=\"num\">Changed rows</th>")
	fmt.Fprintf(w, "<th data-sort=\"num\">Match rate</th>")
	if numeric {
		fmt.Fprintf(w, "<th class=\"num\" data-sort=\"num\">Max &Delta;</th><th class=\"num\" data-sort=\"num\">Mean &Delta;</th>")
	}
	fmt.Fprintf(w, "</tr></thead>\n<tbody>\n")
	for _, c := range cols {
		fmt.Fprintf(w, "<tr><td><code>%s</code></td><td class=\"num\" data-v=\"%d\">%s</td>",
			esc(c.name), c.n, comma(c.n))
		if c.stat == nil {
			fmt.Fprintf(w, "<td class=\"empty\">&mdash;</td>")
		} else {
			fmt.Fprintf(w, "<td data-v=\"%g\"><div class=\"bar\"><div class=\"track\">"+
				"<div class=\"fill\" style=\"width:%.2f%%\"></div></div><div class=\"pct\">%s</div></div></td>",
				c.stat.MatchRate, c.stat.MatchRate*100, pct(c.stat.MatchRate))
		}
		if numeric {
			if c.stat != nil && c.stat.Numeric {
				fmt.Fprintf(w, "<td class=\"num\" data-v=\"%g\">%s</td><td class=\"num\" data-v=\"%g\">%s</td>",
					c.stat.MaxAbsDiff, trimFloat(c.stat.MaxAbsDiff),
					c.stat.MeanAbsDiff, trimFloat(c.stat.MeanAbsDiff))
			} else {
				fmt.Fprintf(w, "<td class=\"num empty\" data-v=\"-1\">&mdash;</td><td class=\"num empty\" data-v=\"-1\">&mdash;</td>")
			}
		}
		fmt.Fprintf(w, "</tr>\n")
	}
	fmt.Fprintf(w, "</tbody>\n</table></div>\n")
}

func htmlExamples(w io.Writer, res *diff.Result) {
	if len(res.ChangedExamples) > 0 {
		fmt.Fprintf(w, "<h2>Changed rows <small class=\"empty\">(%d shown)</small></h2>\n", len(res.ChangedExamples))
		fmt.Fprintf(w, "<div class=\"scroll\"><table>\n<thead><tr>"+
			"<th data-sort=\"str\">Key</th><th data-sort=\"str\">Column</th>"+
			"<th>Left</th><th>Right</th></tr></thead>\n<tbody>\n")
		for _, ex := range res.ChangedExamples {
			for _, c := range ex.Columns {
				fmt.Fprintf(w, "<tr><td class=\"mono\">%s</td><td><code>%s</code></td>"+
					"<td class=\"mono left\">%s</td><td class=\"mono right\">%s</td></tr>\n",
					esc(ex.Key), esc(c.Column), esc(c.Left), esc(c.Right))
			}
		}
		fmt.Fprintf(w, "</tbody>\n</table></div>\n")
	}
	noun := "keys"
	if res.Keyless {
		noun = "rows" // there is no key to name a row by
	}
	htmlKeyList(w, "Added "+noun, res.AddedExamples, res.Added)
	htmlKeyList(w, "Removed "+noun, res.RemovedExamples, res.Removed)
	htmlKeyList(w, "Duplicate keys", res.DupExamples, res.DupsLeft+res.DupsRight+res.DupKeys)
	if n := int64(len(res.AddedExamples) + len(res.RemovedExamples) + len(res.ChangedExamples)); n > 0 &&
		n < res.Added+res.Removed+res.Changed {
		fmt.Fprintf(w, "<p class=\"empty\">Examples truncated — raise <code>--limit</code> for more.</p>\n")
	}
	fmt.Fprintf(w, "<footer>Generated by venn. Self-contained: no external requests.</footer>\n")
}

func htmlKeyList(w io.Writer, title string, keys []string, total int64) {
	if len(keys) == 0 {
		return
	}
	fmt.Fprintf(w, "<h2>%s <small class=\"empty\">(%d of %s)</small></h2>\n<p class=\"keys\">",
		esc(title), len(keys), comma(total))
	for _, k := range keys {
		fmt.Fprintf(w, "<span class=\"mono\">%s</span>", esc(k))
	}
	fmt.Fprintf(w, "</p>\n")
}
