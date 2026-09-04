package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type caseResult struct {
	Case  string  `json:"case"`
	Rows  int64   `json:"rows"`
	Runs  int     `json:"runs"`
	WallS float64 `json:"wall_s"`
	CPUS  float64 `json:"cpu_s"`
	RSSMB int64   `json:"rss_mb"`
	Gate  string  `json:"gate"` // OK | WRONG | ERROR
}

type runFile struct {
	Schema    int          `json:"schema"`
	Timestamp string       `json:"timestamp"`
	Commit    string       `json:"commit"`
	Dirty     bool         `json:"dirty"`
	GoVersion string       `json:"go_version"`
	OS        string       `json:"os"`
	Arch      string       `json:"arch"`
	Tier      string       `json:"tier"`
	Cases     []caseResult `json:"cases"`
}

func newRunFile(tier string, cases []caseResult) runFile {
	commit, dirty := gitState()
	return runFile{
		Schema:    1,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Commit:    commit,
		Dirty:     dirty,
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		Tier:      tier,
		Cases:     cases,
	}
}

func gitState() (string, bool) {
	out, err := exec.Command("git", "rev-parse", "--short=12", "HEAD").Output()
	if err != nil {
		return "unknown", false
	}
	commit := strings.TrimSpace(string(out))
	st, err := exec.Command("git", "status", "--porcelain").Output()
	return commit, err == nil && len(strings.TrimSpace(string(st))) > 0
}

func saveResults(dir string, rf runFile) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	suffix := ""
	if rf.Dirty {
		suffix = "-dirty"
	}
	name := fmt.Sprintf("%s-%s-%s%s.json",
		time.Now().UTC().Format("20060102-150405"), rf.Tier, rf.Commit, suffix)
	p := filepath.Join(dir, name)
	b, err := json.MarshalIndent(rf, "", " ")
	if err != nil {
		return "", err
	}
	return p, os.WriteFile(p, append(b, '\n'), 0o644)
}

func loadBaseline(path string) (map[string]caseResult, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rf runFile
	if err := json.Unmarshal(b, &rf); err != nil {
		return nil, err
	}
	return indexResults(rf.Cases), nil
}

func indexResults(cs []caseResult) map[string]caseResult {
	m := map[string]caseResult{}
	for _, c := range cs {
		m[c.Case] = c
	}
	return m
}

func fmtRows(n int64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dk", n/1_000)
	}
	return fmt.Sprintf("%d", n)
}

// renderTable prints the single-binary results table.
func renderTable(w *strings.Builder, cases []caseResult) {
	fmt.Fprintf(w, "%-26s %6s %5s %9s %9s %8s  %s\n",
		"case", "rows", "runs", "wall", "cpu", "rss", "gate")
	for _, c := range cases {
		fmt.Fprintf(w, "%-26s %6s %5d %8.3fs %8.3fs %6dMB  %s\n",
			c.Case, fmtRows(c.Rows), c.Runs, c.WallS, c.CPUS, c.RSSMB, c.Gate)
	}
}

// compareOpts are the regression gate thresholds. Per-case gates only apply
// above the CPU floor: below it, process startup and timer noise dominate.
type compareOpts struct {
	maxCaseCPU    float64 // per-case CPU ratio limit
	maxGeomeanCPU float64
	maxCaseRSS    float64
	maxGeomeanRSS float64
	floorCPUms    float64
}

func defaultCompareOpts() compareOpts {
	return compareOpts{maxCaseCPU: 1.25, maxGeomeanCPU: 1.10, maxCaseRSS: 1.35, maxGeomeanRSS: 1.15, floorCPUms: 20}
}

type comparison struct {
	rows   []compareRow
	failed []string
}

type compareRow struct {
	name               string
	curCPU, refCPU     float64
	curRSS, refRSS     int64
	cpuRatio, rssRatio float64
	skipped            bool
	verdict            string
}

func compare(cur []caseResult, ref map[string]caseResult, o compareOpts) comparison {
	var cmp comparison
	var cpuRatios, rssRatios []float64
	for _, c := range cur {
		r, ok := ref[c.Case]
		row := compareRow{name: c.Name(), curCPU: c.CPUS, curRSS: c.RSSMB}
		if !ok || r.Gate != "OK" || c.Gate != "OK" {
			row.skipped = true
			row.verdict = "n/a"
			cmp.rows = append(cmp.rows, row)
			continue
		}
		row.refCPU, row.refRSS = r.CPUS, r.RSSMB
		row.cpuRatio = ratio(c.CPUS, r.CPUS)
		row.rssRatio = ratio(float64(c.RSSMB), float64(r.RSSMB))
		row.verdict = "ok"
		if r.CPUS*1000 >= o.floorCPUms {
			cpuRatios = append(cpuRatios, row.cpuRatio)
			rssRatios = append(rssRatios, row.rssRatio)
			if row.cpuRatio > o.maxCaseCPU {
				row.verdict = "CPU REGRESSION"
				cmp.failed = append(cmp.failed, fmt.Sprintf("%s: cpu %.3fs -> %.3fs (%.0f%%)",
					c.Case, r.CPUS, c.CPUS, (row.cpuRatio-1)*100))
			} else if row.rssRatio > o.maxCaseRSS {
				row.verdict = "RSS REGRESSION"
				cmp.failed = append(cmp.failed, fmt.Sprintf("%s: rss %dMB -> %dMB (%.0f%%)",
					c.Case, r.RSSMB, c.RSSMB, (row.rssRatio-1)*100))
			}
		} else {
			row.verdict = "ok (below floor)"
		}
		cmp.rows = append(cmp.rows, row)
	}
	if g := geomean(cpuRatios); g > o.maxGeomeanCPU {
		cmp.failed = append(cmp.failed, fmt.Sprintf("geomean cpu ratio %.3f exceeds %.3f", g, o.maxGeomeanCPU))
	}
	if g := geomean(rssRatios); g > o.maxGeomeanRSS {
		cmp.failed = append(cmp.failed, fmt.Sprintf("geomean rss ratio %.3f exceeds %.3f", g, o.maxGeomeanRSS))
	}
	return cmp
}

// Name lets compareRow reuse caseResult naming without exporting more fields.
func (c caseResult) Name() string { return c.Case }

func ratio(cur, ref float64) float64 {
	if ref <= 0 {
		return 1
	}
	return cur / ref
}

func geomean(xs []float64) float64 {
	if len(xs) == 0 {
		return 1
	}
	s := 0.0
	for _, x := range xs {
		s += math.Log(x)
	}
	return math.Exp(s / float64(len(xs)))
}

func renderComparison(w *strings.Builder, refName string, cmp comparison) {
	fmt.Fprintf(w, "%-26s %9s %9s %7s %8s %8s %7s  %s\n",
		"case", "cpu(ref)", "cpu(cur)", "Δcpu", "rss(ref)", "rss(cur)", "Δrss", "verdict")
	for _, r := range cmp.rows {
		if r.skipped {
			fmt.Fprintf(w, "%-26s %9s %8.3fs %7s %8s %6dMB %7s  %s\n",
				r.name, "-", r.curCPU, "-", "-", r.curRSS, "-", r.verdict)
			continue
		}
		fmt.Fprintf(w, "%-26s %8.3fs %8.3fs %+6.1f%% %6dMB %6dMB %+6.1f%%  %s\n",
			r.name, r.refCPU, r.curCPU, (r.cpuRatio-1)*100,
			r.refRSS, r.curRSS, (r.rssRatio-1)*100, r.verdict)
	}
	if len(cmp.failed) > 0 {
		fmt.Fprintf(w, "\nREGRESSIONS vs %s:\n", refName)
		for _, f := range cmp.failed {
			fmt.Fprintf(w, "  - %s\n", f)
		}
	} else {
		fmt.Fprintf(w, "\nno regressions vs %s\n", refName)
	}
}

// markdownComparison renders the A/B table for GitHub step summaries.
func markdownComparison(refName string, cmp comparison) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### tdiff perf: current vs `%s`\n\n", refName)
	b.WriteString("| case | cpu ref | cpu cur | Δcpu | rss ref | rss cur | Δrss | verdict |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	for _, r := range cmp.rows {
		if r.skipped {
			fmt.Fprintf(&b, "| %s | - | %.3fs | - | - | %dMB | - | %s |\n", r.name, r.curCPU, r.curRSS, r.verdict)
			continue
		}
		fmt.Fprintf(&b, "| %s | %.3fs | %.3fs | %+.1f%% | %dMB | %dMB | %+.1f%% | %s |\n",
			r.name, r.refCPU, r.curCPU, (r.cpuRatio-1)*100, r.refRSS, r.curRSS, (r.rssRatio-1)*100, r.verdict)
	}
	b.WriteString("\n")
	if len(cmp.failed) > 0 {
		b.WriteString("**Regressions:**\n\n")
		for _, f := range cmp.failed {
			fmt.Fprintf(&b, "- %s\n", f)
		}
	} else {
		b.WriteString("No regressions detected.\n")
	}
	return b.String()
}

func writeStepSummary(md string) {
	p := os.Getenv("GITHUB_STEP_SUMMARY")
	if p == "" {
		return
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(md)
}
