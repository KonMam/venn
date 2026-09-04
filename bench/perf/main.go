// perf is venn's performance-regression suite (in the spirit of SQLite's
// speedtest1 and TigerBeetle's devhub): a fixed, deterministic workload matrix
// over every command, format, and data shape, correctness-gated before any
// timing, measured by CPU time + peak RSS, with results stored as JSON and an
// interleaved A/B mode against any git ref for noise-immune regression gating.
//
// Usage:
//
//	go run ./bench/perf -tier smoke                  # measure current tree
//	go run ./bench/perf -tier smoke -against main    # A/B vs a ref
//	go run ./bench/perf -check -against main         # exit 1 on regression
//	go run ./bench/perf -baseline results/x.json     # compare vs saved run
//	go run ./bench/perf -list                        # show the case matrix
//
// Tiers: tiny (10k, harness self-test), smoke (1M, CI), standard (10M,
// pre-release), large (100M, opt-in). Fixtures are generated on demand into
// bench/perf/cache and reused.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/KonMam/venn/internal/fixture"
)

var exeSuffix = map[string]string{"windows": ".exe"}[runtime.GOOS]

type counts struct {
	Added   int64 `json:"added"`
	Removed int64 `json:"removed"`
	Changed int64 `json:"changed"`
}

type side struct {
	name string // "cur" | ref name
	bin  string
	// snapshots built by this binary, per dataset dir (a binary must read its
	// own .snap: the snapshot format is not covered by the A/B contract)
	snaps map[string]string
}

func main() {
	os.Exit(run())
}

func run() int {
	tier := flag.String("tier", "smoke", "tiny | smoke | standard | large")
	filter := flag.String("filter", "", "regexp selecting case names")
	list := flag.Bool("list", false, "list selected cases and exit")
	runsOverride := flag.Int("runs", 0, "timed runs per case per binary (0 = tier default)")
	against := flag.String("against", "", "git ref to A/B against (built in a worktree)")
	baseline := flag.String("baseline", "", "results JSON to compare against (same machine!)")
	check := flag.Bool("check", false, "exit 1 when the comparison shows a regression")
	cacheDir := flag.String("cache-dir", "", "fixture cache (default <repo>/bench/perf/cache)")
	resultsDir := flag.String("results-dir", "", "results dir (default <repo>/bench/perf/results)")
	timeout := flag.Duration("timeout", 10*time.Minute, "per-run timeout")
	maxCaseCPU := flag.Float64("max-case-cpu", 1.25, "per-case CPU ratio limit for -check")
	maxGeoCPU := flag.Float64("max-geomean-cpu", 1.10, "geomean CPU ratio limit for -check")
	flag.Parse()

	ts, ok := tiers[*tier]
	if !ok {
		fmt.Fprintf(os.Stderr, "perf: unknown tier %q\n", *tier)
		return 2
	}
	runs := ts.runs
	if *runsOverride > 0 {
		runs = *runsOverride
	}

	cases, err := selectCases(*tier, *filter)
	if err != nil {
		fmt.Fprintln(os.Stderr, "perf:", err)
		return 2
	}
	if *list {
		for _, c := range cases {
			k := c.dataset(ts)
			fmt.Printf("%-26s %-16s %s\n", c.Name, c.Workload, k.dirName())
		}
		return 0
	}

	root, err := gitRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "perf:", err)
		return 2
	}
	cache := *cacheDir
	if cache == "" {
		cache = filepath.Join(root, "bench", "perf", "cache")
	}
	results := *resultsDir
	if results == "" {
		results = filepath.Join(root, "bench", "perf", "results")
	}
	workDir := filepath.Join(cache, "work")
	if err := os.RemoveAll(workDir); err != nil {
		fmt.Fprintln(os.Stderr, "perf:", err)
		return 2
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "perf:", err)
		return 2
	}

	// fixtures
	needed := datasetFormats(cases, ts)
	dsDirs := map[datasetKey]string{}
	manifests := map[datasetKey]*fixture.Manifest{}
	for k, formats := range needed {
		dir, man, err := ensureDataset(cache, k, formats)
		if err != nil {
			fmt.Fprintln(os.Stderr, "perf:", err)
			return 2
		}
		dsDirs[k], manifests[k] = dir, man
	}

	// binaries
	cur := &side{name: "cur", snaps: map[string]string{}}
	cur.bin = filepath.Join(workDir, "venn-cur"+exeSuffix)
	fmt.Println("[build] current tree")
	if err := buildVenn(root, cur.bin); err != nil {
		fmt.Fprintln(os.Stderr, "perf: build:", err)
		return 2
	}
	sides := []*side{cur}
	if *against != "" {
		ref := &side{name: *against, snaps: map[string]string{}}
		ref.bin = filepath.Join(workDir, "venn-ref"+exeSuffix)
		fmt.Printf("[build] %s (worktree)\n", *against)
		if err := buildRef(root, *against, ref.bin, workDir); err != nil {
			fmt.Fprintln(os.Stderr, "perf: build ref:", err)
			return 2
		}
		sides = append(sides, ref)
	}

	// measure
	perSide := map[string][]caseResult{}
	gateFailed := false
	for _, c := range cases {
		k := c.dataset(ts)
		dsDir, man := dsDirs[k], manifests[k]
		fmt.Printf("[case] %s (%s rows)\n", c.Name, fmtRows(k.Rows))
		active := make([]*side, 0, len(sides))
		gates := map[string]string{}
		for _, s := range sides {
			gate, err := gateCase(c, s, dsDir, workDir, man, *timeout)
			gates[s.name] = gate
			if gate != "OK" {
				msg := gate
				if err != nil {
					msg += ": " + err.Error()
				}
				fmt.Printf("  gate %-4s %s\n", s.name, msg)
				if s == cur {
					gateFailed = true
				}
				perSide[s.name] = append(perSide[s.name], caseResult{Case: c.Name, Rows: k.Rows, Gate: gate})
				continue
			}
			fmt.Printf("  gate %-4s OK\n", s.name)
			active = append(active, s)
		}
		aggs := map[string]*agg{}
		for _, s := range active {
			aggs[s.name] = &agg{}
		}
		for i := 0; i < runs; i++ {
			for _, s := range active { // interleaved: A B A B ... same machine, same moment
				m, err := timedRun(c, s, dsDir, workDir, *timeout)
				if err != nil {
					fmt.Printf("  run %-4s error: %v\n", s.name, err)
					gates[s.name] = "ERROR"
					if s == cur {
						gateFailed = true
					}
					break
				}
				aggs[s.name].add(m)
			}
		}
		for _, s := range active {
			a := aggs[s.name]
			cr := caseResult{
				Case: c.Name, Rows: k.Rows, Runs: a.n, Gate: gates[s.name],
				WallS: a.wall.Seconds(), CPUS: a.cpu.Seconds(), RSSMB: a.rss / (1 << 20),
			}
			cr.WallS = round3(cr.WallS)
			cr.CPUS = round3(cr.CPUS)
			perSide[s.name] = append(perSide[s.name], cr)
			fmt.Printf("  %-6s wall %.3fs  cpu %.3fs  rss %dMB\n", s.name, cr.WallS, cr.CPUS, cr.RSSMB)
		}
	}

	// report
	rf := newRunFile(*tier, perSide["cur"])
	path, err := saveResults(results, rf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "perf: save:", err)
	} else {
		fmt.Println("\nresults:", path)
	}
	var out strings.Builder
	out.WriteString("\n")
	renderTable(&out, perSide["cur"])
	fmt.Print(out.String())

	exit := 0
	if gateFailed {
		fmt.Fprintln(os.Stderr, "\nperf: correctness gate failed (see WRONG/ERROR above)")
		exit = 1
	}

	var ref map[string]caseResult
	refName := ""
	if *against != "" {
		ref = indexResults(perSide[*against])
		refName = *against
	} else if *baseline != "" {
		var err error
		ref, err = loadBaseline(*baseline)
		if err != nil {
			fmt.Fprintln(os.Stderr, "perf: baseline:", err)
			return 2
		}
		refName = *baseline
	}
	if ref != nil {
		opts := defaultCompareOpts()
		opts.maxCaseCPU = *maxCaseCPU
		opts.maxGeomeanCPU = *maxGeoCPU
		cmp := compare(perSide["cur"], ref, opts)
		var cb strings.Builder
		cb.WriteString("\n")
		renderComparison(&cb, refName, cmp)
		fmt.Print(cb.String())
		writeStepSummary(markdownComparison(refName, cmp))
		if *check && len(cmp.failed) > 0 {
			exit = 1
		}
	}
	return exit
}

func selectCases(tier, filter string) ([]Case, error) {
	var re *regexp.Regexp
	if filter != "" {
		var err error
		re, err = regexp.Compile(filter)
		if err != nil {
			return nil, fmt.Errorf("bad -filter: %w", err)
		}
	}
	var out []Case
	for _, c := range allCases {
		c = c.normalized()
		if tier == "large" && !c.LargeOK {
			continue
		}
		if re != nil && !re.MatchString(c.Name) {
			continue
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no cases match")
	}
	return out, nil
}

func gitRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("not in a git repo: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func buildVenn(srcRoot, out string) error {
	cmd := exec.Command("go", "build", "-o", out, "./cmd/venn")
	cmd.Dir = srcRoot
	if b, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v\n%s", err, b)
	}
	return nil
}

func buildRef(root, ref, out, scratch string) error {
	wt := filepath.Join(scratch, "ref-worktree")
	add := exec.Command("git", "worktree", "add", "--detach", wt, ref)
	add.Dir = root
	if b, err := add.CombinedOutput(); err != nil {
		return fmt.Errorf("worktree add %s: %v\n%s", ref, err, b)
	}
	defer func() {
		rm := exec.Command("git", "worktree", "remove", "--force", wt)
		rm.Dir = root
		_ = rm.Run()
	}()
	return buildVenn(wt, out)
}

// paths a case run touches for a given side.
func casePaths(c Case, s *side, dsDir, workDir string) (snapPath, outPath string) {
	base := c.Name + "-" + sanitize(s.name)
	if c.needsSnapshot() {
		snapPath = s.snaps[dsDir]
	} else if c.Workload == "snapshot-create" {
		snapPath = filepath.Join(workDir, base+".snap")
	}
	if ext := c.exportExt(); ext != "" {
		outPath = filepath.Join(workDir, base+".out."+ext)
	}
	return
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '.' {
			return r
		}
		return '_'
	}, s)
}

// gateCase runs the case once with output captured and validates correctness
// against the fixture manifest. Only gated tools get timed: a fast wrong
// answer is worthless (and a silent one is dangerous).
func gateCase(c Case, s *side, dsDir, workDir string, man *fixture.Manifest, timeout time.Duration) (string, error) {
	if c.needsSnapshot() {
		if err := ensureSnap(c, s, dsDir, workDir, timeout); err != nil {
			return "ERROR", err
		}
	}
	snapPath, outPath := casePaths(c, s, dsDir, workDir)
	cleanupRunOutputs(snapPath, outPath, c)
	m, err := runOnce(s.bin, c.args(dsDir, workDir, snapPath, outPath), true, timeout)
	if err != nil {
		return "ERROR", err
	}
	if m.Exit != 0 && m.Exit != 1 {
		return "ERROR", fmt.Errorf("exit %d: %s", m.Exit, tail(m.Stderr, 300))
	}
	switch c.Workload {
	case "schema", "snapshot-create":
		if m.Exit != 0 {
			return "WRONG", fmt.Errorf("expected exit 0, got %d", m.Exit)
		}
		if c.Workload == "snapshot-create" && !fileExists(snapPath) {
			return "WRONG", fmt.Errorf("snapshot file not written")
		}
		return "OK", nil
	}
	var got counts
	if err := json.Unmarshal([]byte(m.Stdout), &got); err != nil {
		return "ERROR", fmt.Errorf("unparseable JSON output: %v", err)
	}
	want := counts{Added: man.Added, Removed: man.Removed, Changed: man.Changed}
	if got != want {
		return "WRONG", fmt.Errorf("got %+v want %+v", got, want)
	}
	if ext := c.exportExt(); ext != "" {
		wantRows := man.Added + man.Removed + man.Changed
		gotRows, err := exportRows(outPath, ext)
		if err != nil {
			return "ERROR", err
		}
		if gotRows != wantRows {
			return "WRONG", fmt.Errorf("export has %d rows, want %d", gotRows, wantRows)
		}
	}
	return "OK", nil
}

func ensureSnap(c Case, s *side, dsDir, workDir string, timeout time.Duration) error {
	if s.snaps[dsDir] != "" {
		return nil
	}
	p := filepath.Join(workDir, "base-"+sanitize(s.name)+"-"+filepath.Base(dsDir)+".snap")
	m, err := runOnce(s.bin, []string{"snapshot", filepath.Join(dsDir, "left."+c.Left),
		"--key", "id", "--output", p}, true, timeout)
	if err != nil {
		return err
	}
	if m.Exit != 0 {
		return fmt.Errorf("snapshot build exit %d: %s", m.Exit, tail(m.Stderr, 300))
	}
	s.snaps[dsDir] = p
	return nil
}

func timedRun(c Case, s *side, dsDir, workDir string, timeout time.Duration) (runMetrics, error) {
	snapPath, outPath := casePaths(c, s, dsDir, workDir)
	cleanupRunOutputs(snapPath, outPath, c)
	m, err := runOnce(s.bin, c.args(dsDir, workDir, snapPath, outPath), false, timeout)
	if err != nil {
		return m, err
	}
	if m.Exit != 0 && m.Exit != 1 {
		return m, fmt.Errorf("exit %d: %s", m.Exit, tail(m.Stderr, 300))
	}
	return m, nil
}

// cleanupRunOutputs removes files a run writes, so each run starts clean.
func cleanupRunOutputs(snapPath, outPath string, c Case) {
	if outPath != "" {
		_ = os.Remove(outPath)
	}
	if c.Workload == "snapshot-create" && snapPath != "" {
		_ = os.Remove(snapPath)
	}
}

func exportRows(path, ext string) (int64, error) {
	if ext == "csv" {
		b, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		n := int64(0)
		for _, ch := range b {
			if ch == '\n' {
				n++
			}
		}
		return n - 1, nil // header
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

func round3(f float64) float64 { return float64(int64(f*1000+0.5)) / 1000 }
