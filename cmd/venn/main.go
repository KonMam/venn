// venn — a fast, single-binary tabular data differ.
//
//	venn a.parquet b.csv --key id     keyed row diff across formats
//	venn schema a.parquet b.csv       schema diff only
//
// Exit codes: 0 inputs equal, 1 differences found, 2 error.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
	"runtime/debug"
	"runtime/pprof"
	"strings"

	"venn/internal/diff"
	"venn/internal/output"
	"venn/internal/schema"
	"venn/internal/source"
)

const version = "0.1.0-dev"

func usage() {
	fmt.Fprintf(os.Stderr, `venn %s — diff tabular data files (parquet, csv, tsv), in any combination

usage:
  venn <left> <right> --key <col>[,<col>...] [flags]   row + schema diff
  venn schema <left> <right> [flags]                   schema diff only
  venn snapshot <file> --key <col> --output <b.snap>   save a hash baseline
  venn <file> --against <b.snap>                       diff vs the baseline

flags:
  --key <cols>             key column(s), comma-separated; omitted = inferred
                           (a column unique in both inputs, id-ish names first)
  --ignore-columns <cols>  columns to exclude from comparison
  --format human|json      output format (default human)
  --limit <n>              max example rows shown per category (default 10)
  --verbose                print example rows
  --summary                counts + exit code only (fastest mode)
  --mode auto|memory|stream  join strategy; stream spills hashes to disk and
                           keeps peak memory flat for larger-than-RAM inputs
  --tmpdir <dir>           spill directory for stream mode
  --infer-rows <n>         CSV type-inference sample size (default 1000; -1 = whole file)
  --on-dup error|warn      duplicate keys: fail (default) or keep first per side
  --output <file>          write differing rows as data (.csv or .parquet):
                           key cols, diff_status, <col>__left/<col>__right
  --max-diff <n | p%%>     CI gate: exit 0 while total differing rows stay
                           within budget (schema changes still exit 1)
  --float-precision <n>    round float comparisons to n decimal digits
                           (quantization: exact and hash-consistent, unlike
                           an epsilon)
  --version                print version

exit codes: 0 inputs equal · 1 differences found · 2 error
`, version)
}

func main() {
	// Page-decode buffers cycle through pools quickly; the default GC target
	// (100) collects so often that the pools drain and spans bounce between
	// the heap and the OS. A higher target costs no measurable RSS here
	// because the live set (join table + in-flight pages) is what it is.
	// GOGC set explicitly in the environment still wins. (150 balances the
	// kernel path’s buffer reuse against peak-RSS growth.)
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(150)
	}
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("venn", flag.ContinueOnError)
	fs.Usage = usage
	key := fs.String("key", "", "key column(s), comma-separated")
	ignore := fs.String("ignore-columns", "", "columns to exclude, comma-separated")
	format := fs.String("format", "human", "output format: human or json")
	limit := fs.Int("limit", 10, "max examples per category")
	verbose := fs.Bool("verbose", false, "print example rows")
	summary := fs.Bool("summary", false, "counts and exit code only (fastest; skips column attribution and examples)")
	mode := fs.String("mode", "auto", "join strategy: auto, memory, or stream (constant-memory grace hash join)")
	tmpdir := fs.String("tmpdir", "", "spill directory for --mode stream (default: system temp)")
	inferRows := fs.Int("infer-rows", 0, "CSV type-inference sample rows (default 1000; -1 = whole file)")
	onDup := fs.String("on-dup", "error", "duplicate keys: error, or warn (keep first occurrence per side)")
	outFile := fs.String("output", "", "write the differing rows as data to this .csv or .parquet file")
	against := fs.String("against", "", "diff a single file against a snapshot baseline (.snap)")
	maxDiff := fs.String("max-diff", "", "CI gate: exit 0 while added+removed+changed stays within this budget (a count like 1000, or a percentage like 0.5%)")
	floatPrec := fs.Int("float-precision", 0, "round float comparisons to N decimal digits (0 = exact)")
	showVersion := fs.Bool("version", false, "print version")
	cpuProfile := fs.String("cpuprofile", "", "write CPU profile to file (dev)")
	memProfile := fs.String("memprofile", "", "write heap profile to file (dev)")

	// Accept flags before or after positional args.
	var pos []string
	rest := args
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		pos = append(pos, rest[0])
		rest = rest[1:]
	}

	if *showVersion {
		fmt.Println("venn", version)
		return 0
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			return fail(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fail(err)
		}
		defer pprof.StopCPUProfile()
	}

	schemaOnly := false
	snapshotCmd := false
	if len(pos) > 0 && pos[0] == "schema" {
		schemaOnly = true
		pos = pos[1:]
	}
	if len(pos) > 0 && pos[0] == "snapshot" {
		snapshotCmd = true
		pos = pos[1:]
	}
	if snapshotCmd {
		if len(pos) != 1 || *outFile == "" {
			fmt.Fprintln(os.Stderr, "usage: venn snapshot <file> --key <col> --output <base.snap>")
			return 2
		}
		return runSnapshot(pos[0], *outFile, *key, *inferRows, *floatPrec, *onDup)
	}
	if *against != "" {
		if len(pos) != 1 {
			fmt.Fprintln(os.Stderr, "usage: venn <file> --against <base.snap>")
			return 2
		}
		return runAgainst(pos[0], *against, *inferRows, *limit, *onDup, *format)
	}
	if len(pos) != 2 {
		usage()
		return 2
	}

	srcOpts := source.Options{InferRows: *inferRows}
	left, err := source.OpenWith(pos[0], srcOpts)
	if err != nil {
		return fail(err)
	}
	defer left.Close()
	right, err := source.OpenWith(pos[1], srcOpts)
	if err != nil {
		return fail(err)
	}
	defer right.Close()
	printWarnings(pos[0], left)
	printWarnings(pos[1], right)

	if schemaOnly {
		sd := schema.Compare(left.Schema(), right.Schema())
		if *format == "json" {
			if err := output.SchemaJSON(os.Stdout, &sd); err != nil {
				return fail(err)
			}
		} else {
			output.SchemaHuman(os.Stdout, &sd)
		}
		if sd.Same() {
			return 0
		}
		return 1
	}

	if *onDup != "error" && *onDup != "warn" {
		return fail(fmt.Errorf("--on-dup must be error or warn, got %q", *onDup))
	}
	opts := diff.Options{Limit: *limit, Summary: *summary, Mode: *mode, TempDir: *tmpdir, OnDup: *onDup, FloatPrecision: *floatPrec}
	if *key != "" {
		opts.Keys = splitList(*key)
	} else {
		inferred, ierr := diff.InferKey(left, right)
		if ierr != nil {
			return fail(fmt.Errorf("no --key given and none could be inferred: %w", ierr))
		}
		fmt.Fprintf(os.Stderr, "venn: using inferred key column %q (pass --key to override)\n", inferred)
		opts.Keys = []string{inferred}
	}
	if *ignore != "" {
		opts.IgnoreColumns = splitList(*ignore)
	}
	// Dirty-data recovery: a value contradicting an inferred CSV column type
	// demotes that column to string and the diff restarts (bounded by the
	// column count). Sampled inference cannot see the whole file; refusing
	// to diff over one stray cell would be worse than the restart.
	if st, serr := os.Stderr.Stat(); serr == nil && st.Mode()&os.ModeCharDevice != 0 {
		prog := &diff.Progress{}
		opts.Progress = prog
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			tick := time.NewTicker(500 * time.Millisecond)
			defer tick.Stop()
			printed := false
			for {
				select {
				case <-stop:
					if printed {
						fmt.Fprintf(os.Stderr, "\r\033[K")
					}
					return
				case <-tick.C:
					phase, rows := prog.Snapshot()
					if phase != "" {
						fmt.Fprintf(os.Stderr, "\r\033[K%s: %s rows…", phase, humanCount(rows))
						printed = true
					}
				}
			}
		}()
	}

	var res *diff.Result
	for attempt := 0; ; attempt++ {
		var closeSink func() error
		if *outFile != "" {
			kn, kt, vn, vt, rerr := diff.ResolveColumns(left.Schema(), right.Schema(), opts)
			if rerr != nil {
				return fail(rerr)
			}
			sink, closer, serr := output.NewExport(*outFile, kn, kt, vn, vt)
			if serr != nil {
				return fail(serr)
			}
			opts.Sink, closeSink = sink, closer
		}
		res, err = diff.Run(left, right, opts)
		if err == nil {
			if closeSink != nil {
				if cerr := closeSink(); cerr != nil {
					return fail(cerr)
				}
			}
			break
		}
		if closeSink != nil {
			closeSink()
		}
		var coerce *source.TypeCoercionError
		if !errors.As(err, &coerce) || attempt > 64 {
			return fail(err)
		}
		retyped := false
		for _, s := range []source.Source{left, right} {
			if rt, ok := s.(source.Retypeable); ok && rt.ForceStringColumn(coerce.Column) {
				retyped = true
			}
		}
		if !retyped {
			return fail(err)
		}
		fmt.Fprintf(os.Stderr, "venn: warning: %v — re-reading column %q as string\n", coerce, coerce.Column)
	}
	if *format == "json" {
		if err := output.JSON(os.Stdout, res); err != nil {
			return fail(err)
		}
	} else {
		output.Human(os.Stdout, res, *verbose)
	}
	if *memProfile != "" {
		f, ferr := os.Create(*memProfile)
		if ferr == nil {
			pprof.Lookup("heap").WriteTo(f, 0)
			f.Close()
		}
	}
	if res.Same() {
		return 0
	}
	if *maxDiff != "" && res.Schema.Same() {
		budget, berr := parseBudget(*maxDiff, max(res.LeftRows, res.RightRows))
		if berr != nil {
			return fail(berr)
		}
		total := res.Added + res.Removed + res.Changed
		if total <= budget {
			fmt.Fprintf(os.Stderr, "venn: %d differing rows within --max-diff budget of %d\n", total, budget)
			return 0
		}
	}
	return 1
}

// parseBudget turns "1000" or "0.5%" into an absolute row budget.
// runSnapshot creates a hash-manifest baseline of one file.
func runSnapshot(path, out, key string, inferRows, floatPrec int, onDup string) int {
	src, err := source.OpenWith(path, source.Options{InferRows: inferRows})
	if err != nil {
		return fail(err)
	}
	defer src.Close()
	opts := diff.Options{FloatPrecision: floatPrec, OnDup: onDup}
	if key != "" {
		opts.Keys = splitList(key)
	} else {
		k, kerr := diff.InferKey(src, src)
		if kerr != nil {
			return fail(fmt.Errorf("no --key given and none could be inferred: %w", kerr))
		}
		fmt.Fprintf(os.Stderr, "venn: using inferred key column %q\n", k)
		opts.Keys = []string{k}
	}
	rows, err := diff.WriteSnapshot(src, out, opts)
	if err != nil {
		return fail(err)
	}
	st, _ := os.Stat(out)
	fmt.Printf("snapshot: %s rows -> %s (%s)\n", humanCount(rows), out, humanBytes(st.Size()))
	return 0
}

// runAgainst diffs a live file against a snapshot baseline.
func runAgainst(path, snap string, inferRows, limit int, onDup, format string) int {
	src, err := source.OpenWith(path, source.Options{InferRows: inferRows})
	if err != nil {
		return fail(err)
	}
	defer src.Close()
	res, err := diff.DiffAgainstSnapshot(snap, src, diff.Options{Limit: limit, OnDup: onDup})
	if err != nil {
		return fail(err)
	}
	if format == "json" {
		if err := output.JSON(os.Stdout, res); err != nil {
			return fail(err)
		}
	} else {
		fmt.Printf("baseline: %s\n", snap)
		output.Human(os.Stdout, res, false)
		if res.Removed > 0 {
			fmt.Println("(removed keys are not recoverable from a snapshot: counts only)")
		}
	}
	if res.RowsSame() {
		return 0
	}
	return 1
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
}

// humanCount renders large counts compactly (12.3M).
func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 10_000:
		return fmt.Sprintf("%.0fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func parseBudget(s string, rows int64) (int64, error) {
	if strings.HasSuffix(s, "%") {
		pct, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
		if err != nil || pct < 0 {
			return 0, fmt.Errorf("bad --max-diff percentage %q", s)
		}
		return int64(pct / 100 * float64(rows)), nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad --max-diff %q (want a count or percentage)", s)
	}
	return n, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fail(err error) int {
	fmt.Fprintln(os.Stderr, "venn:", err)
	return 2
}

// printWarnings surfaces reader warnings (skipped columns etc.) on stderr.
func printWarnings(path string, s source.Source) {
	if w, ok := s.(interface{ Warnings() []string }); ok {
		for _, msg := range w.Warnings() {
			fmt.Fprintf(os.Stderr, "venn: warning: %s: %s\n", path, msg)
		}
	}
}
