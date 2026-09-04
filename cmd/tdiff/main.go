// tdiff is a fast, single-binary tabular data differ.
//
//	tdiff a.parquet b.csv --key id     keyed row diff across formats
//	tdiff schema a.parquet b.csv       schema diff only
//
// Exit codes: 0 inputs equal, 1 differences found, 2 error.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/KonMam/tdiff/internal/diff"
	"github.com/KonMam/tdiff/internal/output"
	"github.com/KonMam/tdiff/internal/schema"
	"github.com/KonMam/tdiff/internal/source"
)

// version is the fallback for dev builds; releases override it via
// -ldflags "-X main.version=..." (a const would silently defeat that).
var version = "0.5.0-dev"

func usage() {
	fmt.Fprintf(os.Stderr, `tdiff %s: diff tabular data files (parquet, csv, tsv), in any combination

usage:
  tdiff <left> <right> --key <col>[,<col>...] [flags]   row + schema diff
  tdiff <left> <right> --keyless [flags]                diff whole rows
  tdiff schema <left> <right> [flags]                   schema diff only
  tdiff snapshot <file> --key <col> --output <b.snap>   save a hash baseline
  tdiff <file> --against <b.snap>                       diff vs the baseline

flags:
  --key <cols>             key column(s), comma-separated; omitted = inferred
                           (a column unique in both inputs, id-ish names first)
  --keyless                no key: match whole rows as a multiset, so
                           leftovers on either side are added/removed and
                           nothing is ever "changed"
  --where <predicate>      keep only the rows matching this predicate, on
                           both sides (repeatable, ANDed): price > 10 |
                           region = 'eu' | ts >= 2026-01-01 | note IS NULL.
                           Filtering happens before the diff, so the counts
                           are of the filtered rows
  --ignore-columns <cols>  columns to exclude from comparison
  --rename <right>=<left>  compare a right-side column under a left-side name
                           (repeatable), instead of reporting the pair as one
                           added and one removed column
  --format <fmt>           output format: human, json, or markdown (default human)
  --limit <n>              max example rows shown per category (default 10)
  --verbose                print example rows
  --summary                counts + exit code only (fastest mode)
  --mode auto|memory|stream  join strategy; stream spills hashes to disk and
                           keeps peak memory flat for larger-than-RAM inputs
  --tmpdir <dir>           spill directory for stream mode
  --infer-rows <n>         CSV type-inference sample size (default 1000; -1 = whole file)
  --delimiter <char>       delimited-text field separator (default: , for .csv, tab for .tsv)
  --on-dup <mode>          duplicate keys: error (default, fail), warn (keep
                           the first occurrence per side), or match (pair a
                           key's rows as multisets: identical rows cancel,
                           leftovers are added/removed, no change attribution
                           inside a duplicate group; needs --mode memory)
  --output <file>          write differing rows as data (.csv or .parquet):
                           key cols, diff_status, <col>__left/<col>__right
  --max-diff <n | p%%>      CI gate: exit 0 while total differing rows stay
                           within budget (schema changes still exit 1). The
                           run stops as soon as the budget is provably blown
  --report <file>          also write a report file (repeatable): .html for a
                           self-contained page, anything else markdown (for
                           CI step summaries)
  --float-precision <n>    round float comparisons to n decimal digits
                           (quantization: exact and hash-consistent, unlike
                           an epsilon)
  --ignore-case            compare strings case-insensitively
  --trim                   ignore leading/trailing whitespace in strings
  --timestamp-precision <p>  compare timestamps at s, ms or us precision
  --mask <cols>            replace these columns' values with a stable short
                           token in examples, reports and --output (the
                           comparison still uses the real values)
  --tolerance <spec>       treat numeric differences this small as equal
                           (repeatable): 0.01 | 0.01,rel=1e-6 | rel=1e-6 |
                           price=0.01. Reported as "within tolerance", not
                           as unchanged; needs full diff mode
  --version                print version

exit codes: 0 inputs equal, 1 differences found, 2 error
`, version)
}

func main() {
	// Page-decode buffers cycle through pools quickly; the default GC target
	// (100) collects so often that the pools drain and spans bounce between
	// the heap and the OS. A higher target costs no measurable RSS here
	// because the live set (join table + in-flight pages) is what it is.
	// GOGC set explicitly in the environment still wins. (150 balances the
	// kernel path's buffer reuse against peak-RSS growth.)
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(150)
	}
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("tdiff", flag.ContinueOnError)
	fs.Usage = usage
	key := fs.String("key", "", "key column(s), comma-separated")
	keyless := fs.Bool("keyless", false, "diff without a key: match whole rows as a multiset")
	var where whereFlags
	fs.Var(&where, "where", "keep only rows matching this predicate, repeatable and ANDed: \"price > 10\"")
	ignore := fs.String("ignore-columns", "", "columns to exclude, comma-separated")
	var rename renameFlags
	fs.Var(&rename, "rename", "compare a right-side column under a left-side name, repeatable: right_name=left_name")
	format := fs.String("format", "human", "output format: human, json, or markdown")
	limit := fs.Int("limit", 10, "max examples per category")
	verbose := fs.Bool("verbose", false, "print example rows")
	summary := fs.Bool("summary", false, "counts and exit code only (fastest; skips column attribution and examples)")
	mode := fs.String("mode", "auto", "join strategy: auto, memory, or stream (constant-memory grace hash join)")
	tmpdir := fs.String("tmpdir", "", "spill directory for --mode stream (default: system temp)")
	inferRows := fs.Int("infer-rows", 0, "CSV type-inference sample rows (default 1000; -1 = whole file)")
	onDup := fs.String("on-dup", "error", "duplicate keys: error, warn (keep first occurrence per side), or match (multiset pairing)")
	outFile := fs.String("output", "", "write the differing rows as data to this .csv or .parquet file")
	against := fs.String("against", "", "diff a single file against a snapshot baseline (.snap)")
	maxDiff := fs.String("max-diff", "", "CI gate: exit 0 while added+removed+changed stays within this budget (a count like 1000, or a percentage like 0.5%)")
	var report reportFlags
	fs.Var(&report, "report", "also write a report to this file, repeatable: .html for a self-contained page, else markdown")
	floatPrec := fs.Int("float-precision", 0, "round float comparisons to N decimal digits (0 = exact)")
	ignoreCase := fs.Bool("ignore-case", false, "compare strings case-insensitively")
	trim := fs.Bool("trim", false, "ignore leading and trailing whitespace in string comparisons")
	tsPrec := fs.String("timestamp-precision", "", "compare timestamps at this precision: s, ms or us (default us, exact)")
	delimiter := fs.String("delimiter", "", "delimited-text field separator (default: , for .csv, tab for .tsv)")
	mask := fs.String("mask", "", "columns whose values are shown as a stable token instead, comma-separated")
	var tol tolFlags
	fs.Var(&tol, "tolerance", "numeric epsilon, repeatable: 0.01 | 0.01,rel=1e-6 | rel=1e-6 | price=0.01")
	showVersion := fs.Bool("version", false, "print version")
	cpuProfile := fs.String("cpuprofile", "", "write CPU profile to file (dev)")
	memProfile := fs.String("memprofile", "", "write heap profile to file (dev)")

	// Accept flags before or after positional args.
	var pos []string
	rest := args
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			if err == flag.ErrHelp {
				return 0 // explicitly requested help is not an error
			}
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
		fmt.Println("tdiff", version)
		return 0
	}
	switch *onDup {
	case "error", "warn", "match":
	default:
		return fail(fmt.Errorf("--on-dup must be error, warn or match, got %q", *onDup))
	}
	if *maxDiff != "" {
		// fail on a malformed budget before opening anything
		if _, err := diff.ParseBudget(*maxDiff, 100); err != nil {
			return fail(err)
		}
	}
	// the comparison semantics shared by every command
	cmp := diff.Options{
		OnDup: *onDup, FloatPrecision: *floatPrec,
		IgnoreCase: *ignoreCase, Trim: *trim, TimestampPrecision: *tsPrec,
		Tolerance: tol.global, ColumnTolerance: tol.byCol,
		Rename: rename.byName, Mask: splitList(*mask), Keyless: *keyless,
		Where: source.PredicateString(where.preds),
	}
	srcOpts := source.Options{InferRows: *inferRows, Delimiter: *delimiter}

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
			fmt.Fprintln(os.Stderr, "usage: tdiff snapshot <file> --key <col> --output <base.snap>")
			return 2
		}
		if cmp.Tolerance != nil || len(cmp.ColumnTolerance) > 0 {
			return fail(fmt.Errorf("--tolerance cannot be baked into a snapshot (a baseline stores hashes, not values); use --float-precision, which is hash-consistent"))
		}
		if len(cmp.Rename) > 0 {
			return fail(fmt.Errorf("--rename compares two inputs; a snapshot has one side (rename when diffing against the baseline instead)"))
		}
		if cmp.Keyless {
			return fail(fmt.Errorf("--keyless snapshots are not supported: a baseline is keyed by design"))
		}
		return runSnapshot(pos[0], *outFile, *key, srcOpts, cmp, where.preds)
	}
	if *against != "" {
		if len(pos) != 1 {
			fmt.Fprintln(os.Stderr, "usage: tdiff <file> --against <base.snap>")
			return 2
		}
		if cmp.Keyless {
			return fail(fmt.Errorf("--keyless cannot diff against a snapshot: a baseline is keyed by design"))
		}
		cmp.Limit = *limit
		return runAgainst(pos[0], *against, srcOpts, cmp, *format, where.preds)
	}
	if len(pos) != 2 {
		usage()
		return 2
	}

	var left, right source.Source
	var pair source.PairInfo
	var err error
	if schemaOnly {
		// schema comparison must see every file, not the pruned sets
		left, err = source.OpenWith(pos[0], srcOpts)
		if err != nil {
			return fail(err)
		}
		right, err = source.OpenWith(pos[1], srcOpts)
		if err != nil {
			left.Close()
			return fail(err)
		}
	} else if len(where.preds) > 0 {
		// the shared-file shortcut folds a file's row count in as unchanged
		// without scanning it, which a filter would have reduced, so with
		// --where both sides are opened in full
		left, err = source.OpenWith(pos[0], srcOpts)
		if err != nil {
			return fail(err)
		}
		right, err = source.OpenWith(pos[1], srcOpts)
		if err != nil {
			left.Close()
			return fail(err)
		}
	} else {
		left, right, pair, err = source.OpenPair(pos[0], pos[1], srcOpts)
		if err != nil {
			return fail(err)
		}
		if pair.SharedFiles > 0 {
			plural := "s"
			if pair.SharedFiles == 1 {
				plural = ""
			}
			fmt.Fprintf(os.Stderr, "tdiff: %s: skipping %d data file%s shared by both snapshots (%s rows per side)\n",
				pair.Table, pair.SharedFiles, plural, humanCount(pair.SharedRows))
		}
	}
	defer left.Close()
	defer right.Close()
	printWarnings(pos[0], left)
	printWarnings(pos[1], right)

	var filesPruned int
	if len(where.preds) > 0 && !schemaOnly {
		var pruned int
		if left, pruned, err = applyFilter(left, where.preds); err != nil {
			return fail(err)
		}
		filesPruned = pruned
		if right, pruned, err = applyFilter(right, where.preds); err != nil {
			return fail(err)
		}
		filesPruned += pruned
		if filesPruned > 0 {
			fmt.Fprintf(os.Stderr, "tdiff: --where: %d data file(s) skipped by partition value\n", filesPruned)
		}
	}

	if schemaOnly {
		rs, renames, rerr := schema.ApplyRenames(left.Schema(), right.Schema(), cmp.Rename)
		if rerr != nil {
			return fail(rerr)
		}
		sd := schema.Compare(left.Schema(), rs)
		sd.Renames = renames
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

	opts := cmp
	opts.Limit, opts.Summary, opts.Mode, opts.TempDir = *limit, *summary, *mode, *tmpdir
	// the engine stops early once the budget is provably blown; it declines
	// to when an export is in flight (see abortThreshold)
	opts.MaxDiff = *maxDiff
	if *keyless && *key != "" {
		return fail(fmt.Errorf("--keyless and --key are mutually exclusive"))
	}
	if !*keyless {
		keys, kerr := resolveKeys(*key, left, right, cmp.Rename)
		if kerr != nil {
			return fail(kerr)
		}
		opts.Keys = keys
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
	// Each retype-and-restart demotes one more column to string, so the
	// retry count is naturally bounded by the column count; 64 is a safety
	// backstop against a retype that fails to stick.
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
			_ = closeSink() // the diff error is what matters here
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
		fmt.Fprintf(os.Stderr, "tdiff: warning: %v; re-reading column %q as string\n", coerce, coerce.Column)
	}
	// rows in files shared by both snapshots were skipped, not scanned;
	// fold them back into the totals
	res.LeftRows += pair.SharedRows
	res.RightRows += pair.SharedRows
	res.Unchanged += pair.SharedRows
	res.FinishStats() // the folded-in rows widen every match-rate denominator
	res.FilesPruned = filesPruned
	switch *format {
	case "json":
		if err := output.JSON(os.Stdout, res); err != nil {
			return fail(err)
		}
	case "markdown":
		output.Markdown(os.Stdout, res, pos[0], pos[1])
	default:
		output.Human(os.Stdout, res, *verbose)
	}
	for _, path := range report.paths {
		if ferr := writeReport(path, res, pos[0], pos[1]); ferr != nil {
			return fail(ferr)
		}
	}
	if *memProfile != "" {
		f, ferr := os.Create(*memProfile)
		if ferr == nil {
			_ = pprof.Lookup("heap").WriteTo(f, 0) // dev-only profile
			f.Close()
		}
	}
	if res.Same() {
		return 0
	}
	if res.Aborted {
		// the engine already established the budget was blown; the report on
		// stdout carries the detail
		fmt.Fprintln(os.Stderr, "tdiff: stopped early (--max-diff budget exceeded)")
		return 1
	}
	if *maxDiff != "" && res.Schema.Same() {
		budget, berr := diff.ParseBudget(*maxDiff, max(res.LeftRows, res.RightRows))
		if berr != nil {
			return fail(berr)
		}
		total := res.Added + res.Removed + res.Changed
		if total <= budget {
			fmt.Fprintf(os.Stderr, "tdiff: %d differing rows within --max-diff budget of %d\n", total, budget)
			return 0
		}
	}
	return 1
}

// resolveKeys returns the key columns: --key when given, otherwise the
// inferred key, announced on stderr.
func resolveKeys(key string, left, right source.Source, renames map[string]string) ([]string, error) {
	if key != "" {
		return splitList(key), nil
	}
	k, err := diff.InferKey(left, right, renames)
	if err != nil {
		return nil, fmt.Errorf("no --key given and none could be inferred: %w", err)
	}
	fmt.Fprintf(os.Stderr, "tdiff: using inferred key column %q (pass --key to override)\n", k)
	return []string{k}, nil
}

// runSnapshot creates a hash-manifest baseline of one file.
func runSnapshot(path, out, key string, srcOpts source.Options, opts diff.Options, preds []source.Predicate) int {
	src, err := source.OpenWith(path, srcOpts)
	if err != nil {
		return fail(err)
	}
	defer src.Close()
	if len(preds) > 0 {
		filtered, _, ferr := applyFilter(src, preds)
		if ferr != nil {
			return fail(ferr)
		}
		src = filtered
	}
	keys, kerr := resolveKeys(key, src, src, nil)
	if kerr != nil {
		return fail(kerr)
	}
	opts.Keys = keys
	rows, err := diff.WriteSnapshot(src, out, opts)
	if err != nil {
		return fail(err)
	}
	size := "unknown size"
	if st, serr := os.Stat(out); serr == nil {
		size = humanBytes(st.Size())
	}
	fmt.Printf("snapshot: %s rows -> %s (%s)\n", humanCount(rows), out, size)
	return 0
}

// runAgainst diffs a live file against a snapshot baseline.
func runAgainst(path, snap string, srcOpts source.Options, opts diff.Options, format string, preds []source.Predicate) int {
	src, err := source.OpenWith(path, srcOpts)
	if err != nil {
		return fail(err)
	}
	defer src.Close()
	if len(preds) > 0 {
		filtered, _, ferr := applyFilter(src, preds)
		if ferr != nil {
			return fail(ferr)
		}
		src = filtered
	}
	res, err := diff.DiffAgainstSnapshot(snap, src, opts)
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

// reportFlags collects the repeatable --report flag, so one run can write
// both a markdown summary for the CI step and an HTML page for the artifact.
type reportFlags struct{ paths []string }

func (r *reportFlags) String() string { return strings.Join(r.paths, " ") }

func (r *reportFlags) Set(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("--report needs a file path")
	}
	r.paths = append(r.paths, path)
	return nil
}

// writeReport writes one report, choosing the renderer by extension: .html
// is a self-contained page (CI artifact, email attachment), anything else is
// markdown.
func writeReport(path string, res *diff.Result, left, right string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".html", ".htm":
		output.HTML(f, res, left, right)
	default:
		output.Markdown(f, res, left, right)
	}
	return f.Close()
}

// whereFlags collects the repeatable --where flag; the clauses are ANDed.
type whereFlags struct {
	preds []source.Predicate
	specs []string
}

func (w *whereFlags) String() string { return strings.Join(w.specs, " ") }

func (w *whereFlags) Set(spec string) error {
	preds, err := source.ParseWhere(spec)
	if err != nil {
		return err
	}
	w.preds = append(w.preds, preds...)
	w.specs = append(w.specs, spec)
	return nil
}

// applyFilter wraps one side in the row filter and reports how many data
// files partition pruning let it skip.
func applyFilter(src source.Source, preds []source.Predicate) (source.Source, int, error) {
	out, err := source.Filter(src, preds)
	if err != nil {
		return src, 0, err
	}
	pruned := 0
	if p, ok := out.(interface{ PrunedFiles() int }); ok {
		pruned = p.PrunedFiles()
	}
	return out, pruned, nil
}

// renameFlags collects the repeatable --rename flag, keyed by the right-side
// name (the side being renamed).
type renameFlags struct {
	byName map[string]string
	specs  []string
}

func (r *renameFlags) String() string { return strings.Join(r.specs, " ") }

func (r *renameFlags) Set(spec string) error {
	right, left, ok := strings.Cut(spec, "=")
	right, left = strings.TrimSpace(right), strings.TrimSpace(left)
	if !ok || right == "" || left == "" {
		return fmt.Errorf("bad --rename %q (want right_name=left_name)", spec)
	}
	if prev, dup := r.byName[right]; dup && prev != left {
		return fmt.Errorf("--rename gives %q two targets (%q and %q)", right, prev, left)
	}
	if r.byName == nil {
		r.byName = map[string]string{}
	}
	r.byName[right] = left
	r.specs = append(r.specs, spec)
	return nil
}

// tolFlags collects the repeatable --tolerance flag: bare specs set the
// default for every numeric column, "col=..." specs override one column.
type tolFlags struct {
	global *diff.Tolerance
	byCol  map[string]diff.Tolerance
	specs  []string
}

func (t *tolFlags) String() string { return strings.Join(t.specs, " ") }

func (t *tolFlags) Set(spec string) error {
	col, parsed, err := diff.ParseTolerance(spec)
	if err != nil {
		return err
	}
	t.specs = append(t.specs, spec)
	if col == "" {
		t.global = &parsed
		return nil
	}
	if t.byCol == nil {
		t.byCol = map[string]diff.Tolerance{}
	}
	t.byCol[col] = parsed
	return nil
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
	fmt.Fprintln(os.Stderr, "tdiff:", err)
	return 2
}

// printWarnings surfaces reader warnings (skipped columns etc.) on stderr.
func printWarnings(path string, s source.Source) {
	if w, ok := s.(interface{ Warnings() []string }); ok {
		for _, msg := range w.Warnings() {
			fmt.Fprintf(os.Stderr, "tdiff: warning: %s: %s\n", path, msg)
		}
	}
}
