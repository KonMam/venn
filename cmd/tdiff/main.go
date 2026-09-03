// tdiff — a fast, single-binary tabular data differ.
//
//	tdiff a.parquet b.csv --key id     keyed row diff across formats
//	tdiff schema a.parquet b.csv       schema diff only
//
// Exit codes: 0 inputs equal, 1 differences found, 2 error.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"strings"

	"tdiff/internal/diff"
	"tdiff/internal/output"
	"tdiff/internal/schema"
	"tdiff/internal/source"
)

const version = "0.1.0-dev"

func usage() {
	fmt.Fprintf(os.Stderr, `tdiff %s — diff tabular data files (parquet, csv, tsv), in any combination

usage:
  tdiff <left> <right> --key <col>[,<col>...] [flags]   row + schema diff
  tdiff schema <left> <right> [flags]                   schema diff only

flags:
  --key <cols>             key column(s), comma-separated (required for row diff)
  --ignore-columns <cols>  columns to exclude from comparison
  --format human|json      output format (default human)
  --limit <n>              max example rows shown per category (default 10)
  --verbose                print example rows
  --summary                counts + exit code only (fastest mode) in human output
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
	fs := flag.NewFlagSet("tdiff", flag.ContinueOnError)
	fs.Usage = usage
	key := fs.String("key", "", "key column(s), comma-separated")
	ignore := fs.String("ignore-columns", "", "columns to exclude, comma-separated")
	format := fs.String("format", "human", "output format: human or json")
	limit := fs.Int("limit", 10, "max examples per category")
	verbose := fs.Bool("verbose", false, "print example rows")
	summary := fs.Bool("summary", false, "counts and exit code only (fastest; skips column attribution and examples)")
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
		fmt.Println("tdiff", version)
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
	if len(pos) > 0 && pos[0] == "schema" {
		schemaOnly = true
		pos = pos[1:]
	}
	if len(pos) != 2 {
		usage()
		return 2
	}

	left, err := source.Open(pos[0])
	if err != nil {
		return fail(err)
	}
	defer left.Close()
	right, err := source.Open(pos[1])
	if err != nil {
		return fail(err)
	}
	defer right.Close()

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

	opts := diff.Options{Limit: *limit, Summary: *summary}
	if *key != "" {
		opts.Keys = splitList(*key)
	}
	if *ignore != "" {
		opts.IgnoreColumns = splitList(*ignore)
	}
	res, err := diff.Run(left, right, opts)
	if err != nil {
		return fail(err)
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
	return 1
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
