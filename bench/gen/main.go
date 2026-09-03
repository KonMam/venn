// gen is the CLI for the fixture generator (internal/fixture): it produces
// left/right file pairs with known planted diffs plus a ground-truth
// manifest.json, for tests and benchmarks.
//
//	go run ./bench/gen --rows 1000000 --out testdata/1m \
//	    --changed 0.01 --added 0.005 --removed 0.005 --seed 1
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"tdiff/internal/fixture"
)

func main() {
	rows := flag.Int64("rows", 1000, "base row count (left side)")
	ncols := flag.Int("cols", 15, "column count including key")
	seed := flag.Uint64("seed", 1, "generator seed")
	pctChanged := flag.Float64("changed", 0.01, "fraction of rows changed")
	pctAdded := flag.Float64("added", 0.005, "added rows as fraction of base")
	pctRemoved := flag.Float64("removed", 0.005, "fraction of rows removed")
	out := flag.String("out", "", "output directory (required)")
	formats := flag.String("formats", "parquet,csv", "formats to emit, comma-separated")
	variant := flag.String("variant", "standard", "standard | wide | stringy")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "gen: --out is required")
		os.Exit(2)
	}
	man, err := fixture.Generate(fixture.Config{
		Rows: *rows, Cols: *ncols, Seed: *seed,
		PctChanged: *pctChanged, PctAdded: *pctAdded, PctRemoved: *pctRemoved,
		Out: *out, Formats: strings.Split(*formats, ","), Variant: *variant,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(2)
	}
	fmt.Printf("gen: %s rows_left=%d rows_right=%d added=%d removed=%d changed=%d\n",
		*out, man.RowsLeft, man.RowsRight, man.Added, man.Removed, man.Changed)
}
