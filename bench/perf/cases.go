package main

import (
	"fmt"
	"path/filepath"
)

// The perf suite is a curated case list, not a full cartesian product: every
// command and every format appears at least once, the core density sweep runs
// on the default shape, and each case pins the dimensions that matter to it.
// Tiers scale row counts so the same matrix serves CI smoke runs and local
// deep runs.

type density struct{ changed, added, removed float64 }

var densities = map[string]density{
	"d0":  {0, 0, 0},
	"d1":  {0.01, 0.005, 0.005},
	"d10": {0.10, 0.05, 0.05},
}

type tierSpec struct {
	rows int64
	runs int // timed runs per binary per case (gate run is extra)
}

var tiers = map[string]tierSpec{
	"tiny":     {10_000, 1},      // harness self-test; seconds
	"smoke":    {1_000_000, 5},   // CI gate; a few minutes
	"standard": {10_000_000, 3},  // local pre-release gate
	"large":    {100_000_000, 2}, // opt-in; needs ~20GB of fixtures
}

// Case is one measured workload. Zero values mean: shape=standard, cols=15,
// density=d1, left=right=parquet.
type Case struct {
	Name     string
	Workload string // full | summary | stream | export-csv | export-parquet | snapshot-create | snapshot-diff | inferkey | schema
	Left     string
	Right    string
	Shape    string
	Cols     int
	Density  string
	MaxRows  int64 // cap dataset size regardless of tier (0 = no cap)
	LargeOK  bool  // include in the large tier
}

func (c Case) normalized() Case {
	if c.Workload == "" {
		c.Workload = "full"
	}
	if c.Left == "" {
		c.Left = "parquet"
	}
	if c.Right == "" {
		c.Right = c.Left
	}
	if c.Shape == "" {
		c.Shape = "standard"
	}
	if c.Cols == 0 {
		c.Cols = 15
	}
	if c.Density == "" {
		c.Density = "d1"
	}
	return c
}

var allCases = []Case{
	// density sweep, parquet
	{Name: "full-parquet-d0", Density: "d0", LargeOK: true},
	{Name: "full-parquet-d1", LargeOK: true},
	{Name: "full-parquet-d10", Density: "d10"},
	// format coverage
	{Name: "full-csv-d1", Left: "csv", LargeOK: true},
	{Name: "full-ndjson-d1", Left: "ndjson"},
	{Name: "full-csvgz-d1", Left: "csv.gz"},
	{Name: "full-csvzst-d1", Left: "csv.zst"},
	{Name: "full-cross-d1", Left: "parquet", Right: "csv"},
	// shapes
	{Name: "full-wide-d1", Shape: "wide", Cols: 100, MaxRows: 1_000_000},
	{Name: "full-stringy-d1", Shape: "stringy", MaxRows: 10_000_000},
	// commands / modes
	{Name: "summary-parquet-d1", Workload: "summary", LargeOK: true},
	{Name: "stream-parquet-d1", Workload: "stream"},
	{Name: "export-csv-d1", Workload: "export-csv"},
	{Name: "export-parquet-d1", Workload: "export-parquet"},
	{Name: "snapshot-create", Workload: "snapshot-create"},
	{Name: "snapshot-diff-d1", Workload: "snapshot-diff"},
	{Name: "inferkey-parquet-d1", Workload: "inferkey"},
	{Name: "schema-parquet", Workload: "schema"},
}

// datasetKey identifies one generated fixture pair in the cache.
type datasetKey struct {
	Shape   string
	Cols    int
	Density string
	Rows    int64
}

func (k datasetKey) dirName() string {
	return fmt.Sprintf("%s-c%d-%s-r%d-s1", k.Shape, k.Cols, k.Density, k.Rows)
}

func (c Case) dataset(tier tierSpec) datasetKey {
	rows := tier.rows
	if c.MaxRows > 0 && rows > c.MaxRows {
		rows = c.MaxRows
	}
	return datasetKey{Shape: c.Shape, Cols: c.Cols, Density: c.Density, Rows: rows}
}

// formats returns the fixture formats this case reads.
func (c Case) formats() []string {
	if c.Left == c.Right {
		return []string{c.Left}
	}
	return []string{c.Left, c.Right}
}

// args builds the venn command line. snapPath is the per-binary snapshot
// baseline (used by the snapshot workloads); outPath is the per-case export
// target.
func (c Case) args(dsDir, tmpDir, snapPath, outPath string) []string {
	left := filepath.Join(dsDir, "left."+c.Left)
	right := filepath.Join(dsDir, "right."+c.Right)
	switch c.Workload {
	case "schema":
		return []string{"schema", left, right}
	case "snapshot-create":
		return []string{"snapshot", left, "--key", "id", "--output", snapPath}
	case "snapshot-diff":
		return []string{right, "--against", snapPath, "--format", "json"}
	}
	a := []string{left, right, "--format", "json"}
	if c.Workload != "inferkey" {
		a = append(a, "--key", "id")
	}
	switch c.Workload {
	case "summary":
		a = append(a, "--summary")
	case "stream":
		a = append(a, "--mode", "stream", "--tmpdir", tmpDir)
	case "export-csv", "export-parquet":
		a = append(a, "--output", outPath)
	}
	return a
}

// exportExt returns the export file extension for export workloads, "" otherwise.
func (c Case) exportExt() string {
	switch c.Workload {
	case "export-csv":
		return "csv"
	case "export-parquet":
		return "parquet"
	}
	return ""
}

// needsSnapshot reports whether the case consumes a pre-built .snap baseline.
func (c Case) needsSnapshot() bool { return c.Workload == "snapshot-diff" }
