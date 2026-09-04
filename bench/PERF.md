# The perf & torture suite

Two arms, one philosophy borrowed from SQLite and TigerBeetle: **measure a
fixed deterministic workload, gate on correctness before timing, and make
every failure reproducible from a name or a seed.**

(`bench/run_bench.py` is a different thing: the *competitor* benchmark that
produces the public BENCHMARKS.md chart. This suite tracks venn against
itself.)

## Arm 1: perf regression (`bench/perf`)

A curated case matrix — every command, every format, three data shapes, three
diff densities — generated deterministically by `internal/fixture` (seeded;
ground truth in `manifest.json`) into `bench/perf/cache/` and reused.

The generator can also plant rows that differ *only* within
`fixture.ToleranceAbs` (`--tolerable`), recorded in the manifest as
`tolerable`/`tolerable_keys`. Those are the oracle for `--tolerance`: a diff
run with that tolerance must move exactly those rows out of `changed` and
into `within_tolerance` (see `TestToleranceOracle`).

```bash
go run ./bench/perf -list                    # show the matrix
go run ./bench/perf -tier tiny               # 10k rows, seconds — harness self-test
go run ./bench/perf -tier smoke              # 1M rows — the CI tier
go run ./bench/perf -tier standard           # 10M rows — pre-release gate
go run ./bench/perf -tier large              # 100M rows — opt-in, ~20GB fixtures
go run ./bench/perf -filter 'csv|stream'     # subset by regexp
```

Mechanics, in order:

1. **Correctness gate.** Each case runs once with output captured; its
   added/removed/changed counts must match the fixture manifest exactly
   (exports additionally must contain exactly the differing rows). A tool
   that answers wrong is never timed — a fast wrong answer is worthless.
2. **Timing.** N runs per tier; we keep min wall, min CPU (user+sys, summed
   across cores — so it exceeds wall on parallel workloads), max peak RSS.
   **CPU time is the regression metric** — it is largely immune to
   the scheduler/disk noise that makes wall time useless on shared runners
   (our portable stand-in for SQLite's cachegrind cycle counts).
3. **Results** land as JSON in `bench/perf/results/` (gitignored), stamped
   with commit/tier/host — TigerBeetle-devhub-style history. On pushes to
   main, CI uploads them as artifacts.

### Regression detection: A/B against a git ref

```bash
go run ./bench/perf -tier smoke -against main -check
go run ./bench/perf -baseline bench/perf/results/<file>.json   # same machine only
```

`-against` builds the ref in a temporary git worktree and **interleaves runs
A,B,A,B on the same machine at the same moment**, so both binaries see the
same thermal/cache/load conditions and machine variance cancels out of the
ratio. `-check` fails (exit 1) when a case regresses CPU by >25% (`-max-case-cpu`),
the geomean regresses by >10% (`-max-geomean-cpu`), or RSS regresses past the
analogous limits. Cases under a 20ms CPU floor are reported but never gated —
below that, process startup noise dominates.

Snapshot cases build each binary's `.snap` with that same binary: the snapshot
format is not part of the A/B contract.

The `perf-smoke` CI job runs this on every PR against the base branch and
writes the comparison table to the job's step summary.

### Adding a case

Add one entry to `allCases` in `bench/perf/cases.go`. Zero values default to
workload=full, parquet/parquet, standard shape, 15 cols, d1 density. Cap huge
shapes with `MaxRows`; opt fast paths into the 100M tier with `LargeOK`.

## Arm 2: torture (`internal/torture`)

SQLite's malformed-database discipline, applied end-to-end through the real
binary: build a well-formed file, break it *outside* venn, and verify venn
"finds the errors and reports them without performing unwholesome actions."

The matrix: every format (parquet, csv, ndjson, csv.gz, csv.zst, `.snap`) ×
mutators (bit flips, truncation, zeroed/deleted regions, appended garbage,
and for text: injected newlines/CRLF, lone quotes, NUL bytes, invalid UTF-8)
× seeds, plus a 17-step truncation ladder per format, degenerate hand-built
files (empty, header-only, ragged rows, unterminated quotes, BOM, fake
parquet magic…), and *valid-but-nasty* inputs where survival is not enough
and exact counts are required (quoted embedded newlines, 9KB fields, rows
straddling the 1MB parallel-split blocks).

The contract asserted for every mutated input:

- terminates within 30s (no hangs), exit code 0/1/2 only — never a crash;
- no panic / stack trace in either output stream;
- exit 2 ⇒ an error message on stderr;
- exit 0/1 ⇒ stdout is valid JSON with non-negative counts (no silent
  garbage "successes");
- peak RSS stays bounded (<1GB on a ~300KB input — catches corrupt length
  fields turning into huge allocations).

It runs inside plain `go test ./...` (≈240 subtests, ~3s), so every CI OS
runs it on every push; the `torture-soak` CI job runs the same matrix at
`VENN_TORTURE_ROUNDS=25`. Every subtest is named `mutator/seedN` — a failure
reproduces from the test name alone.

The parse-level fuzz targets in `internal/source` (`go test -fuzz`) remain
the deeper per-decoder search; torture is the whole-binary contract check.
