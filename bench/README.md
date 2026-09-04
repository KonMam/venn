# Performance and torture suites

Two arms, one approach: measure a fixed deterministic workload, gate on
correctness before timing, and make every failure reproducible from a name or
a seed.

`bench/run_bench.py` is a separate thing, the competitor benchmark behind the
numbers in the README. The suites here track tdiff against itself.

## Perf regression (`bench/perf`)

A curated case matrix covering every command, every format, three data shapes
and three diff densities, generated deterministically by `internal/fixture`
into `bench/perf/cache/` and reused. Ground truth lands in `manifest.json`.

The generator can also plant rows that differ only within
`fixture.ToleranceAbs` (`--tolerable`), recorded in the manifest as
`tolerable` and `tolerable_keys`. Those are the oracle for `--tolerance`: a
run with that tolerance has to move exactly those rows out of `changed` and
into `within_tolerance`, which `TestToleranceOracle` checks.

```bash
go run ./bench/perf -list                    # show the matrix
go run ./bench/perf -tier tiny               # 10k rows; harness self-test
go run ./bench/perf -tier smoke              # 1M rows; the CI tier
go run ./bench/perf -tier standard           # 10M rows; pre-release gate
go run ./bench/perf -tier large              # 100M rows; opt-in, ~20GB fixtures
go run ./bench/perf -filter 'csv|stream'     # subset by regexp
```

Mechanics, in order:

1. **Correctness gate.** Each case runs once with output captured, and its
   added/removed/changed counts have to match the manifest exactly. Exports
   additionally have to contain exactly the differing rows. A tool that
   answers wrong is never timed.
2. **Timing.** N runs per tier, keeping min wall, min CPU (user+sys, summed
   across cores, so it exceeds wall on parallel workloads) and max peak RSS.
   CPU time is the regression metric: it is largely immune to the scheduler
   and disk noise that makes wall time useless on shared runners.
3. **Results** land as JSON in `bench/perf/results/` (gitignored), stamped
   with commit, tier and host. On pushes to main, CI uploads them as
   artifacts.

### A/B against a git ref

```bash
go run ./bench/perf -tier smoke -against main -check
go run ./bench/perf -baseline bench/perf/results/<file>.json   # same machine only
```

`-against` builds the ref in a temporary git worktree and interleaves runs
A,B,A,B on the same machine at the same moment, so both binaries see the same
thermal, cache and load conditions, and machine variance cancels out of the
ratio.

`-check` exits 1 when a case regresses CPU by more than 25%
(`-max-case-cpu`), the geomean regresses by more than 10%
(`-max-geomean-cpu`), or RSS regresses past the analogous limits. Cases under
a 20 ms CPU floor are reported but never gated, since below that process
startup noise dominates.

Snapshot cases build each binary's `.snap` with that same binary: the
snapshot format is not part of the A/B contract.

The `perf-smoke` CI job runs this on every PR against the base branch and
writes the comparison table to the job's step summary.

### Adding a case

Add one entry to `allCases` in `cases.go`. Zero values default to
workload=full, parquet against parquet, standard shape, 15 columns, d1
density. Cap huge shapes with `MaxRows`, and opt fast paths into the 100M
tier with `LargeOK`.

## Torture (`internal/torture`)

Build a well-formed file, break it outside tdiff, and check that tdiff
reports the damage instead of misbehaving.

The matrix is every format (parquet, csv, ndjson, csv.gz, csv.zst, `.snap`)
against mutators (bit flips, truncation, zeroed and deleted regions, appended
garbage, and for text: injected newlines and CRLF, lone quotes, NUL bytes,
invalid UTF-8) across seeds, plus a 17-step truncation ladder per format,
degenerate hand-built files (empty, header-only, ragged rows, unterminated
quotes, BOM, fake parquet magic), and valid-but-nasty inputs where survival
is not enough and exact counts are required (quoted embedded newlines, 9 KB
fields, rows straddling the 1 MB parallel-split blocks).

The contract asserted for every mutated input:

- terminates within 30 s, with an exit code of 0, 1 or 2 and never a crash;
- no panic or stack trace in either output stream;
- exit 2 implies an error message on stderr;
- exit 0 or 1 implies stdout is valid JSON with non-negative counts, so a
  claimed success cannot be silent garbage;
- peak RSS stays bounded, under 1 GB on a ~300 KB input, which catches a
  corrupt length field turning into a huge allocation.

It runs inside plain `go test ./...` (about 240 subtests, ~3 s), so every CI
platform runs it on every push, and the `torture-soak` job runs the same
matrix at `TDIFF_TORTURE_ROUNDS=25`. Every subtest is named
`mutator/seedN`, so a failure reproduces from the test name alone.

The parse-level fuzz targets in `internal/source` (`go test -fuzz`) are the
deeper per-decoder search; torture is the whole-binary contract check.

## Fixtures

Go fixtures are generated on demand by the suites above. The lake-table
fixtures are checked in, because they are written by the authoritative
writers rather than by tdiff, and regenerating them needs a Python
environment with `pyiceberg`, `deltalake` and `pyarrow`:

```bash
python testdata/tables/gen_tables.py testdata/tables   # Iceberg and Delta tables
python testdata/tables/gen_mor.py testdata/tables      # merge-on-read delete files
python testdata/interop/gen_corpus.py                  # foreign-written parquet
```

`run_bench.py` writes its raw timings into `bench/results/` (gitignored) and
`render_benchmarks.py` turns them into a `BENCHMARKS.md`. `versions.lock`
records the tool and library versions the published numbers were measured
with.
