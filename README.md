# tdiff

Row-level keyed diff of tabular datasets: files, directories, and lake tables.
One binary, pure Go, no server and no SQL to write.

```console
$ tdiff prod_export.parquet migrated.csv --key id
schema: identical
rows:   +50,000 added   -50,137 removed   ~100,008 changed   =9,849,855 unchanged   (left 10,000,000, right 9,999,863)
changed columns: price(41,203) qty(38,900) updated_at(31,077)
$ echo $?
1
```

Point it at two Iceberg snapshots and it tells you which rows changed, not
just which files:

```console
$ tdiff s3://lake/orders#8412 s3://lake/orders#8500 --key order_id
tdiff: s3://lake/orders: skipping 412 data files shared by both snapshots (96,401,220 rows per side)
schema: identical
rows:   +1,204,551 added   -0 removed   ~88,012 changed   =96,530,190 unchanged   (left 96,618,202, right 97,822,753)
changed columns: status(88,012)
```

## Install

```bash
go install github.com/KonMam/tdiff/cmd/tdiff@latest
```

## What it's for

- **Migration validation.** The old pipeline's output against the new one's,
  before you cut over. Any format diffs against any other, and values compare
  by logical type, so a parquet INT64 `10` equals a CSV `10.0`.
- **Pipeline regression in CI.** Yesterday's output against today's, with an
  allowed-change budget, an exit code, and a markdown report in the job
  summary. There is a [GitHub Action](#github-action).
- **Lake table snapshot deltas.** Which rows changed between two Iceberg
  snapshots or Delta versions. No engine, no cluster, no warehouse.

## What it reads

| Input | Notes |
|---|---|
| `.parquet` | own decode kernels: dictionary, v1/v2 pages, snappy/gzip/zstd, INT96, int-backed DECIMAL, delta encodings |
| `.csv` `.tsv` `.ndjson` `.jsonl` | plus `.gz` and `.zst`; types inferred from a sample, with automatic recovery when later values contradict it |
| directories and globs | one dataset: union-by-name schemas, hive partition columns (`region=eu/…`) become real columns |
| Iceberg tables | table root, `#<snapshot-id>` picks a snapshot; v1/v2, merge-on-read (position *and* equality deletes) |
| Delta tables | `#<version>` picks a version; checkpoint plus log replay, deletion vectors |
| `s3://` `http(s)://` | tables and files alike; parquet reads by byte range instead of downloading |

Diffing two snapshots of the same table skips data files that are live in
both. A copy-on-write table holds each row in exactly one file, so shared
files contribute identical rows to both sides and cancel exactly; a 1% churn
on a huge table scans about 2% of it.

## Usage

```
tdiff <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
tdiff schema <left> <right>                            schema diff only
tdiff snapshot <file> --key <col> --output <b.snap>    save a hash baseline
tdiff <file> --against <b.snap>                        diff against a baseline
```

Exit codes: `0` identical or within budget, `1` differences, `2` error.

See [docs/usage.md](docs/usage.md) for the full flag reference and
[docs/semantics.md](docs/semantics.md) for the comparison rules.

## CI

```bash
tdiff old/ new/ --key id --summary          # fastest: counts only
tdiff old/ new/ --key id --max-diff 0.1%    # allow small drift
tdiff old/ new/ --key id --format json      # machine-readable
tdiff old/ new/ --key id --report out.md    # markdown for humans
```

### GitHub Action

```yaml
- uses: KonMam/tdiff@main
  with:
    left: expected/orders.parquet
    right: build/orders.parquet
    key: order_id
    max-diff: "0.1%"
```

The step fails when differences exceed the budget, appends a markdown report
to the job summary, and exposes `added`, `removed`, `changed` and `unchanged`
as outputs.

### Snapshot baselines

When the left side should not be re-read every run, freeze it once. A
snapshot costs 16 bytes per row whatever the table's width:

```bash
tdiff snapshot expected.parquet --key id --output baseline.snap
tdiff build/output.parquet --against baseline.snap
```

## Why not DuckDB?

You can write this diff as a `FULL OUTER JOIN … IS DISTINCT FROM` query, and
DuckDB runs it well. Measured on one laptop (M1 Pro, 16 GB), same data,
single runs, DuckDB 1.4.3, both tools computing counts plus per-column
attribution:

| Workload | tdiff | DuckDB SQL |
|---|---:|---:|
| 10M×15 parquet vs parquet | **1.1 s / 0.7 GB** | 1.3 s / 1.7 GB |
| 10M×15 csv vs csv | **2.5 s / 0.5 GB** | 3.9 s / 1.9 GB |
| 10M×15 csv.gz | **7.5 s / 0.2 GB** | 12.1 s / 2.0 GB |
| 100M×15 parquet | **17.2 s / 0.9 GB** | 129 s / 6.4 GB, swapping |
| 1B×5 parquet (70 GB/side) | **124 s / 1.1 GB** | not attempted on 16 GB |

`--summary` is one scan instead of two: the 1B diff drops to 61 s, csv.gz to
3.8 s.

The rest is what SQL does not hand you: per-column attribution and example
rows without a second unpivot query, exit codes and `--max-diff` budgets,
differing rows exported as data, key inference, duplicate-key handling,
dirty-CSV recovery, snapshot pruning, and merge-on-read semantics.

If your data already lives in a warehouse, use the warehouse. tdiff is for
data in files and lake tables, compared on the machine you are on.

## Not building

Warehouse connectors and live-database diffing, dbt integration, lineage or
data-quality rules, UIs, Excel. tdiff answers one question, which rows
differ, and is built to be the best at that.

## Embedding

`pkg/tdiff` wraps the engine for use as a library:

```go
res, err := tdiff.Diff("a.parquet", "b.parquet", tdiff.Options{Key: []string{"id"}})
```

## Development

```bash
go test ./...              # manifest oracle, interop corpus, table fixtures, torture matrix
go test -race ./internal/...
golangci-lint run ./...
go run ./bench/gen --help  # fixture generator
```

The fixture generator plants a known set of diffs and writes the ground truth
to `manifest.json`; the suite checks against it for every format combination
in both join modes. Lake-table fixtures are written by pyiceberg and
delta-rs, and the merge-on-read delete files are validated by reading them
back with pyiceberg and DuckDB. See [bench/README.md](bench/README.md) for
the performance and torture suites.

## License

MIT. See [LICENSE](LICENSE).
