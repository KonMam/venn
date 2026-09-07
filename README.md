# venn

[![ci](https://github.com/KonMam/venn/actions/workflows/ci.yml/badge.svg)](https://github.com/KonMam/venn/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/KonMam/venn?sort=semver)](https://github.com/KonMam/venn/releases)
[![go reference](https://pkg.go.dev/badge/github.com/KonMam/venn.svg)](https://pkg.go.dev/github.com/KonMam/venn)
[![license](https://img.shields.io/github/license/KonMam/venn)](LICENSE)

Row-level keyed diff of tabular datasets: files, directories, and lake tables.
One binary, pure Go, no server and no SQL to write.

```console
$ venn prod_export.parquet migrated.csv --key id
schema: identical
rows:   +50,000 added   -50,137 removed   ~100,008 changed   =9,849,855 unchanged   (left 10,000,000, right 9,999,863)
changed columns: price(41,203) qty(38,900) updated_at(31,077)
$ echo $?
1
```

Point it at two Iceberg snapshots and it tells you which rows changed, not
just which files:

```console
$ venn s3://lake/orders#8412 s3://lake/orders#8500 --key order_id
venn: s3://lake/orders: skipping 412 data files shared by both snapshots (96,401,220 rows per side)
schema: identical
rows:   +1,204,551 added   -0 removed   ~88,012 changed   =96,530,190 unchanged   (left 96,618,202, right 97,822,753)
changed columns: status(88,012)
```

## Install

A single static binary, no runtime and no dependencies.

**Download** the archive for your platform from
[releases](https://github.com/KonMam/venn/releases/latest), extract, and put
`venn` on your `PATH`. Linux, macOS and Windows, amd64 and arm64. Each
release ships a `checksums.txt`.

```bash
# linux amd64, adjust the tag and platform
VER=0.1.1
curl -fsSL "https://github.com/KonMam/venn/releases/download/v${VER}/venn_${VER}_linux_amd64.tar.gz" \
  | tar -xz venn && sudo mv venn /usr/local/bin/
```

**Homebrew** (macOS):

```bash
brew install KonMam/tap/venn
```

**Docker**:

```bash
docker run --rm -v "$PWD:/data" ghcr.io/konmam/venn /data/left.parquet /data/right.parquet --key id
```

**Python**, if the rest of your stack is:

```bash
pip install venn-bin     # installs the same binary and puts venn on PATH
```

**From source**, needs Go 1.26 or newer:

```bash
go install github.com/KonMam/venn/cmd/venn@latest
```

## Status

Pre-1.0 and in active development, but not experimental: every release is
gated on a ground-truth oracle covering each supported format combination, a
corruption torture suite, and a performance check that runs on every pull
request. Expect flag names and output shapes to still move before 1.0. The
parts to build CI on are the exit codes (`0` equal, `1` differences, `2`
error) and the `--format json` field names; those will not change without a
major version. Issues and bug reports are welcome.

## What it's for

- **Migration validation.** The old pipeline's output against the new one's,
  before you cut over. Any format diffs against any other, and values compare
  by logical type, so a parquet INT64 `10` equals a CSV `10.0`.
- **Pipeline regression in CI.** Yesterday's output against today's, with an
  allowed-change budget, an exit code, and a markdown report in the job
  summary. There is a [GitHub Action](#github-action).
- **Lake table snapshot deltas.** Which rows changed between two Iceberg
  snapshots or Delta versions. No engine, no cluster, no warehouse.

venn answers one question, which rows differ. Warehouse connectors and
live-database diffing, dbt integration, lineage and data-quality rules, UIs
and Excel are out of scope. If your data already lives in a warehouse, use
the warehouse; venn is for data in files and lake tables, compared on the
machine you are on.

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
venn <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
venn schema <left> <right>                            schema diff only
venn snapshot <file> --key <col> --output <b.snap>    save a hash baseline
venn <file> --against <b.snap>                        diff against a baseline
```

Exit codes: `0` identical or within budget, `1` differences, `2` error.

Full reference in [docs/flags.md](docs/flags.md), worked examples in
[docs/usage.md](docs/usage.md), and the comparison rules in
[docs/semantics.md](docs/semantics.md).

## CI

```bash
venn old/ new/ --key id --summary          # fastest: counts only
venn old/ new/ --key id --max-diff 0.1%    # allow small drift
venn old/ new/ --key id --format json      # machine-readable
venn old/ new/ --key id --report out.md    # markdown for humans
```

### GitHub Action

```yaml
- uses: KonMam/venn@v0.1.1
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
venn snapshot expected.parquet --key id --output baseline.snap
venn build/output.parquet --against baseline.snap
```

## Performance

An M1 Pro laptop with 16 GB, counts plus per-column attribution:

| Workload | Wall | Peak RSS |
|---|---:|---:|
| 10M×15 parquet vs parquet | 1.1 s | 0.7 GB |
| 10M×15 csv vs csv | 2.5 s | 0.5 GB |
| 10M×15 csv.gz | 7.5 s | 0.2 GB |
| 100M×15 parquet | 17.2 s | 0.9 GB |
| 1B×5 parquet (70 GB/side) | 124 s | 1.1 GB |

The batch engine keeps a bounded number of rows in flight whatever the input
size, so the 1B-row diff peaks at roughly the footprint of the 10M-row one.
`--summary` is one scan instead of two: the 1B diff drops to 61 s, csv.gz to
3.8 s.

The method, the pinned tool versions and the same workloads under a DuckDB
SQL query are in [docs/performance.md](docs/performance.md).

## Embedding

`pkg/venn` wraps the engine for use as a library:

```go
res, err := venn.Diff("a.parquet", "b.parquet", venn.Options{Keys: []string{"id"}})
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
back with pyiceberg and DuckDB.

[CONTRIBUTING.md](CONTRIBUTING.md) covers what CI checks, including the
per-PR performance gate, and which changes need a test.
[bench/README.md](bench/README.md) documents the performance and torture
suites.

## License

MIT. See [LICENSE](LICENSE).
