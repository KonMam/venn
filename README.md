# venn

**Row-level, keyed diff of tabular datasets — files, directories, and lake
tables — at any scale, on one machine, in CI.**

Single binary, pure Go, no server, no SQL to write.

```
$ venn prod_export.parquet migrated.csv --key id
schema: identical
rows:   +50,000 added   -50,137 removed   ~100,008 changed   =9,849,855 unchanged   (left 10,000,000, right 9,999,863)
changed columns: price(41,203) qty(38,900) updated_at(31,077)
$ echo $?
1
```

```
$ venn s3://lake/orders#8412 s3://lake/orders#8500 --key order_id
venn: s3://lake/orders: skipping 412 data files shared by both snapshots (96,401,220 rows per side)
schema: identical
rows:   +1,204,551 added   -0 removed   ~88,012 changed   =96,530,190 unchanged   (left 96,618,202, right 97,822,753)
changed columns: status(88,012)
```

## What it's for

- **Migration validation** — the old pipeline's output against the new one's,
  parquet against CSV against NDJSON, before you cut over. Any format diffs
  against any other; values compare by logical type, so parquet INT64 `10`
  equals CSV `10.0`.
- **Pipeline regression in CI** — yesterday's output vs today's, with a
  [GitHub Action](#github-action), an allowed-change budget, and a markdown
  report in the job summary.
- **Lake table snapshot deltas** — *which rows* changed between two Iceberg
  snapshots or Delta versions, not just which files. No engine, no cluster,
  no warehouse.

## What it reads

| Input | Notes |
|---|---|
| `.parquet` | custom decode kernels; dictionary, v1/v2 pages, snappy/gzip/zstd, INT96, int-backed DECIMAL, delta encodings |
| `.csv` `.tsv` `.ndjson` `.jsonl` | plus `.gz`/`.zst`; types inferred from a sample, with automatic recovery when later values contradict it |
| directories & globs | multi-file datasets, union-by-name schemas, hive partition columns (`region=eu/…`) become real columns |
| **Iceberg tables** | point at the table root; `#<snapshot-id>` picks a snapshot; v1/v2, merge-on-read (position *and* equality deletes) |
| **Delta tables** | `#<version>` picks a version; checkpoint + log replay, deletion vectors |
| `s3://`, `http(s)://` | tables and files alike; parquet reads by byte range — no full download |

Diffing two snapshots of the *same* table skips data files live in both
(copy-on-write tables hold each row in exactly one file, so shared files
cancel exactly — a 1% churn on a huge table scans ~2% of it). Files whose
delete state differs between snapshots are never skipped.

## Why not just DuckDB?

You can hand-write this diff as a `FULL OUTER JOIN … IS DISTINCT FROM` query,
and DuckDB will run it well. Measured on the same laptop (M1 Pro, 16 GB),
same data, single runs, DuckDB 1.4.3, both tools computing the full result
(counts plus per-column attribution); time / peak RSS:

| Workload | venn | DuckDB SQL |
|---|---:|---:|
| 10M×15 parquet vs parquet | **1.1 s / 0.7 GB** | 1.3 s / 1.7 GB |
| 10M×15 csv vs csv | **2.5 s / 0.5 GB** | 3.9 s / 1.9 GB |
| 10M×15 csv.gz | **7.5 s / 0.2 GB** | 12.1 s / 2.0 GB |
| 100M×15 parquet | **17.2 s / 0.9 GB** | 129 s / 6.4 GB, swapping |
| 1B×5 parquet (70 GB/side) | **124 s / 1.1 GB** | not attempted on 16 GB |

`--summary` (counts only, the CI-gate mode) is one scan instead of two:
the 1B diff drops to **61 s**, csv.gz to 3.8 s.

And the parts SQL doesn't give you: per-column change attribution and example
rows without a second unpivot query, exit codes and `--max-diff` budgets,
`--output` of differing rows as data, key inference, duplicate-key handling,
dirty-CSV recovery, snapshot pruning, and merge-on-read semantics — DuckDB's
own Iceberg reader currently crashes on equality-delete tables.

If you live inside a warehouse and your data is already there, use the
warehouse. venn is for data in files and lake tables, compared on the
machine you're on.

## CI

Exit codes: `0` identical (or within budget) · `1` differences · `2` error.

```
venn old/ new/ --key id --summary                  # fastest: counts only
venn old/ new/ --key id --max-diff 0.1%            # allow small drift
venn old/ new/ --key id --format json              # machine-readable
venn old/ new/ --key id --report report.md         # markdown for humans
```

### GitHub Action

```yaml
- uses: KonMam/venn@main
  with:
    left: expected/orders.parquet
    right: build/orders.parquet
    key: order_id
    max-diff: "0.1%"
```

The step fails when differences exceed the budget, appends a markdown report
(counts table, changed columns, example rows) to the job summary, and exposes
`added` / `removed` / `changed` / `unchanged` as outputs.

### Snapshot baselines

When the "left" side shouldn't be re-read every run, freeze it once —
16 bytes per row, whatever the width:

```
venn snapshot expected.parquet --key id --output baseline.snap
venn build/output.parquet --against baseline.snap
```

## Usage

```
venn <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
venn schema <left> <right>                            schema diff only
venn snapshot <file> --key <col> --output <b.snap>    save a hash baseline
venn <file> --against <b.snap>                        diff vs the baseline

--key <cols>             key column(s); omitted = auto-inferred (unique in
                         both inputs, id-ish names preferred)
--ignore-columns <cols>  columns to exclude from comparison
--format <fmt>           human, json, or markdown (default human)
--report <file.md>       also write a markdown report (CI step summaries)
--limit <n>              max example rows per category (default 10)
--verbose                print example rows
--summary                counts + exit code only — the fastest mode: skips
                         column attribution and example rows
--mode auto|memory|stream  join strategy; stream is a grace hash join that
                         spills hashes to disk and keeps peak memory flat for
                         larger-than-RAM inputs (auto picks it above 40M rows)
--tmpdir <dir>           spill directory for stream mode (default system temp)
--output <file>          write the differing rows as data (.csv or .parquet):
                         key columns, diff_status, <col>__left/<col>__right
--max-diff <n | p%>      CI gate: exit 0 while total differing rows stay
                         within budget (schema changes still exit 1)
--float-precision <n>    round float comparisons to n decimal digits — exact,
                         hash-consistent quantization (not an epsilon)
--on-dup error|warn      duplicate keys: fail (default) or keep the first
                         occurrence per side and report the rest
--infer-rows <n>         CSV/NDJSON type-inference sample (default 1000;
                         -1 = whole file)
```

## Semantics worth knowing

- Column matching is by name; column order and physical type may differ.
  Columns whose logical types are incomparable (e.g. string vs timestamp) are
  reported in the schema diff and excluded from the row diff. Nested parquet
  and JSON columns are skipped with a warning.
- Parquet interop is tested against files written by pyarrow, DuckDB, and
  polars; lake-table support against real pyiceberg and delta-rs tables, with
  merge-on-read fixtures cross-validated by independent readers.
- int64 and float64 columns compare numerically; timestamps compare at
  microsecond precision (UTC); `-0 == 0`; `NaN == NaN` (identical inputs must
  diff as identical).
- Iceberg sequence-number rules are honored: position deletes apply to data
  files at or before the delete's sequence; equality deletes apply strictly
  before — an upsert's re-added row survives its own delete.
- CSV cannot represent NULL for string columns (`""` is an empty string);
  numeric/timestamp empty fields read as NULL.
- Table partition values compare as strings; lake-table data files must be
  parquet (that is what engines write).

## Not building

Warehouse connectors and live-database diffing, dbt integration, lineage or
data-quality rules, UIs, Excel. venn answers one question — *which rows
differ* — and is built to be the best at exactly that.

## Development

```
go test ./...                            # correctness suite (manifest oracle,
                                         # interop corpus, table fixtures, fuzz corpus)
go run ./bench/gen --help                # fixture/dataset generator
bench/venv/bin/python testdata/tables/gen_tables.py testdata/tables   # lake fixtures
bench/venv/bin/python testdata/tables/gen_mor.py testdata/tables      # merge-on-read fixtures
```

The fixture generator plants a known set of diffs and writes the ground truth
to `manifest.json`; the test suite checks against it for every format
combination, in both join modes. Lake-table fixtures are written by pyiceberg
and delta-rs; merge-on-read delete files are built to spec and validated by
reading them back with pyiceberg and DuckDB.
