# tdiff

Fast, single-binary diff for tabular data files. Pure Go, no CGO, no server,
no SQL to write.

```
$ tdiff prod_export.parquet migrated.csv --key id
schema: identical
rows:   +50,000 added   -50,137 removed   ~100,008 changed   =9,849,855 unchanged   (left 10,000,000, right 9,999,863)
changed columns: price(41,203) qty(38,900) updated_at(31,077)
$ echo $?
1
```

## Why

Comparing two tables today means hand-writing a DuckDB `FULL OUTER JOIN …
IS DISTINCT FROM` query, spinning up pandas/DataComPy, or reviving the
archived `data-diff`. tdiff is the one-command version of that workflow:

- **Any format against any format.** parquet vs CSV works — validate a
  CSV→parquet migration or an export against its source directly. The diff
  engine is format-agnostic; each format is just a reader. Values compare by
  logical type, so parquet INT64 `10` equals CSV-inferred `10.0`.
- **Keyed row diff, not text diff.** Added / removed / changed rows by key,
  plus which columns changed and example rows. Row order never matters.
- **Fast and lean.** All cores, columnar batch decoding, ~17 bytes of table
  per row. 10M rows × 15 columns diff in ~1.2 s on a laptop — numbers, method,
  and competitors in [BENCHMARKS.md](BENCHMARKS.md).
- **CI-friendly.** Exit code 0 = identical, 1 = differences, 2 = error.
  `--format json` for machines. The identical-files case is the fastest path.

## Usage

```
tdiff <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
tdiff schema <left> <right>                            schema diff only
tdiff snapshot <file> --key <col> --output <b.snap>    save a hash baseline
tdiff <file> --against <b.snap>                        diff vs the baseline

--key <cols>             key column(s); omitted = auto-inferred (unique in
                         both inputs, id-ish names preferred)
--ignore-columns <cols>  columns to exclude from comparison
--format human|json      output format (default human)
--limit <n>              max example rows per category (default 10)
--verbose                print example rows
--summary                counts + exit code only — the fastest mode, made for
                         CI gates: skips column attribution and example rows
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
--against <b.snap>       diff one file against a snapshot baseline
```

Formats: `.parquet`, `.csv`, `.tsv`, `.ndjson`/`.jsonl` — text formats also
`.gz`/`.zst` compressed. Inputs can be local paths, `http(s)://` URLs
(parquet reads by byte range; servers without range support are downloaded
once), or `s3://bucket/key` (AWS default credential chain). Types for text
formats are inferred from a sample; a value that contradicts the sample
later re-reads that column as string with a warning instead of failing.
Keys must be unique per side unless `--on-dup warn`.

## Semantics worth knowing

- Column matching is by name; column order and physical type may differ.
  Columns whose logical types are incomparable (e.g. string vs timestamp) are
  reported in the schema diff and excluded from the row diff. Nested parquet
  and JSON columns are skipped with a warning.
- Parquet interop is tested against files written by pyarrow, DuckDB and
  polars — dictionary encoding, data page v1/v2, snappy/gzip/zstd/
  uncompressed, INT96 timestamps, int-backed DECIMAL (compared in the float
  domain), delta encodings.
- int64 and float64 columns compare numerically; timestamps compare at
  microsecond precision (UTC); `-0 == 0`; `NaN == NaN` (identical inputs must
  diff as identical).
- CSV cannot represent NULL for string columns (`""` is an empty string);
  numeric/timestamp empty fields read as NULL.
- Nested parquet schemas and DECIMAL are not supported yet.

## Status / roadmap

v0.2 territory: parquet + CSV/TSV, in-memory keyed diff (~17 B/row) plus a
streaming grace-hash-join mode whose peak memory is bounded by partition
size, not input size — 100M-row diffs run on a laptop. Honest gaps, in the
order they'll close:

1. Arrow / SQLite sources.
2. Database connections: only if users actually pull for it.

## Development

```
go test ./...                 # correctness suite (manifest-driven oracle)
go run ./bench/gen --help     # fixture/dataset generator
python3 bench/run_bench.py    # full benchmark suite (see BENCHMARKS.md)
```

The fixture generator plants a known set of diffs and writes the ground truth
to `manifest.json`; the test suite and every benchmark's correctness gate
check against it. Same generator, same manifest, every format combination.
