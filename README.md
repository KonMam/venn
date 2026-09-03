# venn

Fast, single-binary diff for tabular data files. Pure Go, no CGO, no server,
no SQL to write.

```
$ venn prod_export.parquet migrated.csv --key id
schema: identical
rows:   +50,000 added   -50,137 removed   ~100,008 changed   =9,849,855 unchanged   (left 10,000,000, right 9,999,863)
changed columns: price(41,203) qty(38,900) updated_at(31,077)
$ echo $?
1
```

## Why

Comparing two tables today means hand-writing a DuckDB `FULL OUTER JOIN …
IS DISTINCT FROM` query, spinning up pandas/DataComPy, or reviving the
archived `data-diff`. venn is the one-command version of that workflow:

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
venn <left> <right> --key <col>[,<col>...] [flags]   row + schema diff
venn schema <left> <right>                           schema diff only

--key <cols>             key column(s), comma-separated (required for row diff)
--ignore-columns <cols>  columns to exclude from comparison
--format human|json      output format (default human)
--limit <n>              max example rows per category (default 10)
--verbose                print example rows
--summary                counts + exit code only — the fastest mode, made for
                         CI gates: skips column attribution and example rows
```

Formats: `.parquet`, `.csv`, `.tsv` (CSV/TSV types are inferred from a
1000-row sample). Keys must be unique per side.

## Semantics worth knowing

- Column matching is by name; column order and physical type may differ.
  Columns whose logical types are incomparable (e.g. string vs timestamp) are
  reported in the schema diff and excluded from the row diff.
- int64 and float64 columns compare numerically; timestamps compare at
  microsecond precision (UTC); `-0 == 0`; `NaN == NaN` (identical inputs must
  diff as identical).
- CSV cannot represent NULL for string columns (`""` is an empty string);
  numeric/timestamp empty fields read as NULL.
- Nested parquet schemas and DECIMAL are not supported yet.

## Status / roadmap

v0.1 territory: parquet + CSV/TSV, in-memory keyed diff (~17 B/row of the
smaller input, plus changed rows). Honest gaps, in the order they'll close:

1. **Streaming mode** for larger-than-RAM inputs (external sort / sorted
   inputs) — constant memory at 100M+ rows.
2. Key auto-inference, `--tolerance` for floats, NDJSON / Arrow / SQLite
   sources, snapshot-friendly CI output.
3. Database connections: only if users actually pull for it.

## Development

```
go test ./...                 # correctness suite (manifest-driven oracle)
go run ./bench/gen --help     # fixture/dataset generator
python3 bench/run_bench.py    # full benchmark suite (see BENCHMARKS.md)
```

The fixture generator plants a known set of diffs and writes the ground truth
to `manifest.json`; the test suite and every benchmark's correctness gate
check against it. Same generator, same manifest, every format combination.
