# tdiff

**Row-level, keyed diff of tabular datasets — files, directories, and lake
tables — at any scale, on one machine, in CI.**

Single binary, pure Go, no server, no SQL to write.

```
$ tdiff prod_export.parquet migrated.csv --key id
schema: identical
rows:   +50,000 added   -50,137 removed   ~100,008 changed   =9,849,855 unchanged   (left 10,000,000, right 9,999,863)
changed columns: price(41,203) qty(38,900) updated_at(31,077)
$ echo $?
1
```

```
$ tdiff s3://lake/orders#8412 s3://lake/orders#8500 --key order_id
tdiff: s3://lake/orders: skipping 412 data files shared by both snapshots (96,401,220 rows per side)
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

| Workload | tdiff | DuckDB SQL |
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
warehouse. tdiff is for data in files and lake tables, compared on the
machine you're on.

## CI

Exit codes: `0` identical (or within budget) · `1` differences · `2` error.

```
tdiff old/ new/ --key id --summary                  # fastest: counts only
tdiff old/ new/ --key id --max-diff 0.1%            # allow small drift
tdiff old/ new/ --key id --format json              # machine-readable
tdiff old/ new/ --key id --report report.md         # markdown for humans
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
(counts table, changed columns, example rows) to the job summary, and exposes
`added` / `removed` / `changed` / `unchanged` as outputs.

### Snapshot baselines

When the "left" side shouldn't be re-read every run, freeze it once —
16 bytes per row, whatever the width:

```
tdiff snapshot expected.parquet --key id --output baseline.snap
tdiff build/output.parquet --against baseline.snap
```

## Usage

```
tdiff <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
tdiff schema <left> <right>                            schema diff only
tdiff snapshot <file> --key <col> --output <b.snap>    save a hash baseline
tdiff <file> --against <b.snap>                        diff vs the baseline

--key <cols>             key column(s); omitted = auto-inferred (unique in
                         both inputs, id-ish names preferred)
--keyless                no key: match whole rows as a multiset
--where <predicate>      keep only the rows matching this predicate, on both
                         sides (repeatable, ANDed)
--ignore-columns <cols>  columns to exclude from comparison
--rename <right>=<left>  compare a right-side column under a left-side name
                         (repeatable) — a renamed column is compared, not
                         reported as one added and one removed column
--format <fmt>           human, json, or markdown (default human)
--report <file>          also write a report (repeatable): .html for a
                         self-contained page, anything else markdown (CI
                         step summaries)
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
                         within budget (schema changes still exit 1); the run
                         stops as soon as the budget is provably blown
--mask <cols>            show these columns' values as a stable short token
                         in examples, reports and --output
--float-precision <n>    round float comparisons to n decimal digits — exact,
                         hash-consistent quantization (not an epsilon)
--tolerance <spec>       treat numeric differences this small as equal
                         (repeatable): 0.01 · 0.01,rel=1e-6 · rel=1e-6 ·
                         price=0.01 — needs full diff mode
--ignore-case            compare strings case-insensitively
--trim                   ignore leading/trailing whitespace in strings
--timestamp-precision <p>  compare timestamps at s, ms or us precision
--on-dup <mode>          duplicate keys: error (default, fail), warn (keep
                         the first occurrence per side), or match (pair a
                         key's rows as multisets)
--infer-rows <n>         CSV/NDJSON type-inference sample (default 1000;
                         -1 = whole file)
--delimiter <char>       delimited-text field separator (default: `,` for
                         .csv, tab for .tsv)
--version                print version
```

### Renamed columns

A column renamed between the two sides would otherwise show up as one added
and one removed column, with its values never compared. `--rename` maps it
back:

```console
$ tdiff yesterday.parquet today.parquet --rename cust_id=customer_id
schema: ~ column customer_id ⇐ cust_id (renamed)
rows:   +0 added   -0 removed   ~1 changed   =1 unchanged   (left 2, right 2)
```

The mapping is `<right name>=<left name>`, repeatable, and applies before
anything else — so the renamed column can be the key, can be excluded with
`--ignore-columns`, and shows up under its left-side name in `--output` and
in the reports. A rename is reported but does *not* make the schemas differ:
you declared the columns equivalent. Both names are validated, so a typo is
an error rather than a silently unmatched column.

(Rename *detection* — suggesting likely pairs in a schema diff — is a
separate thing tdiff does not do yet.)

### Filtering rows

`--where` cuts both inputs down before the diff runs:

```bash
tdiff a.parquet b.parquet --key id --where "region = 'eu'" --where "price > 10"
tdiff lake/orders lake/orders_v2 --key id --where "dt >= 2026-01-01"
```

The grammar is deliberately small — `col OP literal` (`=` `!=` `<` `<=` `>`
`>=`) or `col IS [NOT] NULL`, ANDed by repeating the flag or writing `and`
between clauses. Literals are typed against the column: numbers for numeric
columns, `2026-01-31` / `2026-01-31T12:00:00Z` for dates and timestamps,
`true`/`false` for booleans, quoted or bare text for strings. A column that
does not exist, or a literal that does not fit its column, is an error —
never a filter that silently keeps everything.

Two things worth knowing:

- **The counts are of the filtered rows.** `filter:` appears in every report
  saying so. A row kept on one side and filtered out on the other is reported
  as added or removed, which is usually what you want (it *did* leave the
  filtered set) but is worth remembering when the predicate touches a column
  that changed.
- **Partition predicates skip whole files.** A predicate on a hive directory
  segment (`region=eu/…`) or an Iceberg/Delta partition column is constant
  per file, so those files are never opened. Correctness never depends on it
  — the row filter would have removed the same rows — it just makes a
  filtered lake diff much cheaper. Partition values compare as text, the same
  as everywhere else in tdiff.

A snapshot records the filter it was taken under, so a filtered baseline can
never be compared against a differently-filtered file.

### Duplicate keys, and no key at all

A keyed diff needs unique keys: without them "which right row corresponds to
this left row?" has no answer. tdiff's default is to say so and stop. Two
flags give it an answer instead.

`--on-dup match` pairs a duplicated key's rows as **multisets**: identical
rows cancel as unchanged, and whatever is left over is added on the right or
removed on the left. Nothing inside a duplicate group is reported as
*changed* — deciding which of three left rows "became" which of three right
rows is genuinely ambiguous, and guessing reads as authoritative when it is
not.

```console
$ tdiff a.csv b.csv --key id --on-dup match --verbose
dups:   2 keys duplicated on the left (4 rows), matched as multisets
rows:   +1 added   -0 removed   ~1 changed   =4 unchanged   (left 5, right 6)
```

The one refinement: leftovers of exactly **one row on each side** keep the
"changed" label, so ordinary changed rows do not lose their column
attribution. That rule depends on the leftover counts, never on arrival
order, so the counts are identical run to run at any thread count — which is
the difference from datacompy, whose rank-within-group pairing assumes a
deterministic row order that a parallel scan does not have.

`--keyless` drops the key entirely and matches whole rows as a multiset.
`Changed` is then always 0 by construction: a rewritten row is one removed
plus one added.

```console
$ tdiff events-before.ndjson events-after.ndjson --keyless --verbose
rows:   +2 added   -1 removed   ~0 changed   =3 unchanged   (left 4, right 5)
+ row=1|A
- row=3|C
```

Both modes need the in-memory join (`--mode memory`): identifying *which*
rows of a duplicate group were the leftovers takes the full row hash, which
the streaming pass matches by key hash alone. tdiff says so rather than
guessing. `--tolerance` is refused with either, for the same reason `--summary`
refuses it: there is no pairing to apply an epsilon to.

### CI gate with early exit

`--max-diff` is a budget: the run passes while the total differing rows stay
within it. Because added and changed rows alone already settle an
over-budget verdict — removed rows can only add to the total — the run stops
the moment the budget is provably blown instead of finishing the scan:

```console
$ tdiff a.parquet b.parquet --key id --max-diff 100
rows:   ≥+0 added   ≥-0 removed   ≥~1,738 changed   (scanned left 1,000,000, right 16,384)
abort:  --max-diff budget of 100 exceeded; the scan stopped early, so the counts are lower bounds
```

On a 1M-row, 10%-different pair that is 0.06 s instead of 0.63 s. The counts
are then marked as lower bounds everywhere — never presented as totals. Two
cases deliberately run to completion instead:

- a **percentage** budget whose denominator is not yet known (CSV/NDJSON have
  no up-front row count, so the budget itself could still grow); parquet and
  lake tables carry theirs, so those abort exactly.
- `--output`: half an export file is worse than a slower run.

### Masking values

`--mask <cols>` replaces those columns' values with a short stable token
(`xxh:9f3a1c22`) wherever they would be shown — examples, keys, every report
format, and `--output`. The comparison itself still uses the real values, so
the counts are unaffected.

```console
$ tdiff a.csv b.csv --key id --mask email --verbose
masked: email (values shown as xxh: tokens)
~ key=2: email: xxh:d526b6cf → xxh:0d6bf068; amount: 20 → 25;
```

Equal values give equal tokens, so rows stay correlatable across a report.
**It is not anonymization**: the token is a plain hash of the value, so a
low-cardinality column can be recovered by hashing the candidates. Use it to
keep values out of a report, not to make a report safe to publish.

### HTML report

`--report out.html` writes one self-contained page — inline CSS and JS, zero
external requests — with the verdict, counts, schema changes, per-column
match-rate bars, and sortable example tables. It follows the reader's
light/dark preference and opens straight out of a CI artifact zip or an email
attachment, air-gapped. Any other extension writes the markdown report, and
`--report` is repeatable — one run can produce both:

```bash
tdiff a.parquet b.parquet --key id --report summary.md --report report.html
```

### Loosening the comparison

Four flags change what counts as equal. Three of them are *normalizations* —
a deterministic canonicalization of each value — so they apply everywhere:
inside the join, in `--summary`, and in snapshots.

| Flag | Effect |
|---|---|
| `--float-precision <n>` | round floats to n decimal digits |
| `--ignore-case` | fold strings to lower case (Unicode) |
| `--trim` | strip leading/trailing whitespace from strings |
| `--timestamp-precision s\|ms\|us` | truncate timestamps to that grid |

These also apply to **key** columns, so keys differing only by case or
padding still join.

`--tolerance` is different. An epsilon is not transitive, so no hash can
encode it — it therefore never touches the join and instead reclassifies
rows during column attribution. Consequences:

- It needs full diff mode: `--tolerance` with `--summary` or `--against` is
  refused, pointing at `--float-precision` instead.
- Tolerable rows are reported as **within tolerance**, not as unchanged —
  the counts never claim two different values were the same.
- It applies to int64 and float columns only. For timestamps use
  `--timestamp-precision`.

```console
$ tdiff yesterday.parquet today.parquet --key id --tolerance price=0.01
rows:   +0 added   -0 removed   ~412 changed   =9,999,588 unchanged   (…)
tol:    1,203 rows differ only within tolerance (not counted as changed)
changed columns: price(412, 99.996% match, max Δ 3.19, mean Δ 0.41)
```

Snapshots record every normalization in their header, so diffing a baseline
under different settings is refused rather than silently comparing different
values.

### Per-column statistics

Every full diff reports, per changed column, how many row pairs differ, the
share that match, and — for numeric columns — the largest and mean absolute
difference. They appear in `human` output, as `column_stats` in `--format
json`, and as a table in the markdown report.

## Semantics worth knowing

- Column matching is by name; column order and physical type may differ.
  Columns whose logical types are incomparable (e.g. string vs timestamp) are
  reported in the schema diff and excluded from the row diff. Nested parquet
  and JSON columns are skipped with a warning.
- Parquet interop is tested against files written by pyarrow, DuckDB, and
  polars; lake-table support against real pyiceberg and delta-rs tables, with
  merge-on-read fixtures cross-validated by independent readers.
- int64 and float64 columns compare numerically; timestamps compare at
  microsecond precision (UTC) unless `--timestamp-precision` coarsens it;
  `-0 == 0`; `NaN == NaN` (identical inputs must diff as identical).
- Iceberg sequence-number rules are honored: position deletes apply to data
  files at or before the delete's sequence; equality deletes apply strictly
  before — an upsert's re-added row survives its own delete.
- CSV cannot represent NULL for string columns (`""` is an empty string);
  numeric/timestamp empty fields read as NULL.
- Table partition values compare as strings; lake-table data files must be
  parquet (that is what engines write).

## Not building

Warehouse connectors and live-database diffing, dbt integration, lineage or
data-quality rules, UIs, Excel. tdiff answers one question — *which rows
differ* — and is built to be the best at exactly that.

## Development

```
go test ./...                            # correctness suite (manifest oracle,
                                         # interop corpus, table fixtures, fuzz corpus)
golangci-lint run ./...                  # lint (also a CI gate)
go run ./bench/gen --help                # fixture/dataset generator
bench/venv/bin/python testdata/tables/gen_tables.py testdata/tables   # lake fixtures
bench/venv/bin/python testdata/tables/gen_mor.py testdata/tables      # merge-on-read fixtures
```

The fixture generator plants a known set of diffs and writes the ground truth
to `manifest.json`; the test suite checks against it for every format
combination, in both join modes. Lake-table fixtures are written by pyiceberg
and delta-rs; merge-on-read delete files are built to spec and validated by
reading them back with pyiceberg and DuckDB.
