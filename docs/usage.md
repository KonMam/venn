# Usage

```
tdiff <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
tdiff schema <left> <right>                            schema diff only
tdiff snapshot <file> --key <col> --output <b.snap>    save a hash baseline
tdiff <file> --against <b.snap>                        diff against a baseline
```

Flags may appear before or after the positional arguments. Exit codes are
`0` for identical (or within budget), `1` for differences, and `2` for an
error.

## Flag reference

### Selecting rows and columns

| Flag | Effect |
|---|---|
| `--key <cols>` | key column(s), comma-separated. Omitted, the key is inferred: a column unique in both inputs, preferring id-ish names |
| `--keyless` | no key; match whole rows as a multiset |
| `--where <predicate>` | keep only matching rows, on both sides. Repeatable and ANDed |
| `--ignore-columns <cols>` | exclude these columns from the comparison |
| `--rename <right>=<left>` | compare a right-side column under a left-side name. Repeatable |

### Output

| Flag | Effect |
|---|---|
| `--format <fmt>` | `human` (default), `json`, or `markdown` |
| `--report <file>` | also write a report. Repeatable; `.html` writes a self-contained page, any other extension writes markdown |
| `--limit <n>` | max example rows per category (default 10) |
| `--verbose` | print example rows |
| `--summary` | counts and exit code only. The fastest mode: skips column attribution and examples |
| `--output <file>` | write the differing rows as data to a `.csv` or `.parquet` file |
| `--mask <cols>` | show these columns' values as a stable token |

### Comparison

| Flag | Effect |
|---|---|
| `--float-precision <n>` | round float comparisons to n decimal digits |
| `--tolerance <spec>` | treat numeric differences this small as equal. Repeatable |
| `--ignore-case` | compare strings case-insensitively |
| `--trim` | ignore leading and trailing whitespace in strings |
| `--timestamp-precision <p>` | compare timestamps at `s`, `ms` or `us` precision |

### Input handling

| Flag | Effect |
|---|---|
| `--infer-rows <n>` | CSV/NDJSON type-inference sample (default 1000; `-1` reads the whole file) |
| `--delimiter <char>` | delimited-text field separator (default `,` for `.csv`, tab for `.tsv`) |
| `--on-dup <mode>` | duplicate keys: `error` (default), `warn` (keep the first occurrence per side), or `match` (multiset pairing) |

### Execution

| Flag | Effect |
|---|---|
| `--mode auto\|memory\|stream` | join strategy. `stream` is a grace hash join that spills hashes to disk and keeps peak memory flat for larger-than-RAM inputs. `auto` picks it above 40M rows |
| `--tmpdir <dir>` | spill directory for stream mode (default: system temp) |
| `--max-diff <n\|p%>` | CI gate: exit 0 while total differing rows stay within budget |
| `--against <file>` | diff a single file against a snapshot baseline |
| `--version` | print version |

`--cpuprofile` and `--memprofile` write pprof profiles and exist for
development.

## Renamed columns

A column renamed between the two sides would otherwise show up as one added
and one removed column, with its values never compared. `--rename` maps it
back:

```console
$ tdiff yesterday.parquet today.parquet --rename cust_id=customer_id
schema: ~ column customer_id ⇐ cust_id (renamed)
rows:   +0 added   -0 removed   ~1 changed   =1 unchanged   (left 2, right 2)
```

The mapping is `<right name>=<left name>`, repeatable, and applies before
anything else, so the renamed column can be the key, can be excluded with
`--ignore-columns`, and appears under its left-side name in `--output` and in
reports. A rename is reported but does not make the schemas differ: you
declared the columns equivalent. Both names are validated, so a typo is an
error rather than a silently unmatched column.

Rename *detection*, suggesting likely pairs in a schema diff, is a separate
thing tdiff does not do.

## Filtering rows

`--where` cuts both inputs down before the diff runs:

```bash
tdiff a.parquet b.parquet --key id --where "region = 'eu'" --where "price > 10"
tdiff lake/orders lake/orders_v2 --key id --where "dt >= 2026-01-01"
```

The grammar is deliberately small: `col OP literal` (`=` `!=` `<` `<=` `>`
`>=`) or `col IS [NOT] NULL`, ANDed by repeating the flag or writing `and`
between clauses. Literals are typed against the column: numbers for numeric
columns, `2026-01-31` or `2026-01-31T12:00:00Z` for dates and timestamps,
`true`/`false` for booleans, quoted or bare text for strings. A column that
does not exist, or a literal that does not fit its column, is an error and
never a filter that silently keeps everything.

Two things worth knowing:

- **The counts are of the filtered rows,** and `filter:` appears in every
  report saying so. A row kept on one side and filtered out on the other is
  reported as added or removed, which is usually what you want, since it did
  leave the filtered set, but is worth remembering when the predicate touches
  a column that changed.
- **Partition predicates skip whole files.** A predicate on a hive directory
  segment (`region=eu/…`) or an Iceberg/Delta partition column is constant
  per file, so those files are never opened. Correctness does not depend on
  it, since the row filter would have removed the same rows; it just makes a
  filtered lake diff much cheaper.

A snapshot records the filter it was taken under, so a filtered baseline
cannot be compared against a differently-filtered file.

## Duplicate keys, and no key at all

A keyed diff needs unique keys. Without them, "which right row corresponds to
this left row?" has no answer, and tdiff's default is to say so and stop. Two
flags give it an answer instead.

`--on-dup match` pairs a duplicated key's rows as multisets: identical rows
cancel as unchanged, and whatever is left over is added on the right or
removed on the left. Nothing inside a duplicate group is reported as
*changed*, because deciding which of three left rows became which of three
right rows is genuinely ambiguous, and guessing reads as authoritative when
it is not.

```console
$ tdiff a.csv b.csv --key id --on-dup match --verbose
dups:   2 keys duplicated on the left (4 rows), matched as multisets
rows:   +1 added   -0 removed   ~1 changed   =4 unchanged   (left 5, right 6)
```

The one refinement: leftovers of exactly one row on each side keep the
"changed" label, so ordinary changed rows do not lose their column
attribution. That rule depends on the leftover counts and never on arrival
order, so the counts are identical run to run at any thread count.

`--keyless` drops the key entirely and matches whole rows as a multiset.
`Changed` is then always 0 by construction: a rewritten row is one removed
plus one added.

```console
$ tdiff events-before.ndjson events-after.ndjson --keyless --verbose
rows:   +2 added   -1 removed   ~0 changed   =3 unchanged   (left 4, right 5)
+ row=1|A
- row=3|C
```

Both modes need the in-memory join (`--mode memory`). Identifying which rows
of a duplicate group were the leftovers takes the full row hash, and the
streaming pass matches by key hash alone. `--tolerance` is refused with
either, for the same reason `--summary` refuses it: there is no pairing to
apply an epsilon to.

## CI gate with early exit

`--max-diff` is a budget: the run passes while the total differing rows stay
within it. Added and changed rows alone already settle an over-budget
verdict, since removed rows can only add to the total, so the run stops the
moment the budget is provably blown instead of finishing the scan:

```console
$ tdiff a.parquet b.parquet --key id --max-diff 100
rows:   ≥+0 added   ≥-0 removed   ≥~1,738 changed   (scanned left 1,000,000, right 16,384)
abort:  --max-diff budget of 100 exceeded; the scan stopped early, so the counts are lower bounds
```

On a 1M-row, 10%-different pair that is 0.06 s instead of 0.63 s. The counts
are then marked as lower bounds everywhere and never presented as totals.
Two cases run to completion instead:

- A percentage budget whose denominator is not yet known. CSV and NDJSON have
  no up-front row count, so the budget itself could still grow; parquet and
  lake tables carry theirs, so those abort exactly.
- `--output`, where half an export file is worse than a slower run.

## Masking values

`--mask <cols>` replaces those columns' values with a short stable token
(`xxh:9f3a1c22`) wherever they would be shown: examples, keys, every report
format, and `--output`. The comparison itself still uses the real values, so
the counts are unaffected.

```console
$ tdiff a.csv b.csv --key id --mask email --verbose
masked: email (values shown as xxh: tokens)
~ key=2: email: xxh:d526b6cf → xxh:0d6bf068; amount: 20 → 25;
```

Equal values give equal tokens, so rows stay correlatable across a report.
This is not anonymization: the token is a plain hash of the value, so a
low-cardinality column can be recovered by hashing the candidates. Use it to
keep values out of a report, not to make a report safe to publish.

## HTML report

`--report out.html` writes one self-contained page, with inline CSS and JS
and no external requests, holding the verdict, counts, schema changes,
per-column match-rate bars and sortable example tables. It follows the
reader's light/dark preference and opens straight out of a CI artifact zip or
an email attachment. Any other extension writes the markdown report, and
`--report` is repeatable, so one run can produce both:

```bash
tdiff a.parquet b.parquet --key id --report summary.md --report report.html
```

## Loosening the comparison

Four flags change what counts as equal. Three are *normalizations*, a
deterministic canonicalization of each value, so they apply everywhere:
inside the join, in `--summary`, and in snapshots.

| Flag | Effect |
|---|---|
| `--float-precision <n>` | round floats to n decimal digits |
| `--ignore-case` | fold strings to lower case (Unicode) |
| `--trim` | strip leading and trailing whitespace from strings |
| `--timestamp-precision s\|ms\|us` | truncate timestamps to that grid |

These also apply to key columns, so keys differing only by case or padding
still join.

`--tolerance` is different. An epsilon is not transitive, so no hash can
encode it; it never touches the join and instead reclassifies rows during
column attribution. Consequences:

- It needs full diff mode. `--tolerance` with `--summary` or `--against` is
  refused, pointing at `--float-precision` instead.
- Tolerable rows are reported as **within tolerance**, not as unchanged, so
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

## Per-column statistics

Every full diff reports, per changed column, how many row pairs differ, the
share that match, and for numeric columns the largest and mean absolute
difference. They appear in `human` output, as `column_stats` in `--format
json`, and as a table in the markdown report.
