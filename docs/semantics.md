# Comparison semantics

What tdiff treats as equal, and where it refuses to guess.

## Columns

Column matching is by name. Column order and physical type may differ:
a parquet INT64 column and a CSV column inferred as float64 both compare in
the float domain, so equal logical values match across formats.

Columns whose logical types are incomparable, such as a string against a
timestamp, are reported in the schema diff and excluded from the row diff.
Nested parquet and JSON columns are skipped with a warning.

`--rename` declares a right-side column equivalent to a left-side one, which
makes it a compared column instead of an added/removed pair.

## Values

- int64 and float64 columns compare numerically.
- Timestamps compare at microsecond precision in UTC, unless
  `--timestamp-precision` coarsens it.
- `-0` equals `0`.
- `NaN` equals `NaN`. Identical inputs have to diff as identical, which
  matters more here than IEEE 754 does.
- Table partition values compare as strings.
- DECIMAL columns are int32/int64-backed and compare in the float domain.

CSV cannot represent NULL for string columns, so `""` is an empty string.
Empty numeric and timestamp fields read as NULL.

## Keys

A keyed diff needs unique keys. With a duplicate key, "which right row
corresponds to this left row?" has no answer, so the default is to fail with
the offending key rather than pick one. `--on-dup warn` keeps the first
occurrence per side; `--on-dup match` pairs a key's rows as multisets;
`--keyless` drops the key and matches whole rows as a multiset. See
[usage.md](usage.md#duplicate-keys-and-no-key-at-all).

When `--key` is omitted the key is inferred: both inputs are sampled, and a
column unique in the sample on both sides is chosen, preferring id-ish names
and integer or string types. The choice is verified for real during the build
pass, so a duplicate fails with a message naming the inferred key.

## Normalization versus tolerance

A normalization is a deterministic function of one value, so both sides map
equal logical values onto identical bits. That makes it hash-consistent, and
hash consistency is what lets it apply inside the join, in `--summary`, and
in snapshots. `--float-precision`, `--ignore-case`, `--trim` and
`--timestamp-precision` all qualify.

An epsilon tolerance does not: it is not transitive, so no hash can encode
it. `--tolerance` therefore never touches the join and instead reclassifies
rows during column attribution, which is why it needs full diff mode and
reports its rows as "within tolerance" rather than as unchanged.

## Lake tables

Iceberg sequence-number rules are honored: position deletes apply to data
files at or before the delete's sequence, and equality deletes apply strictly
before, so an upsert's re-added row survives its own delete.

Same-table snapshot diffs skip data files that are live in both snapshots. In
a copy-on-write table each live row sits in exactly one data file, so a
shared file contributes identical rows to both sides and cancels exactly.
Skipped files' row counts, from the Iceberg manifest's `record_count` or
Delta's `stats.numRecords`, fold back into the totals. Files whose delete
state differs between the snapshots are never skipped, and neither are files
carrying equality deletes, whose live count is not knowable from metadata
alone. Pruning is also disabled under `--where`, since it would fold in row
counts the filter would have reduced.

Lake-table data files must be parquet, which is what engines write.

## Interop

Parquet reading is tested against files written by pyarrow, DuckDB and
polars, covering dictionary encoding, data pages v1 and v2,
snappy/gzip/zstd/uncompressed, INT96 timestamps, int-backed DECIMAL, delta
encodings and RLE booleans. Each exotic file has to diff as identical against
a canonical CSV of the same logical data, which checks both readers at once.

Lake-table support is tested against real pyiceberg and delta-rs tables.
Merge-on-read delete files are built to the specs and cross-validated by
independent readers: pyiceberg reads the position-delete table and DuckDB's
`delta_scan` reads the deletion-vector table.
