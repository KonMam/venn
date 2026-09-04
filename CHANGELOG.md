# Changelog

Notable changes, newest first. Nothing has been tagged yet, so everything
below is unreleased.

## Unreleased

### Sources

- Parquet with its own decode kernels: windowed chunk reads, own page-header
  walk, zero-copy PLAIN column aliasing, dictionary decode, delta string
  lengths, RLE levels.
- CSV and TSV, block-parallel, with hand-rolled timestamp and date parsers.
  Gzip and zstd variants stream through the same parser.
- NDJSON and JSONL, flat objects, zero-copy tokenizer, block-parallel.
- Multi-file datasets: directories, globs and `s3://` prefixes diff as one
  dataset, with union-by-name schemas and hive partition directories exposed
  as columns.
- Iceberg and Delta tables, local or on `s3://`, auto-detected from the table
  root. `#<snapshot-id>` or `#<version>` picks a point in time. Metadata,
  manifest lists, manifests, checkpoints and log replay are read natively.
- Merge-on-read: Iceberg position and equality deletes under the v2
  sequence-number rules, and Delta deletion vectors, filter rows during the
  scan.
- Same-table snapshot diffs skip data files live in both snapshots, so a
  small churn on a huge table scans only the churn. Identical delete state is
  required, and skipped rows fold back into the counts.
- Remote inputs: `http(s)://` by range request, with a fallback to full
  download when the server lacks range support, and `s3://` via the AWS SDK.
- Interop: parquet written by pyarrow, DuckDB and polars, covering dictionary
  encoding, data pages v1 and v2, snappy/gzip/zstd, INT96, int-backed
  DECIMAL, delta encodings and RLE booleans. Nested columns are skipped with
  a warning.

### Engine

- Format-agnostic keyed hash join over a column-major typed batch engine.
  Sources deliver rows concurrently and the join table is striped 64 ways.
- Streaming grace-hash-join mode for larger-than-RAM inputs, auto-selected
  above 40M rows: 128 to 1024 adaptive partitions, concurrent side scans,
  spill-based partition join, changed-row payload spill for attribution.
  1B rows by 5 columns in 57 s at 1.08 GB peak RSS.
- `--summary` skips column attribution and examples for one scan instead of
  two.
- `--mode` forces the join strategy; `--tmpdir` places the spill.

### Comparison

- Value normalizations, all hash-consistent and so applied inside the join,
  in `--summary` and in snapshots: `--float-precision`, `--ignore-case`,
  `--trim`, `--timestamp-precision`. They apply to key columns too, so keys
  differing only by case or padding still match.
- `--tolerance`, a repeatable numeric epsilon, global or per column. An
  epsilon is not transitive, so it never touches the join and instead
  reclassifies rows during attribution. Tolerable rows are reported as within
  tolerance rather than folded into unchanged, and the flag is refused with
  `--summary`, `snapshot` and `--against`.
- `--rename <right>=<left>` compares a renamed column instead of reporting it
  as one added and one removed column. Both names are validated.
- `--on-dup match` pairs a duplicated key's rows as multisets; `--on-dup
  warn` keeps the first occurrence per side. Nothing inside a duplicate group
  is reported as changed. The rule reads leftover counts and never arrival
  order, so counts are identical run to run at any thread count.
- `--keyless` drops the key and matches whole rows as a multiset.
- Key auto-inference when `--key` is omitted, verified during the build pass.
- `--where` filters both sides before the diff. Batch-level filtering works
  for every format; partition pruning additionally skips whole files for
  predicates on hive, Iceberg or Delta partition columns.
- Per-column statistics in every full diff: changed rows, match rate, and for
  numeric columns the max and mean absolute difference.

### Output and workflow

- `--format human|json|markdown`, and `--report <file>`, repeatable and
  dispatching on extension: `.html` writes a self-contained page with inline
  CSS and JS and no external requests, anything else writes markdown.
- `--output diff.csv|diff.parquet` writes the differing rows as typed data:
  key columns, `diff_status`, and `<col>__left`/`<col>__right` pairs.
- `--max-diff N|P%` as a CI gate, exiting early once the budget is provably
  blown and marking the counts as lower bounds. A percentage budget with an
  unknown denominator and `--output` both run to completion.
- `--mask <cols>` shows a column's values as a stable token in examples,
  reports and `--output`, while the comparison keeps the real values.
- `venn snapshot` saves a 16-byte-per-row hash baseline; `--against` diffs a
  live file against it without the original data. Snapshot headers record
  every hash-affecting setting, and a mismatched baseline is refused.
- A composite GitHub Action: step-summary report, count outputs, `max-diff`
  budget.
- `pkg/venn` exposes the engine as a library.
- TTY progress on stderr.

### Robustness

- CSV and NDJSON values that contradict the inferred type demote the column
  to string and restart the diff automatically. `--infer-rows` tunes the
  sample depth.
- The parquet parse boundary converts corrupt-file panics into clean errors.
  Fuzz targets cover the thrift page-header walk, RLE levels,
  delta-binary-packed, plain byte arrays, the CSV block splitter, type
  inference, z85 and deletion-vector decoding.
- Untrusted-input hardening in the lake layer: deletion-vector offsets are
  bounds-checked, roaring bitmaps are validated before iteration, DV position
  counts are capped by the descriptor's cardinality, short `_delta_log` names
  are guarded, and Delta data paths are URL-decoded per the protocol.
- Truncated snapshots and corrupt row spills error instead of dropping bytes.
  Remote readers return `io.EOF` out of range. Dates like `2023-02-30` no
  longer parse.
- A corrupt Delta checkpoint fails the diff instead of being treated as
  end-of-file, which had yielded a partial live-file set and wrong counts.
- Fixed: parallel CSV and NDJSON scans overran the buffer on lines longer
  than 4 KB that straddled a 1 MB block boundary.
- Fixed: `--against` could deadlock on a snapshot containing duplicate keys.
- Fixed: two corrupt-parquet inputs reached a runtime panic that the recover
  boundary laundered into a confusing error. A definition-level RLE run
  length near 2^63 overflowed the `row+run` clamp, and
  `DELTA_LENGTH_BYTE_ARRAY` could declare a negative string length from its
  signed encoding.
- Fixed: `--infer-rows -1` was silently re-defaulted to 1000 for remote
  inputs.

### Testing

- Fixture generator plants a known set of diffs and writes ground truth to
  `manifest.json`; the suite checks against it for every format combination
  in both join modes.
- `bench/perf` measures a deterministic case matrix, correctness-gated before
  timing, and gates PRs by running interleaved A/B against the base branch on
  CPU time and peak RSS.
- `internal/torture` feeds the real binary seeded corruption and asserts
  clean errors, no crashes or hangs, bounded memory, and valid JSON whenever
  success is claimed.
- CI: test matrix on Linux, macOS and Windows, race detector, golangci-lint,
  fuzz smoke, six-target cross-build, goreleaser snapshot, Action self-test.
