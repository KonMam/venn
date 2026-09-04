# Changelog

## v0.4.0-dev (unreleased)

Lake tables as first-class citizens.

- **Multi-file datasets**: directories and globs diff as one dataset —
  union-by-name schemas, hive partition directories become columns,
  file-level parallelism.
- **Iceberg & Delta tables**: point at a table root (local or `s3://`),
  pick snapshots/versions with `#<id>`; auto-detected. Metadata, manifest
  lists, manifests, checkpoints and log replay are read natively.
- **Merge-on-read**: Iceberg position and equality deletes (v2
  sequence-number rules) and Delta deletion vectors filter rows during the
  scan. Fixtures are spec-built and cross-validated with pyiceberg/DuckDB.
- **Shared-file skipping**: same-table snapshot diffs skip data files live
  in both snapshots (identical delete state required); skipped rows fold
  back into the counts. Small-churn diffs of huge tables scan only the churn.
- **CI packaging**: `--format markdown`, `--report file.md`, and a composite
  GitHub Action (step-summary report, count outputs, `max-diff` budget).

## v0.3.0-dev (unreleased)

The "professional tool" release: interop, hardening, and workflow features.

- **Interop**: parquet written by pyarrow / DuckDB / polars reads correctly
  and fast — dictionary encoding decoded in the native kernels (dictionary
  entries hashed once), data page v1+v2, snappy/gzip/zstd/uncompressed,
  INT96 timestamps, int-backed DECIMAL, delta encodings, RLE booleans.
  Nested columns are skipped with a warning. A corpus of foreign-written
  files gates every build.
- **Hardening**: native fuzz targets for every untrusted-input decoder; the
  parquet parse boundary converts corrupt-file panics (including inside
  parquet-go) into clean errors. CI matrix (linux/mac/windows), fuzz smoke,
  6-target cross-build, goreleaser snapshot.
- **Dirty data**: CSV/NDJSON values contradicting the inferred type demote
  the column to string and restart automatically; `--infer-rows` tunes the
  sample. `--on-dup warn` keeps first occurrences and reports duplicates.
- **Workflow**: `--output diff.csv|diff.parquet` (the differing rows as
  typed data), `--max-diff N|P%` CI budgets, `--float-precision` (quantized,
  hash-consistent float comparison), key auto-inference, TTY progress.
- **Sources**: NDJSON/JSONL (zero-copy tokenizer, block-parallel), gzip/zstd
  CSV, `http(s)://` and `s3://` inputs.
- **Snapshots**: `venn snapshot` saves a 16-byte-per-row hash baseline;
  `--against` diffs a live file without the original data.
- **Embedding**: `pkg/venn` public API.

## v0.2.0-dev

- Streaming grace-hash-join mode: 100M-row diffs on a laptop in <1 GB RSS
  (auto-selected above 40M rows). Concurrent side scans, spill-based
  partition join, changed-row payload spill for attribution.
- Custom parquet decode kernels: windowed chunk reads, own page-header walk,
  zero-copy PLAIN column aliasing, delta string lengths, RLE levels.
- Column-major typed batch engine; `--summary` mode; `--on-dup`; GC tuning.

## v0.1.0-dev

- Format-agnostic keyed diff (parquet, CSV/TSV, any combination), schema
  diff, hash-join engine, fixture generator with ground-truth manifests,
  benchmark harness with correctness gates.
