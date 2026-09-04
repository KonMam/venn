# Changelog

## v0.5.0-dev (unreleased)

Hardening release: every finding from a full code review, fixed — plus the
comparison-semantics arc (Phase A).

### Matching semantics

- **New: `--rename <right>=<left>`** (repeatable) compares a renamed column
  instead of reporting it as one added and one removed column. It applies
  before everything else, so the renamed column can be the key, can be
  ignored, and appears under its left-side name in exports and reports; key
  inference and `--against` see it too. A rename is reported in every format
  but does not make the schemas differ. Both names are validated — a typo is
  an error, not a silently unmatched column.

- **New: `--on-dup match`** pairs a duplicated key's rows as multisets:
  identical rows cancel as unchanged, leftovers are added/removed, and
  nothing inside a duplicate group is reported as changed (which of three
  left rows "became" which of three right rows is genuinely ambiguous). The
  one refinement is that leftovers of exactly one row per side keep the
  "changed" label, so ordinary changed rows keep their column attribution.
  The rule reads leftover *counts*, never arrival order, so the answer is
  identical run to run at any thread count — unlike datacompy's
  rank-within-group pairing, which needs a deterministic row order that a
  parallel scan does not have. Checked against a naive reference
  implementation over 40 random duplicate-heavy datasets.
- **New: `--keyless`** drops the key and matches whole rows as a multiset;
  `changed` is then 0 by construction and a rewritten row is one removed plus
  one added. Suggested by the error when key inference fails.
- Both duplicate modes need the in-memory join: streaming's attribution pass
  identifies rows by key hash, which cannot pick out *which* rows of a
  duplicate group were the leftovers. `--mode stream` with either is refused
  with that reason rather than answered wrongly — streaming support is
  follow-up work.

### Comparison semantics

- **New: value normalizations** — `--ignore-case`, `--trim`,
  `--timestamp-precision s|ms|us`. Each is a deterministic canonicalization
  of a single value, so it is hash-consistent and applies everywhere:
  inside the join (key columns included, so keys differing only by case or
  padding still match), in `--summary`, and in snapshots. `--float-precision`
  is now one member of that set rather than a special case.
- **New: `--tolerance`** — a numeric epsilon, repeatable, global
  (`--tolerance 0.01`, `--tolerance 0.01,rel=1e-6`, `--tolerance rel=1e-6`)
  or per column (`--tolerance price=0.01`). An epsilon is not transitive, so
  it cannot be hashed: it never touches the join and instead reclassifies
  rows in the attribution pass. Consequences, all enforced: tolerable rows
  are reported as **within tolerance** rather than folded into unchanged
  (the counts never claim two different values were equal), they produce no
  examples and no exported rows, and `--tolerance` with `--summary`,
  `snapshot` or `--against` is refused with a pointer to
  `--float-precision`. Applies to int64 and float columns; naming a
  non-numeric or uncompared column is an error, not a silently ignored flag.
- **New: per-column statistics** in every full diff — changed-row count,
  match rate, and for numeric columns the max and mean absolute difference.
  Rendered in `human` output, as `column_stats` in `--format json`, and as a
  table in the markdown report.
- **New: `--delimiter`** for delimited text (`;`, `|`, `\t`, `tab`),
  overriding the extension's default.
- Snapshot headers now record every hash-affecting setting, and a baseline
  read under a different normalization is refused instead of silently
  comparing different values.
- **Fixed: `--infer-rows -1` (whole-file inference) was ignored for remote
  inputs** — the normalized sample size was re-wrapped into fresh Options and
  re-defaulted to 1000. Source options now thread through the remote path
  intact.
- Cost: the disabled path is unchanged (measured within noise, ≤2% CPU on the
  1M-row smoke matrix). Enabled, on 1M×15 parquet: `--trim` +3%,
  `--ignore-case` +4%, `--timestamp-precision` +6%, `--tolerance` ~0%. On a
  string-dominated shape `--ignore-case` costs ~40% (it folds every value);
  `--float-precision` remains the free alternative where it fits.
- `pkg/tdiff` and the GitHub Action expose all of the above; the fixture
  generator can plant rows that differ only within tolerance
  (`--tolerable`), and the manifest records them as the oracle for
  `--tolerance`.

### Workflow

- **New: `--where`** filters both inputs before the diff runs — `col OP
  literal` or `col IS [NOT] NULL`, ANDed by repeating the flag or writing
  `and` between clauses, with literals typed against the column (dates and
  timestamps parse, booleans parse, strings may be quoted). Two layers: a
  wrapping source that filters decoded batches, so it works for every format
  and layout; and partition pruning, so a predicate on a hive/Iceberg/Delta
  partition column skips whole data files without opening them (correctness
  never depends on the second layer). An unknown column or an unparseable
  literal is an error, never a filter that quietly keeps everything. Every
  report states the filter, because the counts are then of the filtered
  universe; a snapshot records it, so a filtered baseline cannot be compared
  against a differently-filtered file; and the shared-file shortcut for
  same-table snapshot diffs is disabled under a filter, since it folds in row
  counts the filter would have reduced.

- **`--max-diff` now exits early.** Added and changed rows alone settle an
  over-budget verdict, so the run stops there instead of finishing the scan
  (1M rows at 10% difference with a small budget: 0.06 s instead of 0.63 s).
  The counts are then marked as lower bounds in every format, never passed
  off as totals. Two cases still run to completion by design: a percentage
  budget whose denominator is not yet known (CSV/NDJSON carry no up-front row
  count, so the budget could still grow — parquet and lake tables abort
  exactly), and `--output`, where half an export file would be worse than a
  slower run. Streaming mode stops after the join instead, skipping the
  attribution pass with the counts still exact.
- **New: `--mask <cols>`** replaces those columns' values with a stable short
  token (`xxh:9f3a1c22`) in examples, keys, every report format and
  `--output`, while the comparison keeps using the real values. Masked
  columns export as strings. Documented for what it is: a way to keep values
  out of a report, not anonymization — the token is a plain hash, so a
  low-cardinality column can be recovered by hashing candidates.
- **New: `--report out.html`** writes a self-contained HTML report — inline
  CSS/JS, zero external requests, light/dark aware — with the verdict,
  counts, schema changes, per-column match-rate bars and sortable example
  tables. `--report` now dispatches on the extension (anything but
  `.html`/`.htm` stays markdown) and is repeatable, so one run can write both
  a markdown step summary and an HTML artifact.

### Hardening

- **Fixed: two corrupt-parquet inputs reached a runtime panic** that the
  reader's recover boundary then laundered into `corrupt parquet data:
  runtime error: …`. Both are now clean errors, pinned by regression tests
  (the 25-seed torture soak is what surfaced them):
  - a definition-level RLE run length near 2^63 made the `row+run` clamp
    overflow, so the fill loop walked past the output slice. The guard now
    rejects an over-declared run before the int conversion, the way
    `decodeRLEHybrid32` already did — the hardening pass fixed that shape in
    one decoder and missed its sibling. Bit-packed group counts are bounded
    by the bytes present for the same reason.
  - `DELTA_LENGTH_BYTE_ARRAY` lengths come from a *signed* encoding, so a
    corrupt page could declare a negative string length. `off+ln` then passed
    the overrun check (it moves *backwards* inside the page) and reached
    `unsafe.String` with a negative length.
- CI's fuzz smoke job budgets in executions (`-fuzztime 20000x`) rather than
  seconds: a wall-clock budget makes the fuzzing coordinator race its own
  shutdown deadline on a slow runner and fail with "context deadline
  exceeded" and no crasher, which is a flake, not a finding.


- **Fixed: parallel CSV/NDJSON scans crashed on lines longer than 4 KB that
  straddled a 1 MB block boundary** (buffer-capacity overrun in the block
  feeders; the refill step is now shared and grows the buffer). Regression
  tests pin it.
- **Fixed: a corrupt Delta checkpoint was silently treated as end-of-file**,
  yielding a partial live-file set and wrong counts; it now fails the diff.
- **Fixed: `--against` could deadlock on a snapshot containing duplicate
  keys** (workers died while the loader still fed them); it errors promptly.
- Untrusted-input hardening in the lake layer: deletion-vector offsets are
  bounds-checked, roaring bitmaps are validated before iteration (a fuzzing
  find — malformed run containers panicked), DV position counts are capped
  by the descriptor's cardinality before materializing, short `_delta_log`
  names no longer panic, and Delta data paths are URL-decoded per the
  protocol. New fuzz targets cover z85 and DV decoding.
- Truncated snapshots and corrupt row spills now error instead of silently
  dropping trailing bytes; remote readers return `io.EOF` on out-of-range
  reads; dates like `2023-02-30` no longer parse.
- `--help` exits 0; `--version` documented; release builds get the real
  version string (ldflags now land on a `var`).
- `pkg/tdiff` is a real public API: self-contained types, no internal
  leakage; module path is now `github.com/KonMam/tdiff`.
- Test coverage for the previously untested output writers (CSV/parquet
  export, markdown, human) and schema comparison; golangci-lint is clean
  and enforced in CI.
- **New: perf-regression + torture suite** (`bench/PERF.md`). `bench/perf`
  measures a deterministic workload matrix — every command × format × shape ×
  diff density, correctness-gated before timing — and gates PRs by running
  the current tree interleaved A/B against the base branch, comparing CPU
  time and peak RSS. `internal/torture` feeds the real binary seeded
  corruption (bit flips, truncation ladders, injected newlines, broken
  quotes, corrupt snapshots, degenerate files) and asserts clean errors, no
  crashes/hangs, bounded memory, and valid JSON on claimed success; it runs
  in plain `go test` on all CI platforms, with a 25-seed soak job.

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
- **Snapshots**: `tdiff snapshot` saves a 16-byte-per-row hash baseline;
  `--against` diffs a live file without the original data.
- **Embedding**: `pkg/tdiff` public API.

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
