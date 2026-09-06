# Flag reference

Every flag, grouped by what it affects. For worked examples of the
features these drive, see [usage.md](usage.md); for what counts as equal,
see [semantics.md](semantics.md).

```
tdiff <left> <right> [--key <col>[,<col>...]] [flags]  row + schema diff
tdiff schema <left> <right>                            schema diff only
tdiff snapshot <file> --key <col> --output <b.snap>    save a hash baseline
tdiff <file> --against <b.snap>                        diff against a baseline
```

Flags may appear before or after the positional arguments. Exit codes are
`0` for identical (or within budget), `1` for differences, and `2` for an
error.

## Selecting rows and columns

| Flag | Effect |
|---|---|
| `--key <cols>` | key column(s), comma-separated. Omitted, the key is inferred: a column unique in both inputs, preferring id-ish names |
| `--keyless` | no key; match whole rows as a multiset |
| `--where <predicate>` | keep only matching rows, on both sides. Repeatable and ANDed |
| `--ignore-columns <cols>` | exclude these columns from the comparison |
| `--rename <right>=<left>` | compare a right-side column under a left-side name. Repeatable |

## Output

| Flag | Effect |
|---|---|
| `--format <fmt>` | `human` (default), `json`, or `markdown` |
| `--report <file>` | also write a report. Repeatable; `.html` writes a self-contained page, any other extension writes markdown |
| `--limit <n>` | max example rows per category (default 10) |
| `--verbose` | print example rows |
| `--summary` | counts and exit code only. The fastest mode: skips column attribution and examples |
| `--output <file>` | write the differing rows as data to a `.csv` or `.parquet` file |
| `--mask <cols>` | show these columns' values as a stable token |

## Comparison

| Flag | Effect |
|---|---|
| `--float-precision <n>` | round float comparisons to n decimal digits |
| `--tolerance <spec>` | treat numeric differences this small as equal. Repeatable |
| `--ignore-case` | compare strings case-insensitively |
| `--trim` | ignore leading and trailing whitespace in strings |
| `--timestamp-precision <p>` | compare timestamps at `s`, `ms` or `us` precision |

## Input handling

| Flag | Effect |
|---|---|
| `--infer-rows <n>` | CSV/NDJSON type-inference sample (default 1000; `-1` reads the whole file) |
| `--delimiter <char>` | delimited-text field separator (default `,` for `.csv`, tab for `.tsv`) |
| `--on-dup <mode>` | duplicate keys: `error` (default), `warn` (keep the first occurrence per side), or `match` (multiset pairing) |

## Execution

| Flag | Effect |
|---|---|
| `--mode auto\|memory\|stream` | join strategy. `stream` is a grace hash join that spills hashes to disk and keeps peak memory flat for larger-than-RAM inputs. `auto` picks it above 40M rows |
| `--tmpdir <dir>` | spill directory for stream mode (default: system temp) |
| `--max-diff <n\|p%>` | CI gate: exit 0 while total differing rows stay within budget |
| `--against <file>` | diff a single file against a snapshot baseline |
| `--version` | print version |

`--cpuprofile` and `--memprofile` write pprof profiles and exist for
development.

