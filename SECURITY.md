# Security

tdiff parses untrusted binary and text input: parquet files, Iceberg and Delta
metadata, deletion vectors, and CSV/NDJSON from wherever your pipeline got
them. Parser bugs reachable from a crafted file are in scope.

## Reporting

Report privately through GitHub's
[security advisories](https://github.com/KonMam/tdiff/security/advisories/new)
rather than a public issue. Include the input that triggers it, or a script
that generates it, and what you observed.

## In scope

- Memory-unsafe behaviour or an unrecovered panic from a malformed input file
- Unbounded memory or CPU from a small crafted input
- Path traversal out of a table directory via metadata-recorded paths
- Reading or writing outside the paths given on the command line

## Not in scope

- A clean error message on a corrupt file. That is the intended behaviour;
  the parse boundary converts parser panics into errors on purpose.
- Resource use proportional to input size. Diffing a large table uses memory
  and CPU; `--mode stream` bounds the memory.
- Anything requiring the attacker to already control the command line.

## What is already hardened

The parquet parse boundary converts panics into errors, fuzz targets cover the
thrift page-header walk, RLE levels, delta-binary-packed decoding, plain byte
arrays, the CSV block splitter, type inference, z85 and deletion-vector
decoding, and their regression corpus runs on every `go test`. The lake layer
bounds-checks deletion-vector offsets, validates roaring bitmaps before
iteration, and caps DV position counts by the descriptor's cardinality.
`internal/torture` feeds the real binary seeded corruption and asserts clean
errors, no crashes or hangs, and bounded memory.

Supported versions: the latest release.
