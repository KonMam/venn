# Contributing

Bug reports are the most useful thing you can send. A diff that reports the
wrong counts is the worst class of bug this tool can have, so a reproducer
matters more than a diagnosis.

## Reporting a bug

Include the `venn --version`, the command you ran, and what you expected
against what you got. If the inputs are not shareable, a generated pair that
reproduces it is just as good:

```bash
go run ./bench/gen --rows 100000 --out /tmp/repro --formats parquet,csv
```

Wrong counts, a crash, a hang, or a confusing error on valid input are all
bugs. So is a clean error that should have been a successful diff.

## Building and testing

Needs Go 1.26 or newer (see the floor in `go.mod`).

```bash
go build ./cmd/venn
go test ./...              # ground-truth oracle, interop corpus, table fixtures, torture
go test -race ./internal/...
golangci-lint run ./...
gofmt -l cmd internal pkg bench   # must print nothing
```

`go test ./...` is self-contained: it generates its fixtures into temp
directories and checks them against the planted ground truth. Nothing in the
suite depends on the large fixtures under `testdata/10k/`, `1m/`, `10m/`,
`100m/` or `1b/`, which are gitignored and only used by the benchmarks.

## What CI will check

Everything above, on Linux, macOS and Windows, plus:

- the same suite on the latest stable Go, so a new release cannot break users
  before it breaks us
- a fuzz smoke run over every target (the regression corpus in
  `internal/source/testdata/fuzz` always runs as part of `go test`)
- a six-target cross-build and a goreleaser snapshot
- a self-test of the composite GitHub Action, asserting exact counts
- **a performance gate.** On a pull request, `bench/perf` runs the smoke-tier
  case matrix interleaved A/B against your base branch on the same runner and
  compares CPU time and peak RSS. It is correctness-gated: a wrong answer
  fails the job before anything is timed. See [bench/README.md](bench/README.md).

Run the perf gate yourself before pushing a change to the engine:

```bash
go run ./bench/perf -tier smoke -against main -check
```

The first run generates fixtures and is slow; they are cached afterwards.

## Changes that need a test

- Anything touching comparison semantics, key handling, or the counts. Add a
  case to the manifest-driven suite so the ground truth covers it.
- A new format or encoding. Add a file to the interop corpus
  (`testdata/interop/gen_corpus.py`) so it has to diff as identical against
  the canonical CSV.
- A parser fix from malformed input. Add the input to the fuzz corpus or the
  torture matrix, whichever found it.

## Style

Match the surrounding code. Two things that are not obvious from reading it:

- Comments explain *why*, not what. A comment that restates the code is worse
  than no comment. The ones worth writing are the ones recording a decision
  someone would otherwise undo.
- No planning artifacts in the repo: roadmaps, dated work logs, market notes
  and benchmark scratch output do not belong in version control. The reasoning
  belongs in the code comment or the commit message.

Commit messages: a short imperative subject, then a body explaining what
changed and why, wrapped at 76 columns. The existing log is the reference.

## Scope

venn answers one question, which rows differ. The README's
[What it's for](README.md#what-its-for) section lists what is deliberately
out of scope, and a PR adding one of those is likely to be declined however
good it is. If you are unsure whether something fits, open an issue before
writing it.
