# Development and benchmark entry points. The benchmark targets exist so the
# numbers in README.md can be reproduced from a clean checkout rather than
# taken on trust.
#
# Needs: go, python3, duckdb, hyperfine. Fixtures are generated from a seed
# and the generator is byte-deterministic, so at a given commit a tier
# produces the same inputs everywhere. Across commits the row values stay
# fixed but parquet framing can shift, which is why every tool is gated on
# the fixture's own manifest rather than on a recorded file hash.

GEN := go run ./bench/gen
TIER ?= small

.PHONY: help build test race lint bench bench-data bench-run bench-clean \
        bench-data-small bench-data-standard bench-data-large

help:
	@echo "build              build ./venn"
	@echo "test               full suite: oracle, interop, tables, torture"
	@echo "race               go test -race ./internal/..."
	@echo "lint               golangci-lint run ./..."
	@echo
	@echo "bench              run the comparison (TIER=small), write BENCHMARKS.md"
	@echo "  TIER=small       1M rows, 7 cases, about 1.5 GB, a few minutes"
	@echo "  TIER=standard    10M rows, adds 7 cases, about 25 GB"
	@echo "  TIER=large       100M rows, adds 1 case, about 17 GB"
	@echo "                   README's numbers are the standard and large tiers."
	@echo "                   At 1M rows the two tools are close on parquet; the"
	@echo "                   gap is in memory, and in wall time at scale."
	@echo "bench-data         generate the fixtures for TIER, then stop"
	@echo "bench-clean        delete fixtures, raw results and generated SQL"

build:
	go build -o venn ./cmd/venn

test:
	go test ./...

race:
	go test -race ./internal/...

lint:
	golangci-lint run ./...

# ---- benchmark ----------------------------------------------------------
#
# Every dataset is (seed, shape, density). The seeds are the ones the
# published numbers were measured with, so changing one invalidates the
# comparison rather than improving it. run_bench.py gates each tool on the
# fixture's manifest before it is allowed into the timing chart, so a tool
# that answers wrong cannot win on speed.

CASES_small    := parquet-1m-identical parquet-1m-0.1pct parquet-1m-1pct \
                  parquet-1m-10pct csv-1m-1pct wide-100col-1m-1pct stringy-1m-1pct
CASES_standard := $(CASES_small) parquet-10m-identical parquet-10m-1pct \
                  csv-10m-1pct crossformat-10m-1pct ndjson-10m-1pct \
                  csvgz-10m-1pct export-10m-1pct
CASES_large    := $(CASES_standard) parquet-100m-1pct

bench-data-small:
	@test -f bench/data/1m-d0/manifest.json     || $(GEN) --out bench/data/1m-d0     --rows 1000000  --seed 11 --changed 0     --added 0      --removed 0
	@test -f bench/data/1m-d01/manifest.json    || $(GEN) --out bench/data/1m-d01    --rows 1000000  --seed 12 --changed 0.001 --added 0.0005 --removed 0.0005
	@test -f bench/data/1m-d1/manifest.json     || $(GEN) --out bench/data/1m-d1     --rows 1000000  --seed 1  --changed 0.01  --added 0.005  --removed 0.005
	@test -f bench/data/1m-d10/manifest.json    || $(GEN) --out bench/data/1m-d10    --rows 1000000  --seed 14 --changed 0.10  --added 0.005  --removed 0.005
	@test -f bench/data/wide-1m/manifest.json   || $(GEN) --out bench/data/wide-1m   --rows 1000000  --seed 16 --changed 0.01  --added 0.005  --removed 0.005 --cols 100 --variant wide
	@test -f bench/data/stringy-1m/manifest.json || $(GEN) --out bench/data/stringy-1m --rows 1000000 --seed 17 --changed 0.01 --added 0.005 --removed 0.005 --variant stringy

# testdata/10m carries the extra formats (ndjson and the compressed CSVs) that
# the format cases need; bench/data/10m-d1 is the same data as parquet and csv
# only, kept separate so the density cases share one shape.
bench-data-standard: bench-data-small
	@test -f bench/data/10m-d0/manifest.json || $(GEN) --out bench/data/10m-d0 --rows 10000000 --seed 15 --changed 0    --added 0     --removed 0
	@test -f bench/data/10m-d1/manifest.json || $(GEN) --out bench/data/10m-d1 --rows 10000000 --seed 2  --changed 0.01 --added 0.005 --removed 0.005
	@test -f testdata/10m/manifest.json      || $(GEN) --out testdata/10m      --rows 10000000 --seed 2  --changed 0.01 --added 0.005 --removed 0.005 --formats parquet,csv,ndjson,csv.gz,csv.zst

bench-data-large: bench-data-standard
	@test -f testdata/100m/manifest.json || $(GEN) --out testdata/100m --rows 100000000 --seed 3 --changed 0.01 --added 0.005 --removed 0.005 --formats parquet

bench-data: bench-data-$(TIER)

bench-run:
	python3 bench/run_bench.py $(CASES_$(TIER))
	python3 bench/render_benchmarks.py

bench: bench-data bench-run

bench-clean:
	rm -rf bench/data bench/results bench/sql testdata/10m testdata/100m
