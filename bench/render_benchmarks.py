#!/usr/bin/env python3
"""Render bench/results/*.json into BENCHMARKS.md."""
import json
import subprocess
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"

CASE_ORDER = [
    "parquet-1m-identical", "parquet-1m-0.1pct", "parquet-1m-1pct", "parquet-1m-10pct",
    "parquet-10m-identical", "parquet-10m-1pct",
    "csv-1m-1pct", "csv-10m-1pct",
    "crossformat-10m-1pct",
    "wide-100col-1m-1pct", "stringy-1m-1pct",
    "ndjson-10m-1pct", "csvgz-10m-1pct", "dictparquet-10m-1pct",
    "export-10m-1pct",
    "parquet-100m-1pct",
]

TOOL_ORDER = ["venn", "venn-summary", "venn-export", "duckdb-full", "duckdb-counts",
              "duckdb-export", "datacompy-polars", "datacompy-pandas", "pandas-naive", "csvdiff"]

CASE_DESC = {
    "parquet-1m-identical": "parquet, 1M rows × 15 cols, identical content (CI hot path)",
    "parquet-1m-0.1pct": "parquet, 1M rows, 0.1% changed +0.05% added/removed",
    "parquet-1m-1pct": "parquet, 1M rows, 1% changed +0.5% added/removed",
    "parquet-1m-10pct": "parquet, 1M rows, 10% changed",
    "parquet-10m-identical": "parquet, 10M rows × 15 cols, identical content",
    "parquet-10m-1pct": "parquet, 10M rows, 1% changed",
    "csv-1m-1pct": "CSV, 1M rows, 1% changed",
    "csv-10m-1pct": "CSV, 10M rows (1.7 GB/side), 1% changed",
    "crossformat-10m-1pct": "cross-format: parquet vs CSV, 10M rows, 1% changed",
    "wide-100col-1m-1pct": "parquet, 1M rows × 100 cols, 1% changed",
    "stringy-1m-1pct": "parquet, 1M rows, high-cardinality strings, 1% changed",
    "parquet-100m-1pct": "parquet, 100M rows × 15 cols (8.3 GB/side), 1% changed; venn streams",
    "ndjson-10m-1pct": "NDJSON, 10M rows (2.6 GB/side), 1% changed",
    "csvgz-10m-1pct": "gzipped CSV, 10M rows (830 MB/side compressed), 1% changed",
    "dictparquet-10m-1pct": "pyarrow-written parquet (dictionary encoding, v1 pages), 10M rows, 1% changed",
    "export-10m-1pct": "export the differing rows as CSV (~200K rows), 10M-row inputs",
}


def sh(cmd: str) -> str:
    return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()


def machine() -> str:
    chip = sh("sysctl -n machdep.cpu.brand_string")
    mem = int(sh("sysctl -n hw.memsize")) // (1 << 30)
    cores = sh("sysctl -n hw.ncpu")
    osv = sh("sw_vers -productVersion")
    return f"{chip}, {cores} cores, {mem} GB RAM, macOS {osv}, AC power, quiesced"


def fmt_time(t: dict) -> str:
    if "mean_s" in t:
        return f"{t['mean_s']:.2f} ± {t['stddev_s']:.2f}"
    return {"N/A": "n/a", "WRONG": "**wrong output**", "DNF": "DNF",
            "ERROR": "error"}.get(t.get("status", "?"), "?")


def fmt_mem(t: dict) -> str:
    return f"{t['peak_rss_mb']:,}" if "peak_rss_mb" in t else ""


def main() -> None:
    cases = {}
    for f in RESULTS.glob("*.json"):
        if f.name.endswith(".hyperfine.json"):
            continue
        d = json.loads(f.read_text())
        cases[d["case"]] = d

    lines = []
    w = lines.append
    w("# venn benchmarks")
    w("")
    w(f"_Run {date.today().isoformat()} on: {machine()}._")
    w("")
    w("Everything here is reproducible: `bench/gen` generates the datasets with a")
    w("ground-truth manifest, `bench/run_bench.py` runs the correctness gate,")
    w("hyperfine timing, and peak-RSS measurement, `bench/render_benchmarks.py`")
    w("writes this file. Versions are pinned in `bench/versions.lock`.")
    w("")
    w("## Method")
    w("")
    w("- **Correctness gate before timing**: every tool's added/removed/changed")
    w("  counts must match the fixture manifest exactly, or it is disqualified")
    w("  from that chart (marked 'wrong output'), venn included.")
    w("- **Timing**: hyperfine, warm cache, ≥10 runs for fast tools (≥3 with")
    w("  warmup for runs over ~20 s). End-to-end wall time including process and")
    w("  interpreter startup, which is the workflow being compared. For the")
    w("  Python tools the table also lists compute-only time (after imports),")
    w("  so nothing hides behind interpreter startup.")
    w("- **Memory**: peak RSS via `/usr/bin/time -l`, single run.")
    w("- **Fairness**: DuckDB runs its native readers with default (all-core)")
    w("  threading, reading the same files, with the SQL a practitioner would")
    w("  write (generated per schema by `bench/competitors/gen_duckdb_sql.py`).")
    w("  Two output tiers are compared at equal work: `duckdb-counts` (added/")
    w("  removed/changed counts) pairs with `venn --summary`; `duckdb-full`")
    w("  (adds per-column changed counts) pairs with plain `venn`, whose")
    w("  default output also includes per-column attribution and example rows.")
    w("- **Excluded**: `bdt` (v0.18.0 fails to build from crates.io on this")
    w("  toolchain); `data-diff` (archived March 2024; requires live database")
    w("  connections even for local diffs, the workflow venn replaces);")
    w("  browser/WASM tools (not scriptable); Spark (cluster-class, unfair in")
    w("  both directions). `csvdiff` appears only in CSV cases (CSV-only tool).")
    w("- **Row order**: the right-hand file is written in a different physical")
    w("  row order than the left, as reconciliation inputs usually are. Tools")
    w("  being compared all do keyed diffs, so this is fair game.")
    w("")
    w("Lower is better everywhere. *wrong output* = failed the correctness gate;")
    w("n/a = tool cannot run the case.")
    w("")

    for case in CASE_ORDER:
        if case not in cases:
            continue
        d = cases[case]
        w(f"## {CASE_DESC.get(case, case)}")
        w("")
        w("| tool | wall time (s) | peak RSS (MB) | compute-only (s) |")
        w("|---|---:|---:|---:|")
        for tool in TOOL_ORDER:
            t = d["tools"].get(tool)
            if t is None or t.get("status") == "N/A":
                continue
            compute = f"{t['compute_s']:.2f}" if "compute_s" in t else ""
            w(f"| {tool} | {fmt_time(t)} | {fmt_mem(t)} | {compute} |")
        w("")

    (ROOT / "BENCHMARKS.md").write_text("\n".join(lines) + "\n")
    print("wrote BENCHMARKS.md")


if __name__ == "__main__":
    main()
