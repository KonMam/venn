#!/usr/bin/env python3
"""venn benchmark harness.

For every case in the matrix:
  1. correctness gate — each tool runs once; its added/removed/changed counts
     must match the fixture manifest exactly or the tool is disqualified from
     that case's timing chart (reported as WRONG). Tools that cannot run a
     case at all are N/A; tools exceeding the timeout are DNF.
  2. timing — hyperfine, warm cache (see BENCHMARKS.md for methodology).
  3. memory — one /usr/bin/time -l run per tool, peak RSS.

Results land in bench/results/*.json; render_benchmarks.py turns them into
BENCHMARKS.md. Resumable: finished cases are skipped.
"""
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BENCH = ROOT / "bench"
DATA = BENCH / "data"
RESULTS = BENCH / "results"
SQLDIR = BENCH / "sql"
VENV_PY = str(BENCH / "venv" / "bin" / "python")
VENN = str(ROOT / "venn")
CSVDIFF = os.path.expanduser("~/go/bin/csvdiff")
COMP = BENCH / "competitors"

TIMEOUT = 600  # seconds, correctness-gate cap per tool run


def files(ds: str, combo: str) -> tuple[str, str]:
    d = DATA / ds
    if ds == "100m":
        d = ROOT / "testdata" / "100m"
    lf, rf = combo.split("-")
    return str(d / f"left.{lf}"), str(d / f"right.{rf}")


def manifest(ds: str) -> dict:
    d = DATA / ds
    if ds == "100m":
        d = ROOT / "testdata" / "100m"
    return json.loads((d / "manifest.json").read_text())


class Tool:
    """One competitor: how to build its command line and parse its counts."""

    def __init__(self, name, cmd_fn, parse_fn, formats=("parquet", "csv"), cross=False):
        self.name = name
        self.cmd_fn = cmd_fn
        self.parse_fn = parse_fn
        self.formats = formats
        self.cross = cross

    def runnable(self, combo: str) -> bool:
        lf, rf = combo.split("-")
        if lf != rf and not self.cross:
            return False
        return lf in self.formats and rf in self.formats


def counts_json(out: str) -> dict:
    return json.loads(out.strip().splitlines()[-1])


def venn_cmd(ds, combo):
    l, r = files(ds, combo)
    return [VENN, l, r, "--key", "id", "--format", "json"]


def venn_summary_cmd(ds, combo):
    return venn_cmd(ds, combo) + ["--summary"]


def venn_parse(out):
    d = json.loads(out)
    return d["added"], d["removed"], d["changed"]


def duckdb_sql_path(ds, combo, full) -> str:
    SQLDIR.mkdir(exist_ok=True)
    suffix = "-full" if full else ""
    p = SQLDIR / f"{ds}-{combo}{suffix}.sql"
    if not p.exists():
        l, r = files(ds, combo)
        args = [sys.executable, str(COMP / "gen_duckdb_sql.py"), l, r, "id"]
        if full:
            args.append("--full")
        sql = subprocess.run(args, capture_output=True, text=True, check=True).stdout
        p.write_text(sql)
    return str(p)


def duckdb_cmd(full):
    def fn(ds, combo):
        return ["duckdb", "-init", "/dev/null", "-batch", "-noheader", "-list",
                "-f", duckdb_sql_path(ds, combo, full)]
    return fn


def duckdb_parse(out):
    fields = out.strip().split("|")
    return int(fields[0]), int(fields[1]), int(fields[2])


def datacompy_cmd(backend):
    def fn(ds, combo):
        l, r = files(ds, combo)
        return [VENV_PY, str(COMP / "datacompy_diff.py"), backend, l, r, "id"]
    return fn


def datacompy_parse(out):
    d = counts_json(out)
    return d["added"], d["removed"], d["changed"]


def naive_cmd(ds, combo):
    l, r = files(ds, combo)
    return [VENV_PY, str(COMP / "pandas_naive.py"), l, r, "id"]


def csvdiff_cmd(ds, combo):
    l, r = files(ds, combo)
    return [CSVDIFF, l, r, "--primary-key", "0", "--format", "json"]


def csvdiff_parse(out):
    d = json.loads(out)
    return (len(d.get("Additions") or []), len(d.get("Deletions") or []),
            len(d.get("Modifications") or []))


TOOLS = [
    Tool("venn", venn_cmd, venn_parse, cross=True),
    Tool("venn-summary", venn_summary_cmd, venn_parse, cross=True),
    Tool("duckdb-counts", duckdb_cmd(False), duckdb_parse, cross=True),
    Tool("duckdb-full", duckdb_cmd(True), duckdb_parse, cross=True),
    Tool("datacompy-polars", datacompy_cmd("polars"), datacompy_parse, cross=True),
    Tool("datacompy-pandas", datacompy_cmd("pandas"), datacompy_parse, cross=True),
    Tool("pandas-naive", naive_cmd, datacompy_parse, cross=True),
    Tool("csvdiff", csvdiff_cmd, csvdiff_parse, formats=("csv",)),
]

# (case name, dataset, format combo)
CASES = [
    ("parquet-1m-identical", "1m-d0", "parquet-parquet"),
    ("parquet-1m-0.1pct", "1m-d01", "parquet-parquet"),
    ("parquet-1m-1pct", "1m-d1", "parquet-parquet"),
    ("parquet-1m-10pct", "1m-d10", "parquet-parquet"),
    ("parquet-10m-identical", "10m-d0", "parquet-parquet"),
    ("parquet-10m-1pct", "10m-d1", "parquet-parquet"),
    ("csv-1m-1pct", "1m-d1", "csv-csv"),
    ("csv-10m-1pct", "10m-d1", "csv-csv"),
    ("crossformat-10m-1pct", "10m-d1", "parquet-csv"),
    ("wide-100col-1m-1pct", "wide-1m", "parquet-parquet"),
    ("stringy-1m-1pct", "stringy-1m", "parquet-parquet"),
    # the 100M-row laptop cases: venn auto-selects streaming here; Python
    # tools are expected to OOM/DNF — that is the point of the chart
    ("parquet-100m-1pct", "100m", "parquet-parquet"),
]


def gate(tool: Tool, ds: str, combo: str) -> dict:
    cmd = tool.cmd_fn(ds, combo)
    t0 = time.monotonic()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        return {"status": "DNF", "detail": f"timeout {TIMEOUT}s"}
    elapsed = time.monotonic() - t0
    if p.returncode not in (0, 1):
        return {"status": "ERROR", "detail": (p.stderr or p.stdout)[-400:]}
    try:
        a, r, c = tool.parse_fn(p.stdout)
    except Exception as e:  # noqa: BLE001
        return {"status": "ERROR", "detail": f"unparseable output: {e}"}
    man = manifest(ds)
    ok = (a, r, c) == (man["added"], man["removed"], man["changed"])
    extra = {}
    if tool.name.startswith("datacompy"):
        extra = {k: v for k, v in counts_json(p.stdout).items() if k.endswith("_s")}
    return {
        "status": "OK" if ok else "WRONG",
        "got": [a, r, c],
        "want": [man["added"], man["removed"], man["changed"]],
        "gate_seconds": round(elapsed, 2),
        **extra,
    }


def shell_cmd(cmd: list[str]) -> str:
    import shlex
    return " ".join(shlex.quote(c) for c in cmd) + " > /dev/null 2>&1; true"


def hyperfine(case: str, entries: list[tuple[str, list[str]]], slow: bool) -> dict:
    out = RESULTS / f"{case}.hyperfine.json"
    runs = ["--warmup", "1", "--min-runs", "3", "--max-runs", "5"] if slow else \
           ["--warmup", "2", "--min-runs", "10"]
    cmd = ["hyperfine", "--export-json", str(out), "--style", "basic", *runs]
    for name, c in entries:
        cmd += ["--command-name", name, shell_cmd(c)]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(out.read_text())


def peak_rss(cmd: list[str]) -> int | None:
    try:
        p = subprocess.run(["/usr/bin/time", "-l", *cmd], capture_output=True,
                           text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        return None
    m = re.search(r"(\d+)\s+maximum resident set size", p.stderr)
    return int(m.group(1)) if m else None


def run_case(case: str, ds: str, combo: str) -> None:
    out_path = RESULTS / f"{case}.json"
    if out_path.exists():
        print(f"[skip] {case} (results exist)")
        return
    print(f"[case] {case} ({ds}, {combo})")
    result = {"case": case, "dataset": ds, "combo": combo, "tools": {}}
    qualified = []
    for tool in TOOLS:
        if not tool.runnable(combo):
            result["tools"][tool.name] = {"status": "N/A"}
            continue
        g = gate(tool, ds, combo)
        result["tools"][tool.name] = g
        print(f"  gate {tool.name}: {g['status']}"
              + (f" ({g.get('gate_seconds')}s)" if "gate_seconds" in g else "")
              + (f" got={g.get('got')} want={g.get('want')}" if g["status"] == "WRONG" else ""))
        if g["status"] == "OK":
            qualified.append(tool)

    slow = any(result["tools"][t.name].get("gate_seconds", 0) > 20 for t in qualified)
    entries = [(t.name, t.cmd_fn(ds, combo)) for t in qualified]
    if entries:
        hf = hyperfine(case, entries, slow)
        for res in hf["results"]:
            result["tools"][res["command"]].update({
                "mean_s": round(res["mean"], 3),
                "stddev_s": round(res["stddev"] or 0, 3),
                "min_s": round(res["min"], 3),
                "runs": len(res["times"]),
            })
    for t in qualified:
        rss = peak_rss(t.cmd_fn(ds, combo))
        if rss:
            result["tools"][t.name]["peak_rss_mb"] = round(rss / (1 << 20))
        print(f"  timed {t.name}: {result['tools'][t.name].get('mean_s', '?')}s, "
              f"{result['tools'][t.name].get('peak_rss_mb', '?')}MB")
    out_path.write_text(json.dumps(result, indent=1))


def main() -> None:
    RESULTS.mkdir(exist_ok=True)
    subprocess.run(["go", "build", "-o", VENN, "./cmd/venn"], cwd=ROOT, check=True)
    only = sys.argv[1:]
    for case, ds, combo in CASES:
        if only and case not in only:
            continue
        if not (DATA / ds / "manifest.json").exists():
            print(f"[missing] {case}: dataset {ds} not generated yet")
            continue
        run_case(case, ds, combo)
    print("done — render with bench/render_benchmarks.py")


if __name__ == "__main__":
    main()
