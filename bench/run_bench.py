#!/usr/bin/env python3
"""tdiff benchmark harness.

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
TDIFF = str(ROOT / "tdiff")
CSVDIFF = os.path.expanduser("~/go/bin/csvdiff")
COMP = BENCH / "competitors"

TIMEOUT = 600  # seconds, correctness-gate cap per tool run


def files(ds: str, combo: str) -> tuple[str, str]:
    d = dataset_dir(ds)
    lf, rf = combo.split("-", 1)
    return str(d / f"left.{lf}"), str(d / f"right.{rf}")


def dataset_dir(ds: str) -> Path:
    if ds in ("100m", "10m"):
        return ROOT / "testdata" / ds
    return DATA / ds


def manifest(ds: str) -> dict:
    return json.loads((dataset_dir(ds) / "manifest.json").read_text())


class Tool:
    """One competitor: how to build its command line and parse its counts."""

    def __init__(self, name, cmd_fn, parse_fn, formats=("parquet", "csv", "ndjson", "csv.gz"),
                 cross=False, cases=None):
        self.name = name
        self.cmd_fn = cmd_fn
        self.parse_fn = parse_fn
        self.formats = formats
        self.cross = cross
        self.cases = cases  # None = any case; else restrict to these case names

    def runnable(self, case: str, combo: str) -> bool:
        if self.cases is not None and case not in self.cases:
            return False
        lf, rf = combo.split("-", 1)
        if lf != rf and not self.cross:
            return False
        return lf in self.formats and rf in self.formats


def counts_json(out: str) -> dict:
    return json.loads(out.strip().splitlines()[-1])


def tdiff_cmd(ds, combo):
    l, r = files(ds, combo)
    return [TDIFF, l, r, "--key", "id", "--format", "json"]


def tdiff_summary_cmd(ds, combo):
    return tdiff_cmd(ds, combo) + ["--summary"]


def tdiff_parse(out):
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


EXPORT_DIR = "/tmp/tdiff-bench-export"


def tdiff_export_cmd(ds, combo):
    l, r = files(ds, combo)
    os.makedirs(EXPORT_DIR, exist_ok=True)
    return [TDIFF, l, r, "--key", "id", "--format", "json",
            "--output", f"{EXPORT_DIR}/tdiff-out.csv"]


def duckdb_export_cmd(ds, combo):
    os.makedirs(EXPORT_DIR, exist_ok=True)
    SQLDIR.mkdir(exist_ok=True)
    p = SQLDIR / f"{ds}-{combo}-export.sql"
    if not p.exists():
        l, r = files(ds, combo)
        sql = subprocess.run(
            [sys.executable, str(COMP / "gen_duckdb_sql.py"), l, r, "id",
             "--export", f"{EXPORT_DIR}/duckdb-out.csv"],
            capture_output=True, text=True, check=True).stdout
        p.write_text(sql)
    return ["duckdb", "-init", "/dev/null", "-batch", "-noheader", "-list", "-f", str(p)]


def export_gate_parse(out):
    # export correctness is validated separately; the gate passes when the
    # command ran (counts come from the export files themselves)
    return (-1, -1, -1)


EXPORT_CASES = {"export-10m-1pct"}
DIFF_CASES = None  # any non-export case

# Current focus: tdiff vs the fastest competitor (DuckDB). The Python tools
# (DataComPy pandas/polars, naive pandas) and csvdiff remain implemented in
# bench/competitors for the full public chart later — they cost tens of
# minutes per case and their standing (5-200x slower) is already established.
TOOLS = [
    Tool("tdiff", tdiff_cmd, tdiff_parse, cross=True),
    Tool("tdiff-summary", tdiff_summary_cmd, tdiff_parse, cross=True),
    Tool("duckdb-counts", duckdb_cmd(False), duckdb_parse, cross=True),
    Tool("duckdb-full", duckdb_cmd(True), duckdb_parse, cross=True),
]

EXPORT_TOOLS = [
    Tool("tdiff-export", tdiff_export_cmd, tdiff_parse, cases=EXPORT_CASES),
    Tool("duckdb-export", duckdb_export_cmd, export_gate_parse, cases=EXPORT_CASES),
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
    # new-feature cases
    ("ndjson-10m-1pct", "10m", "ndjson-ndjson"),
    ("csvgz-10m-1pct", "10m", "csv.gz-csv.gz"),
    ("dictparquet-10m-1pct", "dict10m", "parquet-parquet"),
    ("export-10m-1pct", "10m", "parquet-parquet"),
    # the 100M-row laptop cases: tdiff auto-selects streaming here; Python
    # tools are expected to OOM/DNF — that is the point of the chart
    ("parquet-100m-1pct", "100m", "parquet-parquet"),
]


def export_rows(tool_name: str) -> int:
    name = "tdiff-out.csv" if tool_name.startswith("tdiff") else "duckdb-out.csv"
    path = os.path.join(EXPORT_DIR, name)
    with open(path) as f:
        return sum(1 for _ in f) - 1  # minus header


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
    man = manifest(ds)
    if tool.name.endswith("-export") or tool.name == "tdiff-export":
        want = man["added"] + man["removed"] + man["changed"]
        try:
            got = export_rows(tool.name)
        except OSError as e:
            return {"status": "ERROR", "detail": str(e)}
        if got != want:
            return {"status": "WRONG", "got": [got], "want": [want],
                    "gate_seconds": round(elapsed, 2)}
        return {"status": "OK", "got": [got], "want": [want],
                "gate_seconds": round(elapsed, 2)}
    try:
        a, r, c = tool.parse_fn(p.stdout)
    except Exception as e:  # noqa: BLE001
        return {"status": "ERROR", "detail": f"unparseable output: {e}"}
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


def hyperfine(case: str, entries: list[tuple[str, list[str], float]]) -> dict:
    """Time each tool with a run count scaled to its cost: fast tools get
    10 warm runs; >20s tools 3 runs with one warmup; >60s tools 2 cold-ish
    runs (variance on multi-minute commands is a few percent)."""
    merged = {"results": []}
    groups = {"fast": [], "slow": [], "glacial": []}
    for name, c, gate_s in entries:
        if gate_s > 60:
            groups["glacial"].append((name, c))
        elif gate_s > 20:
            groups["slow"].append((name, c))
        else:
            groups["fast"].append((name, c))
    flags = {
        "fast": ["--warmup", "2", "--min-runs", "10"],
        "slow": ["--warmup", "1", "--min-runs", "3", "--max-runs", "3"],
        "glacial": ["--min-runs", "2", "--max-runs", "2"],
    }
    for kind, group in groups.items():
        if not group:
            continue
        out = RESULTS / f"{case}.{kind}.hyperfine.json"
        cmd = ["hyperfine", "--export-json", str(out), "--style", "basic", *flags[kind]]
        for name, c in group:
            cmd += ["--command-name", name, shell_cmd(c)]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        merged["results"] += json.loads(out.read_text())["results"]
    return merged


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
    tools = TOOLS
    if case in EXPORT_CASES:
        tools = EXPORT_TOOLS
    for tool in tools:
        if not tool.runnable(case, combo):
            result["tools"][tool.name] = {"status": "N/A"}
            continue
        g = gate(tool, ds, combo)
        result["tools"][tool.name] = g
        print(f"  gate {tool.name}: {g['status']}"
              + (f" ({g.get('gate_seconds')}s)" if "gate_seconds" in g else "")
              + (f" got={g.get('got')} want={g.get('want')}" if g["status"] == "WRONG" else ""))
        if g["status"] == "OK":
            qualified.append(tool)

    entries = [(t.name, t.cmd_fn(ds, combo), result["tools"][t.name].get("gate_seconds", 0.0))
               for t in qualified]
    if entries:
        hf = hyperfine(case, entries)
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
    subprocess.run(["go", "build", "-o", TDIFF, "./cmd/tdiff"], cwd=ROOT, check=True)
    only = sys.argv[1:]
    for case, ds, combo in CASES:
        if only and case not in only:
            continue
        if not (dataset_dir(ds) / "manifest.json").exists():
            print(f"[missing] {case}: dataset {ds} not generated yet")
            continue
        run_case(case, ds, combo)
    print("done — render with bench/render_benchmarks.py")


if __name__ == "__main__":
    main()
