#!/usr/bin/env python3
"""Generate the DuckDB diff SQL a practitioner would hand-write for a pair
of files: full outer join on the key, IS DISTINCT FROM across every other
column, count added/removed/changed.

Usage: gen_duckdb_sql.py LEFT RIGHT KEY [--full|--export OUT.csv] > q.sql

--full also computes per-column changed counts (the output venn produces by
default), so both output tiers can be compared at equal work.
"""
import subprocess
import sys


def reader(path: str) -> str:
    if path.endswith(".parquet"):
        return f"read_parquet('{path}')"
    if ".ndjson" in path or ".jsonl" in path:
        return f"read_json_auto('{path}', format='newline_delimited')"
    return f"read_csv('{path}')"  # DuckDB handles .gz/.zst transparently


def columns(path: str) -> list[str]:
    out = subprocess.run(
        ["duckdb", "-init", "/dev/null", "-batch", "-noheader", "-list",
         "-c", f"DESCRIBE SELECT * FROM {reader(path)}"],
        capture_output=True, text=True, check=True,
    )
    return [l.split("|")[0] for l in out.stdout.splitlines() if l.strip()]


def main() -> None:
    left, right, key = sys.argv[1], sys.argv[2], sys.argv[3]
    full = "--full" in sys.argv[4:]
    export = ""
    if "--export" in sys.argv[4:]:
        export = sys.argv[sys.argv.index("--export") + 1]
    keys = key.split(",")
    cols = [c for c in columns(left) if c not in keys]
    on = " AND ".join(f"l.{k} = r.{k}" for k in keys)
    k0 = keys[0]
    proj = ", ".join(f"l.{c} l_{c}, r.{c} r_{c}" for c in cols)
    chg = " OR ".join(f"l_{c} IS DISTINCT FROM r_{c}" for c in cols)
    extra = ""
    if full:
        extra = ",\n" + ",\n".join(
            f"       count(*) FILTER (lid IS NOT NULL AND rid IS NOT NULL AND l_{c} IS DISTINCT FROM r_{c}) AS chg_{c}"
            for c in cols)
    if export:
        # differing rows as data: the DuckDB equivalent of venn --output
        sel = ", ".join(f"l_{c} AS {c}__left, r_{c} AS {c}__right" for c in cols)
        print(f"""\
COPY (
WITH l AS (SELECT * FROM {reader(left)}),
     r AS (SELECT * FROM {reader(right)}),
     j AS (SELECT l.{k0} lid, r.{k0} rid, {proj}
           FROM l FULL OUTER JOIN r ON {on})
SELECT coalesce(lid, rid) AS {k0},
       CASE WHEN lid IS NULL THEN 'added' WHEN rid IS NULL THEN 'removed' ELSE 'changed' END AS diff_status,
       {sel}
FROM j
WHERE lid IS NULL OR rid IS NULL OR ({chg})
) TO '{export}' (FORMAT csv);
SELECT 0, 0, 0;""")
        return
    print(f"""\
WITH l AS (SELECT * FROM {reader(left)}),
     r AS (SELECT * FROM {reader(right)}),
     j AS (SELECT l.{k0} lid, r.{k0} rid, {proj}
           FROM l FULL OUTER JOIN r ON {on})
SELECT count(*) FILTER (lid IS NULL)  AS added,
       count(*) FILTER (rid IS NULL)  AS removed,
       count(*) FILTER (lid IS NOT NULL AND rid IS NOT NULL AND ({chg})) AS changed{extra}
FROM j;""")


if __name__ == "__main__":
    main()
