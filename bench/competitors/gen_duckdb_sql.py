#!/usr/bin/env python3
"""Generate the DuckDB diff SQL a practitioner would hand-write for a pair
of files: full outer join on the key, IS DISTINCT FROM across every other
column, count added/removed/changed.

Usage: gen_duckdb_sql.py LEFT RIGHT KEY [--full] > q.sql

--full also computes per-column changed counts (the output venn produces by
default), so both output tiers can be compared at equal work.
"""
import subprocess
import sys


def reader(path: str) -> str:
    if path.endswith(".parquet"):
        return f"read_parquet('{path}')"
    return f"read_csv('{path}')"


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
