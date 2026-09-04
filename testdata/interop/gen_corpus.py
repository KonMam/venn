#!/usr/bin/env python3
"""Generate the parquet interop corpus: the same logical table written by
different writers with their real-world defaults and encodings, plus a
canonical CSV of identical content and one mutated CSV with known diffs.

Oracle contract used by the Go tests:
  tdiff <any corpus file> canonical.csv --key id   -> identical
  tdiff <any corpus file> mutated.csv  --key id    -> exactly diffs.json

Run: bench/venv/bin/python testdata/interop/gen_corpus.py testdata/interop
"""
import datetime as dt
import decimal
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
N = 50_000
CHANGED = {7, 500, 4999, 25_000, 49_999}          # ids with changed i1
CHANGED_S = {12, 800, 30_000}                      # ids with changed s1
REMOVED = {3, 1000, 42_000}                        # in files, not in mutated csv
ADDED = {N + 1, N + 2}                             # only in mutated csv

EPOCH = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)


def row(i: int):
    return {
        "id": i,
        "i1": (i * 2654435761) % 1_000_000,
        "f1": round((i * 97) % 100_000 / 100.0, 2),
        "s1": f"val-{i % 1000:03d}",  # low cardinality -> dictionary encoding
        "s2": f"unique-{i:08d}",      # high cardinality
        "b1": i % 3 == 0,
        "t1": EPOCH + dt.timedelta(seconds=i % 86_400, microseconds=i % 1000),
        "d1": dt.date(2020, 1, 1) + dt.timedelta(days=i % 1000),
        "dec1": decimal.Decimal(i % 100_000) / 100,  # DECIMAL(10,2), int-backed
        "nul1": None if i % 7 == 0 else i % 500,
    }


def table(ids):
    rows = [row(i) for i in ids]
    schema = pa.schema([
        ("id", pa.int64()),
        ("i1", pa.int64()),
        ("f1", pa.float64()),
        ("s1", pa.string()),
        ("s2", pa.string()),
        ("b1", pa.bool_()),
        ("t1", pa.timestamp("us", tz="UTC")),
        ("d1", pa.date32()),
        ("dec1", pa.decimal128(10, 2)),
        ("nul1", pa.int64()),
    ])
    return pa.Table.from_pylist(rows, schema=schema)


def csv_cell(k, v):
    if v is None:
        return ""
    if k == "b1":
        return "true" if v else "false"
    if k == "t1":
        return v.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    if k == "d1":
        return v.isoformat()
    if k == "f1":
        return repr(v)
    if k == "dec1":
        return str(v)
    return str(v)


def write_csv(path, ids, mutate=False):
    cols = list(row(0).keys())
    with open(path, "w") as f:
        f.write(",".join(cols) + "\n")
        for i in ids:
            r = row(i)
            if mutate:
                if i in CHANGED:
                    r["i1"] += 1
                if i in CHANGED_S:
                    r["s1"] = r["s1"] + "x"
            f.write(",".join(csv_cell(k, r[k]) for k in cols) + "\n")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    ids = list(range(N))
    t = table(ids)

    # canonical + mutated CSVs (the cross-format oracle)
    write_csv(OUT / "canonical.csv", ids)
    mut_ids = [i for i in ids if i not in REMOVED] + sorted(ADDED)
    write_csv(OUT / "mutated.csv", mut_ids, mutate=True)
    (OUT / "diffs.json").write_text(json.dumps({
        "added": len(ADDED), "removed": len(REMOVED),
        "changed": len(CHANGED | CHANGED_S),
        "column_changes": {"i1": len(CHANGED), "s1": len(CHANGED_S)},
    }, indent=1))

    # pyarrow defaults: dictionary encoding on, snappy, data page v1
    pq.write_table(t, OUT / "pyarrow-default.parquet")
    # pyarrow, dictionary disabled, v2 pages
    pq.write_table(t, OUT / "pyarrow-plain-v2.parquet",
                   use_dictionary=False, data_page_version="2.0")
    # small pages + small row groups (many page boundaries)
    pq.write_table(t, OUT / "pyarrow-smallpages.parquet",
                   data_page_size=4096, row_group_size=5000)
    # codecs
    pq.write_table(t, OUT / "pyarrow-gzip.parquet", compression="gzip")
    pq.write_table(t, OUT / "pyarrow-zstd.parquet", compression="zstd")
    pq.write_table(t, OUT / "pyarrow-uncompressed.parquet", compression="none")
    # Spark-legacy INT96 timestamps
    pq.write_table(t, OUT / "pyarrow-int96.parquet",
                   use_deprecated_int96_timestamps=True)
    # delta encodings
    pq.write_table(t, OUT / "pyarrow-delta.parquet", use_dictionary=False,
                   column_encoding={"i1": "DELTA_BINARY_PACKED",
                                    "s2": "DELTA_LENGTH_BYTE_ARRAY",
                                    "s1": "DELTA_BYTE_ARRAY"},
                   use_byte_stream_split=False)

    # polars writer
    try:
        import polars as pl
        pl.from_arrow(t).write_parquet(OUT / "polars-default.parquet")
    except Exception as e:  # noqa: BLE001
        print("polars skipped:", e)

    # DuckDB writer
    subprocess.run(["duckdb", "-init", "/dev/null", "-batch", "-c",
                    f"COPY (SELECT * FROM '{OUT}/pyarrow-default.parquet' ORDER BY id) "
                    f"TO '{OUT}/duckdb-default.parquet' (FORMAT parquet)"],
                   check=True)

    # nested schema (graceful-degradation check, not part of the oracle)
    nested = pa.Table.from_pylist(
        [{"id": i, "obj": {"a": i, "b": str(i)}} for i in range(100)],
        schema=pa.schema([("id", pa.int64()),
                          ("obj", pa.struct([("a", pa.int64()), ("b", pa.string())]))]))
    pq.write_table(nested, OUT / "pyarrow-nested.parquet")

    for p in sorted(OUT.glob("*.parquet")):
        print(f"{p.name:32s} {p.stat().st_size:>10,} bytes")


if __name__ == "__main__":
    main()
