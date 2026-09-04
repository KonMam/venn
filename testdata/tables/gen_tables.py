#!/usr/bin/env python3
"""Generate real Iceberg (pyiceberg) and Delta (delta-rs) tables for the
integration tests, each with two versions/snapshots and known differences:

  v1: ids 0..4999
  v2: ids 3..5049, minus {10, 500, 3000} (removed vs v1 keeps 0,1,2... wait:
      v2 = v1 rows with ids {7, 1200, 4999} amount changed (+1.5),
      ids {0, 1, 2} deleted, ids {5000..5049} added.

diffs v1→v2: added=50, removed=3, changed=3 (amount only).

Run: bench/venv/bin/python testdata/tables/gen_tables.py testdata/tables
"""
import shutil
import sys
from pathlib import Path

import pyarrow as pa

OUT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
N = 5000
CHANGED = {7, 1200, 4999}
REMOVED = {0, 1, 2}
ADDED = range(N, N + 50)


def rows(version: int):
    ids = [i for i in range(N) if version == 1 or i not in REMOVED]
    if version == 2:
        ids += list(ADDED)
    out = []
    for i in ids:
        amount = round((i * 37 % 100000) / 100.0, 2)
        if version == 2 and i in CHANGED:
            amount = round(amount + 1.5, 2)
        out.append({
            "id": i,
            "region": f"r{i % 4}",
            "amount": amount,
            "note": f"row-{i:05d}",
        })
    return out


SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("region", pa.string()),
    ("amount", pa.float64()),
    ("note", pa.string()),
])


def table(version: int) -> pa.Table:
    return pa.Table.from_pylist(rows(version), schema=SCHEMA)


def write_csvs():
    for v in (1, 2):
        with open(OUT / f"v{v}.csv", "w") as f:
            f.write("id,region,amount,note\n")
            for r in rows(v):
                f.write(f"{r['id']},{r['region']},{r['amount']},{r['note']}\n")


def write_delta():
    from deltalake import write_deltalake
    p = OUT / "delta_orders"
    shutil.rmtree(p, ignore_errors=True)
    write_deltalake(str(p), table(1), partition_by=["region"])
    write_deltalake(str(p), table(2), mode="overwrite", partition_by=["region"])
    print("delta versions:", sorted(x.name for x in (p / "_delta_log").iterdir()))


def write_iceberg():
    from pyiceberg.catalog.sql import SqlCatalog
    p = OUT / "iceberg_wh"
    shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True)
    catalog = SqlCatalog("local", uri=f"sqlite:///{p}/catalog.db", warehouse=f"file://{p}")
    catalog.create_namespace("db")
    tbl = catalog.create_table("db.orders", schema=SCHEMA)
    tbl.append(table(1))
    tbl.overwrite(table(2))
    snaps = [s.snapshot_id for s in tbl.snapshots()]
    print("iceberg snapshots:", snaps)
    (OUT / "iceberg_snapshots.txt").write_text("\n".join(map(str, snaps)))
    print("table location:", tbl.location())


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    write_csvs()
    write_delta()
    write_iceberg()
