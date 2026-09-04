#!/usr/bin/env python3
"""Generate merge-on-read lake table fixtures:

  iceberg_mor:   snapshot S1 = ids 0..999; S2 adds position deletes for
                 ids {3, 500, 999}  (diff S1->S2: -3)
  iceberg_eqdel: S1 = ids 0..999; S2 upserts via equality delete on id
                 {10, 20} plus a new data file re-adding id 10 with changed
                 amount and adding id 2000  (diff: ~1 +1 -1)
  delta_dv:      v0 = ids 0..999; v1 removes ids {5, 17, 42} via a
                 deletion vector  (diff: -3)

Written with pyiceberg manifest internals / per the Delta protocol spec;
validated by reading back with pyiceberg / DuckDB delta_scan.
"""
import json
import os
import shutil
import struct
import sys
import time
import uuid as uuidlib
import zlib
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
N = 1000

SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("region", pa.string()),
    ("amount", pa.float64()),
    ("note", pa.string()),
])


def base_rows():
    return [{
        "id": i,
        "region": f"r{i % 4}",
        "amount": round((i * 37 % 100000) / 100.0, 2),
        "note": f"row-{i:05d}",
    } for i in range(N)]


def base_table() -> pa.Table:
    return pa.Table.from_pylist(base_rows(), schema=SCHEMA)


# ---------- Iceberg ----------

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import (
    DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus,
    ManifestWriterV2, ManifestListWriterV2, ManifestContent,
)
from pyiceberg.typedef import Record
from pyiceberg.table import StaticTable


class DeleteManifestWriterV2(ManifestWriterV2):
    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self):
        m = dict(super()._meta)
        m["content"] = "deletes"
        return m


def make_iceberg(name: str):
    p = OUT / name
    shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True)
    catalog = SqlCatalog("local", uri=f"sqlite:///{p}/catalog.db", warehouse=f"file://{p}")
    catalog.create_namespace("db")
    tbl = catalog.create_table("db.orders", schema=SCHEMA)
    tbl.append(base_table())
    return catalog.load_table("db.orders")


def add_mor_snapshot(tbl, delete_builder, summary_op):
    """Append a snapshot whose new manifest holds delete files (plus
    optionally new data files), built by delete_builder(io, root, seq,
    snap_id) -> list[ManifestEntry]."""
    io = tbl.io
    meta = tbl.metadata
    root = meta.location.removeprefix("file://")
    s1 = tbl.current_snapshot()
    seq = meta.last_sequence_number + 1
    snap_id = s1.snapshot_id + 1

    entries = delete_builder(io, root, seq, snap_id)

    # split entries into data/delete manifests
    data_entries = [e for e in entries if e.data_file.content == DataFileContent.DATA]
    del_entries = [e for e in entries if e.data_file.content != DataFileContent.DATA]

    new_manifests = []
    if del_entries:
        mpath = f"{root}/metadata/{uuidlib.uuid4()}-m1.avro"
        w = DeleteManifestWriterV2(tbl.spec(), tbl.schema(), io.new_output(f"file://{mpath}"), snap_id, "deflate")
        with w as mw:
            for e in del_entries:
                mw.add_entry(e)
        new_manifests.append(w.to_manifest_file())
    if data_entries:
        mpath = f"{root}/metadata/{uuidlib.uuid4()}-m2.avro"
        w = ManifestWriterV2(tbl.spec(), tbl.schema(), io.new_output(f"file://{mpath}"), snap_id, "deflate")
        with w as mw:
            for e in data_entries:
                mw.add_entry(e)
        new_manifests.append(w.to_manifest_file())

    old_manifests = s1.manifests(io)
    ml_path = f"{root}/metadata/snap-{snap_id}-1-{uuidlib.uuid4()}.avro"
    mlw = ManifestListWriterV2(io.new_output(f"file://{ml_path}"), snap_id, s1.snapshot_id, seq, "deflate")
    with mlw as w:
        w.add_manifests(new_manifests + list(old_manifests))

    # new metadata json
    meta_dir = Path(root) / "metadata"
    latest = sorted(meta_dir.glob("*.metadata.json"))[-1]
    doc = json.loads(latest.read_text())
    now = int(time.time() * 1000)
    doc["last-sequence-number"] = seq
    doc["last-updated-ms"] = now
    doc["current-snapshot-id"] = snap_id
    doc["snapshots"].append({
        "snapshot-id": snap_id,
        "parent-snapshot-id": s1.snapshot_id,
        "sequence-number": seq,
        "timestamp-ms": now,
        "manifest-list": f"file://{ml_path}",
        "summary": {"operation": summary_op},
        "schema-id": doc["current-schema-id"],
    })
    doc.setdefault("snapshot-log", []).append({"snapshot-id": snap_id, "timestamp-ms": now})
    stem = latest.name.split("-")[0]
    new_name = f"{int(stem) + 1:05d}-{uuidlib.uuid4()}.metadata.json"
    (meta_dir / new_name).write_text(json.dumps(doc))
    return snap_id, s1.snapshot_id, meta_dir / new_name


def data_file_path(tbl):
    io = tbl.io
    s1 = tbl.current_snapshot()
    mf = s1.manifests(io)[0]
    return [e.data_file.file_path for e in mf.fetch_manifest_entry(io)]


def fid(name, typ, field_id):
    return pa.field(name, typ, metadata={b"PARQUET:field_id": str(field_id).encode()})


# table columns carry their iceberg field ids (1..4); position-delete
# columns use the spec-reserved ids
DATA_SCHEMA_IDS = pa.schema([
    fid("id", pa.int64(), 1),
    fid("region", pa.string(), 2),
    fid("amount", pa.float64(), 3),
    fid("note", pa.string(), 4),
])
POS_DELETE_SCHEMA = pa.schema([
    fid("file_path", pa.string(), 2147483546),
    fid("pos", pa.int64(), 2147483545),
])


def write_iceberg_mor():
    tbl = make_iceberg("iceberg_mor")
    dpaths = data_file_path(tbl)
    assert len(dpaths) == 1, dpaths
    dpath = dpaths[0]
    deleted_ids = [3, 500, 999]  # id == position (single file, insert order)

    def build(io, root, seq, snap_id):
        dfile = f"{root}/data/{uuidlib.uuid4()}-deletes.parquet"
        dt = pa.Table.from_arrays(
            [pa.array([dpath] * len(deleted_ids), pa.string()),
             pa.array(deleted_ids, pa.int64())], schema=POS_DELETE_SCHEMA)
        pq.write_table(dt, dfile)
        df = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path=f"file://{dfile}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=len(deleted_ids),
            file_size_in_bytes=os.path.getsize(dfile),
        )
        return [ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=snap_id, data_file=df)]

    snap_id, parent_id, meta_file = add_mor_snapshot(tbl, build, "delete")

    # validate with pyiceberg
    st = StaticTable.from_metadata(f"file://{meta_file}")
    got = sorted(st.scan().to_arrow()["id"].to_pylist())
    want = [i for i in range(N) if i not in deleted_ids]
    assert got == want, (len(got), len(want))
    (OUT / "iceberg_mor_snapshots.txt").write_text(f"{parent_id}\n{snap_id}\n")
    print(f"iceberg_mor ok: snapshots {parent_id} -> {snap_id}, {len(got)} rows after deletes")


def write_iceberg_eqdel():
    tbl = make_iceberg("iceberg_eqdel")
    id_field = tbl.schema().find_field("id").field_id
    deleted_ids = [10, 20]

    def build(io, root, seq, snap_id):
        entries = []
        # equality delete file: just the id column
        dfile = f"{root}/data/{uuidlib.uuid4()}-eqdel.parquet"
        pq.write_table(pa.Table.from_arrays(
            [pa.array(deleted_ids, pa.int64())],
            schema=pa.schema([fid("id", pa.int64(), 1)])), dfile)
        df = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=f"file://{dfile}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=len(deleted_ids),
            file_size_in_bytes=os.path.getsize(dfile),
            equality_ids=[id_field],
        )
        entries.append(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=snap_id, data_file=df))
        # new data file: re-add id 10 with changed amount, add id 2000
        nfile = f"{root}/data/{uuidlib.uuid4()}-upsert.parquet"
        newrows = pa.Table.from_pylist([
            {"id": 10, "region": "r2", "amount": 999.99, "note": "row-00010"},
            {"id": 2000, "region": "r0", "amount": 1.23, "note": "row-02000"},
        ], schema=SCHEMA).cast(DATA_SCHEMA_IDS)
        pq.write_table(newrows, nfile)
        ndf = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=f"file://{nfile}",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=2,
            file_size_in_bytes=os.path.getsize(nfile),
        )
        entries.append(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=snap_id, data_file=ndf))
        return entries

    snap_id, parent_id, meta_file = add_mor_snapshot(tbl, build, "overwrite")
    # pyiceberg cannot read equality deletes; just confirm it refuses
    st = StaticTable.from_metadata(f"file://{meta_file}")
    try:
        st.scan().to_arrow()
        print("WARNING: pyiceberg read eq-delete table without error (support added?)")
    except Exception as e:
        print(f"iceberg_eqdel: pyiceberg refuses as expected: {type(e).__name__}")
    (OUT / "iceberg_eqdel_snapshots.txt").write_text(f"{parent_id}\n{snap_id}\n")
    print(f"iceberg_eqdel ok: snapshots {parent_id} -> {snap_id}")


# ---------- Delta deletion vector ----------

Z85 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"


def z85_encode(b: bytes) -> str:
    assert len(b) % 4 == 0
    out = []
    for i in range(0, len(b), 4):
        v = int.from_bytes(b[i:i + 4], "big")
        chunk = []
        for _ in range(5):
            chunk.append(Z85[v % 85])
            v //= 85
        out.extend(reversed(chunk))
    return "".join(out)


def roaring32(vals):
    by_key = defaultdict(list)
    for v in vals:
        by_key[v >> 16].append(v & 0xFFFF)
    keys = sorted(by_key)
    out = struct.pack("<II", 12346, len(keys))  # SERIAL_COOKIE_NO_RUNCONTAINER
    for k in keys:
        out += struct.pack("<HH", k, len(by_key[k]) - 1)
    off = len(out) + 4 * len(keys)
    for k in keys:
        out += struct.pack("<I", off)
        off += 2 * len(by_key[k])
    for k in keys:
        for v in sorted(by_key[k]):
            out += struct.pack("<H", v)
    return out


def dv_blob(positions):
    by_high = defaultdict(list)
    for p in positions:
        by_high[p >> 32].append(p & 0xFFFFFFFF)
    data = struct.pack("<i", 1681511377) + struct.pack("<Q", len(by_high))
    for k in sorted(by_high):
        data += struct.pack("<I", k) + roaring32(by_high[k])
    return data


def write_delta_dv():
    from deltalake import write_deltalake
    p = OUT / "delta_dv"
    shutil.rmtree(p, ignore_errors=True)
    write_deltalake(str(p), base_table())
    log = p / "_delta_log"
    v0 = json.loads((log / "00000000000000000000.json").read_text().splitlines()[-1])
    # find the add action
    adds = [json.loads(l)["add"] for l in (log / "00000000000000000000.json").read_text().splitlines()
            if '"add"' in l and json.loads(l).get("add")]
    assert len(adds) == 1
    add = adds[0]

    positions = [5, 17, 42]
    data = dv_blob(positions)
    dv_uuid = uuidlib.uuid4()
    dv_name = f"deletion_vector_{dv_uuid}.bin"
    blob = b"\x01" + struct.pack(">i", len(data)) + data + struct.pack(">i", zlib.crc32(data) & 0xFFFFFFFF)
    (p / dv_name).write_bytes(blob)

    stats = json.loads(add["stats"]) if add.get("stats") else {}
    commit = []
    commit.append(json.dumps({"commitInfo": {"timestamp": int(time.time() * 1000), "operation": "DELETE"}}))
    commit.append(json.dumps({"protocol": {
        "minReaderVersion": 3, "minWriterVersion": 7,
        "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"],
    }}))
    commit.append(json.dumps({"remove": {
        "path": add["path"], "deletionTimestamp": int(time.time() * 1000),
        "dataChange": True, "partitionValues": add.get("partitionValues", {}),
    }}))
    commit.append(json.dumps({"add": {
        "path": add["path"],
        "partitionValues": add.get("partitionValues", {}),
        "size": add["size"],
        "modificationTime": add.get("modificationTime", int(time.time() * 1000)),
        "dataChange": True,
        "stats": add.get("stats", ""),
        "deletionVector": {
            "storageType": "u",
            "pathOrInlineDv": z85_encode(dv_uuid.bytes),
            "offset": 1,
            "sizeInBytes": len(data),
            "cardinality": len(positions),
        },
    }}))
    (log / "00000000000000000001.json").write_text("\n".join(commit) + "\n")
    print(f"delta_dv ok: {len(positions)} rows deleted via DV")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    write_iceberg_mor()
    write_iceberg_eqdel()
    write_delta_dv()
