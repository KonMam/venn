#!/usr/bin/env python3
"""The naive baseline: what someone writes in five minutes with pandas.
outer merge on the key, compare columns.

Usage: pandas_naive.py LEFT RIGHT KEY
"""
import json
import sys

import pandas as pd


def read(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main() -> None:
    left, right, key = sys.argv[1], sys.argv[2], sys.argv[3]
    keys = key.split(",")
    lf, rf = read(left), read(right)
    m = lf.merge(rf, on=keys, how="outer", suffixes=("_l", "_r"), indicator=True)
    added = int((m["_merge"] == "right_only").sum())
    removed = int((m["_merge"] == "left_only").sum())
    both = m[m["_merge"] == "both"]
    cols = [c for c in lf.columns if c not in keys]
    changed_mask = None
    for c in cols:
        l, r = both[f"{c}_l"], both[f"{c}_r"]
        neq = ~((l == r) | (l.isna() & r.isna()))
        changed_mask = neq if changed_mask is None else (changed_mask | neq)
    changed = int(changed_mask.sum()) if changed_mask is not None else 0
    print(json.dumps({"added": added, "removed": removed, "changed": changed}))
    sys.exit(0 if added == removed == changed == 0 else 1)


if __name__ == "__main__":
    main()
