#!/usr/bin/env python3
"""DataComPy comparison (pandas or polars backend).

Usage: datacompy_diff.py {pandas|polars} LEFT RIGHT KEY

Prints added/removed/changed counts as JSON, plus compute-only seconds
(everything after imports finished) so end-to-end vs compute time can both
be reported.
"""
import json
import sys
import time

T0 = time.monotonic()


def read(backend: str, path: str):
    if backend == "pandas":
        import pandas as pd
        if path.endswith(".parquet"):
            return pd.read_parquet(path)
        return pd.read_csv(path)
    import polars as pl
    if path.endswith(".parquet"):
        return pl.read_parquet(path)
    return pl.read_csv(path)


def main() -> None:
    backend, left, right, key = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    keys = key.split(",")
    import datacompy

    t_import = time.monotonic()
    lf = read(backend, left)
    rf = read(backend, right)
    t_read = time.monotonic()

    if backend == "pandas":
        cmp = datacompy.PandasCompare(lf, rf, join_columns=keys, df1_name="left", df2_name="right")
    else:
        from datacompy import PolarsCompare
        cmp = PolarsCompare(lf, rf, join_columns=keys, df1_name="left", df2_name="right")

    added = int(cmp.df2_unq_rows.shape[0])
    removed = int(cmp.df1_unq_rows.shape[0])
    # rows present in both but with any column mismatch
    common = int(cmp.intersect_rows.shape[0])
    matched = int(cmp.count_matching_rows())
    changed = common - matched
    t_done = time.monotonic()

    print(json.dumps({
        "added": added,
        "removed": removed,
        "changed": changed,
        "import_s": round(t_import - T0, 3),
        "read_s": round(t_read - t_import, 3),
        "compare_s": round(t_done - t_read, 3),
        "compute_s": round(t_done - t_import, 3),
    }))
    sys.exit(0 if added == 0 and removed == 0 and changed == 0 else 1)


if __name__ == "__main__":
    main()
