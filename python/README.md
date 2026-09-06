# tdiff-bin

Installs the [tdiff](https://github.com/KonMam/tdiff) binary and puts it on
your `PATH`. tdiff is a row-level keyed diff of tabular datasets: parquet,
CSV/TSV, NDJSON, directories, and Iceberg and Delta tables.

```bash
pip install tdiff-bin
tdiff left.parquet right.parquet --key id
```

This package contains no Python code beyond a thin launcher: it ships the
prebuilt static binary for your platform. It exists so a Python-first stack
can install tdiff without a Go toolchain.

```python
import subprocess, json

out = subprocess.run(
    ["tdiff", "a.parquet", "b.parquet", "--key", "id", "--format", "json"],
    capture_output=True, text=True,
)
# exit code 0 = equal or within budget, 1 = differences, 2 = error
result = json.loads(out.stdout)
print(result["added"], result["removed"], result["changed"])
```

Documentation, flags and comparison semantics live in the
[main repository](https://github.com/KonMam/tdiff). MIT licensed.
