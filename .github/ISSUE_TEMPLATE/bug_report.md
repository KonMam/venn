---
name: Bug report
about: Wrong counts, a crash, a hang, or a confusing error on valid input
labels: bug
---

**What you ran**

```
venn ...
```

**What you expected, and what you got**

**Version**

Output of `venn --version`, and your OS and architecture.

**Inputs**

The formats involved (parquet, csv, ndjson, Iceberg, Delta), roughly how many
rows and columns, and anything unusual about them: duplicate keys, nulls,
mixed types in a CSV column, a non-UTF-8 encoding.

If the data is not shareable, a generated pair that reproduces it works just
as well:

```
go run ./bench/gen --rows 100000 --out /tmp/repro --formats parquet,csv
```
