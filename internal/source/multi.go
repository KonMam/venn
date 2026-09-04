package source

// Multi-file datasets: directories, globs, and partitioned layouts. Real
// tables are directories of part files (data/date=2026-09-01/part-0.parquet),
// so a Source can be a set of files:
//
//   - schema is the union of the files' schemas, matched by name
//     (union_by_name semantics: a file missing a column yields NULLs; an
//     int64/float64 conflict widens to float64; other conflicts error)
//   - hive-style path segments (k=v) become virtual partition columns,
//     constant per file — exposed as single-entry dictionary columns, so
//     hashing them costs one memoized lookup per batch
//   - scanning parallelizes across files on top of each file's own
//     row-group/block parallelism
//
// File order is sorted for determinism. Keys must be unique across the whole
// dataset, which the engine's duplicate detection enforces naturally.

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// fileEntry is one member of a multi-file dataset.
type fileEntry struct {
	path      string
	partition map[string]string // hive path segments (k=v)
	src       Source            // opened lazily? opened eagerly for schema
}

type multiSource struct {
	label    string
	files    []fileEntry
	schema   Schema
	partCols []string // partition column names, in schema order after data cols
	// colMap[f][i] = column index in file f's schema for union column i;
	// -1 = missing (NULL), -2 = partition constant
	colMap   [][]int
	warnings []string
}

func (m *multiSource) Warnings() []string { return m.warnings }
func (m *multiSource) Schema() Schema     { return m.schema }

func (m *multiSource) Close() error {
	var err error
	for _, f := range m.files {
		if f.src != nil {
			if cerr := f.src.Close(); err == nil {
				err = cerr
			}
		}
	}
	return err
}

// NumRows sums child counts when every child reports one.
func (m *multiSource) NumRows() int64 {
	var total int64
	for _, f := range m.files {
		n, ok := f.src.(interface{ NumRows() int64 })
		if !ok {
			return -1
		}
		total += n.NumRows()
	}
	return total
}

// SizeBytes sums child sizes where reported.
func (m *multiSource) SizeBytes() int64 {
	var total int64
	for _, f := range m.files {
		if sz, ok := f.src.(interface{ SizeBytes() int64 }); ok {
			total += sz.SizeBytes()
		}
	}
	return total
}

func (m *multiSource) ForceStringColumn(name string) bool {
	any := false
	for _, f := range m.files {
		if rt, ok := f.src.(Retypeable); ok && rt.ForceStringColumn(name) {
			any = true
		}
	}
	if any {
		if i := m.schema.ColumnIndex(name); i >= 0 {
			m.schema.Columns[i].Type = TypeString
			m.schema.Columns[i].PhysicalType += "(forced)"
		}
	}
	return any
}

// hivePartition extracts k=v path segments between root and the file.
func hivePartition(root, path string) map[string]string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return nil
	}
	var out map[string]string
	for _, seg := range strings.Split(filepath.Dir(rel), string(filepath.Separator)) {
		if k, v, ok := strings.Cut(seg, "="); ok && k != "" {
			if out == nil {
				out = map[string]string{}
			}
			out[k] = v
		}
	}
	return out
}

// supportedDataExt reports whether a file name looks like a diffable input.
func supportedDataExt(name string) bool {
	n := strings.ToLower(name)
	base := strings.TrimSuffix(strings.TrimSuffix(n, ".gz"), ".zst")
	switch filepath.Ext(base) {
	case ".parquet", ".csv", ".tsv", ".ndjson", ".jsonl":
		return true
	}
	return false
}

// listLocalFiles resolves a directory (recursive) or glob into data files.
func listLocalFiles(path string) (root string, files []string, err error) {
	if strings.ContainsAny(path, "*?[") {
		matches, err := filepath.Glob(path)
		if err != nil {
			return "", nil, err
		}
		for _, mth := range matches {
			if supportedDataExt(mth) {
				files = append(files, mth)
			}
		}
		sort.Strings(files)
		return filepath.Dir(strings.SplitN(path, "*", 2)[0]), files, nil
	}
	root = strings.TrimSuffix(path, "/")
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_") {
				if p != root {
					return filepath.SkipDir // _delta_log, .hidden, _SUCCESS dirs
				}
			}
			return nil
		}
		if supportedDataExt(d.Name()) && !strings.HasPrefix(d.Name(), ".") && !strings.HasPrefix(d.Name(), "_") {
			files = append(files, p)
		}
		return nil
	})
	sort.Strings(files)
	return root, files, err
}

// OpenMulti opens a set of files as one dataset. partition maps (may be nil
// per file) add constant virtual columns. open converts one path into a
// Source (nil = OpenWith).
func OpenMulti(label string, paths []string, partitions []map[string]string, opts Options) (Source, error) {
	return openMulti(label, paths, partitions, opts, nil)
}

func openMulti(label string, paths []string, partitions []map[string]string, opts Options,
	open func(path string) (Source, error)) (Source, error) {
	if open == nil {
		open = func(p string) (Source, error) { return OpenWith(p, opts) }
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("%s: no data files found", label)
	}
	if len(paths) == 1 && (partitions == nil || len(partitions[0]) == 0) {
		return open(paths[0])
	}
	m := &multiSource{label: label}
	for i, p := range paths {
		src, err := open(p)
		if err != nil {
			m.Close()
			return nil, err
		}
		var part map[string]string
		if partitions != nil {
			part = partitions[i]
		}
		m.files = append(m.files, fileEntry{path: p, partition: part, src: src})
		if w, ok := src.(interface{ Warnings() []string }); ok {
			for _, msg := range w.Warnings() {
				m.warnings = append(m.warnings, fmt.Sprintf("%s: %s", filepath.Base(p), msg))
			}
		}
	}
	if err := m.unifySchemas(); err != nil {
		m.Close()
		return nil, err
	}
	return m, nil
}

// OpenDir opens a local directory or glob as a dataset with hive partitions.
func OpenDir(path string, opts Options) (Source, error) {
	root, files, err := listLocalFiles(path)
	if err != nil {
		return nil, err
	}
	parts := make([]map[string]string, len(files))
	for i, f := range files {
		parts[i] = hivePartition(root, f)
	}
	return OpenMulti(path, files, parts, opts)
}

// unifySchemas builds the union schema and per-file column maps.
func (m *multiSource) unifySchemas() error {
	type colInfo struct {
		col   Column
		first string
	}
	var order []string
	byName := map[string]*colInfo{}
	for _, f := range m.files {
		for _, c := range f.src.Schema().Columns {
			ci := byName[c.Name]
			if ci == nil {
				byName[c.Name] = &colInfo{col: c, first: f.path}
				order = append(order, c.Name)
				continue
			}
			if ci.col.Type != c.Type {
				num := func(t Type) bool { return t == TypeInt64 || t == TypeFloat64 }
				str := func(t Type) bool { return t == TypeString || t == TypeBytes }
				switch {
				case num(ci.col.Type) && num(c.Type):
					ci.col.Type = TypeFloat64 // int/float widen
					ci.col.PhysicalType = "mixed:float64"
				case str(ci.col.Type) && str(c.Type):
					ci.col.Type = TypeString
				default:
					return fmt.Errorf("column %q is %s in %s but %s in %s — files in one dataset must agree",
						c.Name, ci.col.Type, filepath.Base(ci.first), c.Type, filepath.Base(f.path))
				}
			}
			if c.Nullable {
				ci.col.Nullable = true
			}
		}
	}
	// partition columns (string-typed virtual columns)
	partSeen := map[string]bool{}
	for _, f := range m.files {
		for k := range f.partition {
			if byName[k] != nil {
				return fmt.Errorf("partition column %q collides with a data column", k)
			}
			if !partSeen[k] {
				partSeen[k] = true
				m.partCols = append(m.partCols, k)
			}
		}
	}
	sort.Strings(m.partCols)

	for _, name := range order {
		ci := byName[name]
		// a column absent from some file is nullable there
		for _, f := range m.files {
			sc := f.src.Schema()
			if sc.ColumnIndex(name) < 0 {
				ci.col.Nullable = true
				break
			}
		}
		m.schema.Columns = append(m.schema.Columns, ci.col)
	}
	for _, k := range m.partCols {
		m.schema.Columns = append(m.schema.Columns, Column{
			Name: k, Type: TypeString, PhysicalType: "partition",
		})
	}

	m.colMap = make([][]int, len(m.files))
	for fi, f := range m.files {
		fm := make([]int, len(m.schema.Columns))
		fs := f.src.Schema()
		for i, c := range m.schema.Columns {
			switch {
			case partSeen[c.Name] && i >= len(order):
				fm[i] = -2
			default:
				sc := fs
				fm[i] = sc.ColumnIndex(c.Name) // -1 when missing
			}
		}
		m.colMap[fi] = fm
	}
	return nil
}

// translator adapts one file's batches to the union schema: existing columns
// alias the child's, missing ones are NULL columns, partition values are
// single-entry dictionaries.
type translator struct {
	m     *multiSource
	fi    int
	out   Batch
	nulls Col     // reusable all-NULL column
	zeros []int32 // reusable zero indices for partition dictionaries
	parts []Col   // per union column: prepared partition constant
}

func newTranslator(m *multiSource, fi int) *translator {
	t := &translator{m: m, fi: fi}
	t.out.Cols = make([]Col, len(m.schema.Columns))
	t.parts = make([]Col, len(m.schema.Columns))
	f := m.files[fi]
	for i, c := range m.schema.Columns {
		if m.colMap[fi][i] == -2 {
			v := f.partition[c.Name]
			t.parts[i] = Col{Type: TypeString, Dict: []string{v}}
		}
	}
	return t
}

// ensure sizes the reusable NULL/constant backing to n rows.
func (t *translator) ensure(n int) {
	if len(t.zeros) < n {
		t.zeros = make([]int32, n)
		t.nulls.Nulls = make([]bool, n)
		for i := range t.nulls.Nulls {
			t.nulls.Nulls[i] = true
		}
		t.nulls.I64 = make([]int64, n)
		t.nulls.F64 = make([]float64, n)
		t.nulls.Str = make([]string, n)
	}
}

func (t *translator) translate(b *Batch) *Batch {
	t.ensure(b.N)
	fm := t.m.colMap[t.fi]
	for i := range t.out.Cols {
		switch ci := fm[i]; {
		case ci >= 0:
			t.out.Cols[i] = b.Cols[ci]
		case ci == -2: // partition constant: single-entry dictionary
			c := t.parts[i]
			c.Idx = t.zeros[:b.N]
			t.out.Cols[i] = c
		default: // missing column: all NULL
			typ := t.m.schema.Columns[i].Type
			c := Col{Type: typ, Nulls: t.nulls.Nulls[:b.N]}
			switch typ {
			case TypeFloat64:
				c.F64 = t.nulls.F64[:b.N]
			case TypeString, TypeBytes:
				c.Str = t.nulls.Str[:b.N]
			default:
				c.I64 = t.nulls.I64[:b.N]
			}
			t.out.Cols[i] = c
		}
	}
	t.out.N = b.N
	return &t.out
}

// ScanBatches runs child scans concurrently: up to fileP files in flight,
// each with the file-level share of workers (a lone big file still gets all
// of them).
func (m *multiSource) ScanBatches(n int, makeWorker func() (BatchFunc, error)) error {
	if n < 1 {
		n = 1
	}
	fileP := n
	if len(m.files) < fileP {
		fileP = len(m.files)
	}
	perFile := max(1, n/fileP)

	work := make(chan int)
	errc := make(chan error, fileP)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < fileP; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range work {
				fi := fi
				err := Scan(m.files[fi].src, perFile, func() (BatchFunc, error) {
					fn, err := makeWorker()
					if err != nil {
						return nil, err
					}
					tr := newTranslator(m, fi)
					return func(b *Batch) error { return fn(tr.translate(b)) }, nil
				})
				if err != nil {
					errc <- fmt.Errorf("%s: %w", m.files[fi].path, err)
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	var firstErr error
feed:
	for fi := range m.files {
		select {
		case work <- fi:
		case firstErr = <-errc:
			break feed
		}
	}
	close(work)
	select {
	case <-done:
	case err := <-errc:
		if firstErr == nil {
			firstErr = err
		}
		<-done
	}
	if firstErr == nil {
		select {
		case firstErr = <-errc:
		default:
		}
	}
	return firstErr
}

// Rows chains the children serially through the union mapping.
func (m *multiSource) Rows() (RowIter, error) {
	return &multiRowIter{m: m}, nil
}

type multiRowIter struct {
	m     *multiSource
	fi    int
	it    RowIter
	child []Value
}

func (it *multiRowIter) Next(dst []Value) (bool, error) {
	for {
		if it.it == nil {
			if it.fi >= len(it.m.files) {
				return false, nil
			}
			var err error
			it.it, err = it.m.files[it.fi].src.Rows()
			if err != nil {
				return false, err
			}
			it.child = make([]Value, len(it.m.files[it.fi].src.Schema().Columns))
		}
		ok, err := it.it.Next(it.child)
		if err != nil {
			return false, err
		}
		if !ok {
			it.it.Close()
			it.it = nil
			it.fi++
			continue
		}
		fm := it.m.colMap[it.fi]
		f := it.m.files[it.fi]
		for i := range dst {
			switch ci := fm[i]; {
			case ci >= 0:
				dst[i] = it.child[ci]
			case ci == -2:
				dst[i] = Value{Type: TypeString, Str: f.partition[it.m.schema.Columns[i].Name]}
			default:
				dst[i] = Value{Type: it.m.schema.Columns[i].Type, Null: true}
			}
		}
		return true, nil
	}
}

func (it *multiRowIter) Close() error {
	if it.it != nil {
		return it.it.Close()
	}
	return nil
}
