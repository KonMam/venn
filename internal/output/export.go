package output

// Diff export: the actual differing rows as data (CSV or parquet), not just
// counts. Schema: key columns, diff_status, then <col>__left/<col>__right
// for every compared column (empty/null on the side that has no row).
// Rows are written in engine order, which is unspecified.

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"

	"venn/internal/diff"
	"venn/internal/source"
)

// NewExport creates a diff-row sink writing to path (.csv or .parquet).
func NewExport(path string, keyNames []string, keyTypes []source.Type, valNames []string, valTypes []source.Type) (diff.RowSink, func() error, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		return newCSVExport(path, keyNames, valNames)
	case ".parquet":
		return newParquetExport(path, keyNames, keyTypes, valNames, valTypes)
	default:
		return nil, nil, fmt.Errorf("--output must end in .csv or .parquet")
	}
}

func statusName(s byte) string {
	switch s {
	case 'a':
		return "added"
	case 'r':
		return "removed"
	default:
		return "changed"
	}
}

// ---- CSV export ----

type csvExport struct {
	mu  sync.Mutex
	f   *os.File
	b   *bufio.Writer
	w   *csv.Writer
	rec []string
	nk  int
}

func newCSVExport(path string, keyNames, valNames []string) (diff.RowSink, func() error, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	b := bufio.NewWriterSize(f, 1<<20)
	w := csv.NewWriter(b)
	header := append([]string{}, keyNames...)
	header = append(header, "diff_status")
	for _, v := range valNames {
		header = append(header, v+"__left", v+"__right")
	}
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, nil, err
	}
	e := &csvExport{f: f, b: b, w: w, rec: make([]string, len(header)), nk: len(keyNames)}
	closer := func() error {
		e.w.Flush()
		if err := e.w.Error(); err != nil {
			e.f.Close()
			return err
		}
		if err := e.b.Flush(); err != nil {
			e.f.Close()
			return err
		}
		return e.f.Close()
	}
	return e, closer, nil
}

func csvCell(v *source.Value) string {
	if v == nil || v.Null {
		return ""
	}
	return v.Display()
}

func (e *csvExport) WriteDiffRow(status byte, key, left, right []source.Value) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec := e.rec
	for i := range key {
		rec[i] = csvCell(&key[i])
	}
	rec[e.nk] = statusName(status)
	p := e.nk + 1
	n := (len(rec) - p) / 2
	for i := 0; i < n; i++ {
		rec[p+2*i] = ""
		rec[p+2*i+1] = ""
		if left != nil {
			rec[p+2*i] = csvCell(&left[i])
		}
		if right != nil {
			rec[p+2*i+1] = csvCell(&right[i])
		}
	}
	return e.w.Write(rec)
}

// ---- parquet export ----

type parquetExport struct {
	mu     sync.Mutex
	f      *os.File
	w      *parquet.Writer
	rb     *parquet.RowBuilder
	keyIdx []int // schema leaf index of key i
	stIdx  int   // schema leaf index of diff_status
	lIdx   []int // schema leaf index of col i left
	rIdx   []int
	types  []source.Type // value column logical types
	kTypes []source.Type
	rows   int64
}

func exportNode(t source.Type) parquet.Node {
	switch t {
	case source.TypeBool:
		return parquet.Leaf(parquet.BooleanType)
	case source.TypeInt64:
		return parquet.Int(64)
	case source.TypeFloat64:
		return parquet.Leaf(parquet.DoubleType)
	case source.TypeTimestamp:
		return parquet.Timestamp(parquet.Microsecond)
	case source.TypeDate:
		return parquet.Date()
	default:
		return parquet.String()
	}
}

func newParquetExport(path string, keyNames []string, keyTypes []source.Type, valNames []string, valTypes []source.Type) (diff.RowSink, func() error, error) {
	group := parquet.Group{}
	for i, k := range keyNames {
		group[k] = parquet.Optional(exportNode(keyTypes[i]))
	}
	group["diff_status"] = parquet.String()
	for i, v := range valNames {
		group[v+"__left"] = parquet.Optional(exportNode(valTypes[i]))
		group[v+"__right"] = parquet.Optional(exportNode(valTypes[i]))
	}
	schema := parquet.NewSchema("venn", group)
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	e := &parquetExport{
		f: f, w: parquet.NewWriter(f, schema, parquet.Compression(&parquet.Snappy)),
		rb: parquet.NewRowBuilder(schema), types: valTypes, kTypes: keyTypes,
	}
	lookup := func(name string) int {
		leaf, ok := schema.Lookup(name)
		if !ok {
			return -1
		}
		return leaf.ColumnIndex
	}
	for _, k := range keyNames {
		e.keyIdx = append(e.keyIdx, lookup(k))
	}
	e.stIdx = lookup("diff_status")
	for _, v := range valNames {
		e.lIdx = append(e.lIdx, lookup(v+"__left"))
		e.rIdx = append(e.rIdx, lookup(v+"__right"))
	}
	closer := func() error {
		if err := e.w.Close(); err != nil {
			e.f.Close()
			return err
		}
		return e.f.Close()
	}
	return e, closer, nil
}

func pqValue(v *source.Value, t source.Type) parquet.Value {
	switch t {
	case source.TypeBool:
		return parquet.BooleanValue(v.Int != 0)
	case source.TypeInt64, source.TypeTimestamp:
		return parquet.Int64Value(v.Int)
	case source.TypeFloat64:
		return parquet.DoubleValue(v.Float)
	case source.TypeDate:
		return parquet.Int32Value(int32(v.Int))
	default:
		return parquet.ByteArrayValue([]byte(v.Display()))
	}
}

func (e *parquetExport) WriteDiffRow(status byte, key, left, right []source.Value) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rb.Reset()
	for i, ci := range e.keyIdx {
		if !key[i].Null {
			e.rb.Add(ci, pqValue(&key[i], e.kTypes[i]))
		}
	}
	e.rb.Add(e.stIdx, parquet.ByteArrayValue([]byte(statusName(status))))
	for i := range e.lIdx {
		if left != nil && !left[i].Null {
			e.rb.Add(e.lIdx[i], pqValue(&left[i], e.types[i]))
		}
		if right != nil && !right[i].Null {
			e.rb.Add(e.rIdx[i], pqValue(&right[i], e.types[i]))
		}
	}
	if _, err := e.w.WriteRows([]parquet.Row{e.rb.Row()}); err != nil {
		return err
	}
	e.rows++
	if e.rows%(1<<20) == 0 {
		return e.w.Flush()
	}
	return nil
}
