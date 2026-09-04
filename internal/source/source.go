// Package source defines the format-agnostic Source interface consumed by
// the diff engine, plus readers for each supported file format.
//
// The diff engine never knows the input format: it sees a Schema of logical
// types and an iterator of rows of canonical Values. Each format reader is
// responsible for mapping its physical types onto the logical model.
package source

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Type is the logical type every physical format maps onto. Comparison and
// hashing are defined purely in terms of these.
type Type uint8

const (
	TypeUnknown Type = iota
	TypeBool
	TypeInt64     // all integer widths widen to int64
	TypeFloat64   // float32 widens to float64
	TypeString    // UTF-8 text (parquet BYTE_ARRAY/UTF8, CSV text)
	TypeBytes     // raw binary
	TypeTimestamp // microseconds since Unix epoch, UTC
	TypeDate      // days since Unix epoch
)

func (t Type) String() string {
	switch t {
	case TypeBool:
		return "bool"
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBytes:
		return "bytes"
	case TypeTimestamp:
		return "timestamp"
	case TypeDate:
		return "date"
	default:
		return "unknown"
	}
}

// Column describes one column of a source's schema.
type Column struct {
	Name     string
	Type     Type
	Nullable bool
	// PhysicalType is the format's own name for the type, for schema-diff
	// display (e.g. "INT32", "DOUBLE", "csv:int").
	PhysicalType string
}

// Schema is an ordered list of columns.
type Schema struct {
	Columns []Column
}

// ColumnIndex returns the index of the named column, or -1.
func (s *Schema) ColumnIndex(name string) int {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

// Value is one cell in canonical form. Exactly one representation is
// meaningful, selected by Type; Null overrides everything.
type Value struct {
	Type  Type
	Null  bool
	Int   int64   // TypeBool (0/1), TypeInt64, TypeTimestamp (µs), TypeDate (days)
	Float float64 // TypeFloat64
	Str   string  // TypeString, TypeBytes
}

func (v Value) GoString() string { return v.Display() }

// Display renders the value for human output.
func (v Value) Display() string {
	if v.Null {
		return "NULL"
	}
	switch v.Type {
	case TypeBool:
		if v.Int != 0 {
			return "true"
		}
		return "false"
	case TypeInt64:
		return fmt.Sprintf("%d", v.Int)
	case TypeFloat64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", v.Float), "0"), ".")
	case TypeString:
		return strings.Clone(v.Str) // Str may alias a reused buffer
	case TypeBytes:
		return fmt.Sprintf("0x%x", v.Str)
	case TypeTimestamp:
		return TimestampMicrosToString(v.Int)
	case TypeDate:
		return DateDaysToString(int32(v.Int))
	default:
		return "?"
	}
}

// RowIter yields rows. Next fills dst (len == number of schema columns) and
// reports whether a row was produced.
//
// Validity contract: Value.Str may alias an internal buffer that is reused;
// values are only guaranteed valid until the next call to Next. Callers that
// retain values across calls must copy strings (strings.Clone).
type RowIter interface {
	Next(dst []Value) (bool, error)
	Close() error
}

// Source is a diffable input: a schema plus the ability to iterate its rows
// (possibly multiple times; Rows returns a fresh iterator).
type Source interface {
	Schema() Schema
	Rows() (RowIter, error)
	Close() error
}

// Col is one column of a batch in dense typed form. The engine hashes and
// compares straight off these arrays, no per-cell boxing. Exactly one of
// I64/F64/Str is populated, selected by Type (I64 carries bool as 0/1,
// timestamps as µs, dates as days). Nulls is nil when the column has no
// nulls in this batch; otherwise Nulls[r] marks row r NULL (its slot in the
// typed array is zero).
type Col struct {
	Type  Type
	Nulls []bool
	I64   []int64
	F64   []float64
	Str   []string
	// Dictionary representation for string columns (alternative to Str):
	// row r's value is Dict[Idx[r]]. Consumers can hash each dictionary
	// entry once instead of hashing every row. Idx is row-aligned (entries
	// at NULL rows are zero and meaningless).
	Dict []string
	Idx  []int32
}

// reset prepares the column to hold n rows of type typ. withNulls allocates
// (and clears) the null mask; otherwise Nulls is nil.
func (c *Col) reset(typ Type, n int, withNulls bool) {
	c.Type = typ
	switch typ {
	case TypeFloat64:
		if cap(c.F64) < n {
			c.F64 = make([]float64, n)
		}
		c.F64 = c.F64[:n]
	case TypeString, TypeBytes:
		if cap(c.Str) < n {
			c.Str = make([]string, n)
		}
		c.Str = c.Str[:n]
	default:
		if cap(c.I64) < n {
			c.I64 = make([]int64, n)
		}
		c.I64 = c.I64[:n]
	}
	c.Dict, c.Idx = nil, nil
	if withNulls {
		if cap(c.Nulls) < n {
			c.Nulls = make([]bool, n)
		}
		c.Nulls = c.Nulls[:n]
		for i := range c.Nulls {
			c.Nulls[i] = false
		}
	} else {
		c.Nulls = nil
	}
}

// truncate shortens the column to n rows.
func (c *Col) truncate(n int) {
	if c.I64 != nil {
		c.I64 = c.I64[:n]
	}
	if c.F64 != nil {
		c.F64 = c.F64[:n]
	}
	if c.Str != nil {
		c.Str = c.Str[:n]
	}
	if c.Idx != nil {
		c.Idx = c.Idx[:n]
	}
	if c.Nulls != nil {
		c.Nulls = c.Nulls[:n]
	}
}

// setNull marks row r NULL, allocating the mask on first use (rows before r
// are backfilled non-null).
func (c *Col) setNull(r, n int) {
	if c.Nulls == nil {
		c.Nulls = make([]bool, n)
	}
	c.Nulls[r] = true
}

// Value materializes one cell. Meant for rare paths (examples, changed-row
// storage, attribution compares); hot paths read the typed arrays.
func (c *Col) Value(r int) Value {
	v := Value{Type: c.Type}
	if c.Nulls != nil && c.Nulls[r] {
		v.Null = true
		return v
	}
	switch c.Type {
	case TypeFloat64:
		v.Float = c.F64[r]
	case TypeString, TypeBytes:
		if c.Idx != nil {
			v.Str = c.Dict[c.Idx[r]]
		} else {
			v.Str = c.Str[r]
		}
	default:
		v.Int = c.I64[r]
	}
	return v
}

// Batch is a column-major batch of rows. A batch (including any aliased
// strings) is only valid for the duration of the BatchFunc call that
// receives it.
type Batch struct {
	Cols []Col
	N    int // rows in the batch
}

// BatchFunc consumes one batch.
type BatchFunc func(b *Batch) error

// BatchScanner is an optional Source fast path: one full scan delivered as
// column-major batches, concurrently from up to n goroutines. makeWorker is
// called once per scanning goroutine (from that goroutine); the returned
// BatchFunc receives all batches of that goroutine. How rows are partitioned
// is the source's business (parquet: row groups; CSV: a read/parse
// pipeline). A BatchFunc error cancels the scan and is returned.
type BatchScanner interface {
	ScanBatches(n int, makeWorker func() (BatchFunc, error)) error
}

// fallbackBatchRows is the batch size used when batching a serial RowIter.
const fallbackBatchRows = 1024

// Scan delivers all rows of src as batches, using the parallel fast path
// when available and otherwise batching the serial iterator.
func Scan(src Source, n int, makeWorker func() (BatchFunc, error)) error {
	if bs, ok := src.(BatchScanner); ok {
		return bs.ScanBatches(n, makeWorker)
	}
	fn, err := makeWorker()
	if err != nil {
		return err
	}
	it, err := src.Rows()
	if err != nil {
		return err
	}
	defer it.Close()
	schema := src.Schema()
	ncols := len(schema.Columns)
	b := &Batch{Cols: make([]Col, ncols)}
	row := make([]Value, ncols)
	for {
		for ci := range b.Cols {
			b.Cols[ci].reset(schema.Columns[ci].Type, fallbackBatchRows, false)
		}
		b.N = 0
		for b.N < fallbackBatchRows {
			ok, err := it.Next(row)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			for ci := range row {
				v := &row[ci]
				c := &b.Cols[ci]
				if v.Null {
					c.setNull(b.N, fallbackBatchRows)
					continue
				}
				switch c.Type {
				case TypeFloat64:
					c.F64[b.N] = v.Float
				case TypeString, TypeBytes:
					// rows accumulate across Next calls: copy aliased strings
					c.Str[b.N] = strings.Clone(v.Str)
				default:
					c.I64[b.N] = v.Int
				}
			}
			b.N++
		}
		if b.N == 0 {
			return nil
		}
		full := b.N == fallbackBatchRows
		for ci := range b.Cols {
			b.Cols[ci].truncate(b.N)
		}
		if err := fn(b); err != nil {
			return err
		}
		if !full {
			return nil
		}
	}
}

// Options tunes how sources are opened.
type Options struct {
	// InferRows is the CSV/NDJSON type-inference sample size (0 = default
	// 1000, negative = whole file).
	InferRows int
	// Delimiter overrides the delimited-text field separator: one character,
	// or the escape "\t". Empty means the extension decides (',' for .csv,
	// tab for .tsv).
	Delimiter string
}

// delimiter resolves the field separator for a delimited-text file, falling
// back to the extension's default.
func (o Options) delimiter(def rune) (rune, error) {
	switch o.Delimiter {
	case "":
		return def, nil
	case `\t`, "tab", "\t":
		return '\t', nil
	}
	r := []rune(o.Delimiter)
	if len(r) != 1 {
		return 0, fmt.Errorf("--delimiter must be a single character (or \\t), got %q", o.Delimiter)
	}
	if r[0] > 127 {
		// the vectorized CSV scanner splits on a single byte
		return 0, fmt.Errorf("--delimiter must be an ASCII character, got %q", o.Delimiter)
	}
	return r[0], nil
}

// Open opens path with a reader chosen by file extension.
func Open(path string) (Source, error) { return OpenWith(path, Options{}) }

// OpenWith opens path with explicit options.
func OpenWith(path string, o Options) (Source, error) {
	infer := o.InferRows
	if infer == 0 {
		infer = defaultInferRows
	} else if infer < 0 {
		infer = 0 // whole file
	}
	if isRemote(path) {
		return openRemote(path, o)
	}
	if strings.ContainsAny(path, "*?[") {
		return OpenDir(path, o)
	}
	// table#snapshot addressing (Iceberg snapshot ids / Delta versions)
	base, snapshot := splitSnapshot(path)
	if st, err := os.Stat(base); err == nil && st.IsDir() {
		switch {
		case isIcebergTable(base):
			return OpenIceberg(base, snapshot, o)
		case isDeltaTable(base):
			return OpenDelta(base, snapshot, o)
		case snapshot != "":
			return nil, fmt.Errorf("%s: #%s given but the directory is not an Iceberg or Delta table", base, snapshot)
		default:
			return OpenDir(base, o)
		}
	}
	name := strings.ToLower(path)
	stem := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".zst")
	switch ext := filepath.Ext(stem); ext {
	case ".parquet":
		return OpenParquet(path)
	case ".csv", ".tsv":
		def := ','
		if ext == ".tsv" {
			def = '\t'
		}
		comma, err := o.delimiter(def)
		if err != nil {
			return nil, err
		}
		return OpenCSVInfer(path, comma, infer)
	case ".ndjson", ".jsonl":
		return OpenNDJSON(path, infer)
	default:
		return nil, fmt.Errorf("unsupported file extension %q (supported: .parquet .csv .tsv .ndjson .jsonl (+.gz/.zst for text))", ext)
	}
}
