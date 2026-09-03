// Package source defines the format-agnostic Source interface consumed by
// the diff engine, plus readers for each supported file format.
//
// The diff engine never knows the input format: it sees a Schema of logical
// types and an iterator of rows of canonical Values. Each format reader is
// responsible for mapping its physical types onto the logical model.
package source

import (
	"fmt"
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
// (possibly multiple times — Rows returns a fresh iterator).
type Source interface {
	Schema() Schema
	Rows() (RowIter, error)
	Close() error
}

// Batch is a column-major slice of rows: Cols[ci][r] is row r of column ci.
// A batch (including any aliased strings) is only valid for the duration of
// the BatchFunc call that receives it.
type Batch struct {
	Cols [][]Value
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
	ncols := len(src.Schema().Columns)
	b := &Batch{Cols: make([][]Value, ncols)}
	for i := range b.Cols {
		b.Cols[i] = make([]Value, fallbackBatchRows)
	}
	row := make([]Value, ncols)
	for {
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
				v := row[ci]
				// rows accumulate across Next calls here, so aliased
				// strings must be copied out of the reused buffers
				v.Str = strings.Clone(v.Str)
				b.Cols[ci][b.N] = v
			}
			b.N++
		}
		if b.N == 0 {
			return nil
		}
		full := b.N == fallbackBatchRows
		for ci := range b.Cols {
			b.Cols[ci] = b.Cols[ci][:b.N]
		}
		if err := fn(b); err != nil {
			return err
		}
		for ci := range b.Cols {
			b.Cols[ci] = b.Cols[ci][:fallbackBatchRows]
		}
		if !full {
			return nil
		}
	}
}

// Open opens path with a reader chosen by file extension.
func Open(path string) (Source, error) {
	switch ext := strings.ToLower(filepath.Ext(path)); ext {
	case ".parquet":
		return OpenParquet(path)
	case ".csv":
		return OpenCSV(path, ',')
	case ".tsv":
		return OpenCSV(path, '\t')
	default:
		return nil, fmt.Errorf("unsupported file extension %q (supported: .parquet .csv .tsv)", ext)
	}
}
