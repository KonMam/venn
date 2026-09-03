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
		return v.Str
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
// reports whether a row was produced. After false, check Err.
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
