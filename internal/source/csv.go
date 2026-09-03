package source

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

// inferSampleRows is how many data rows the CSV reader scans to infer
// column types before committing to a schema.
const inferSampleRows = 1000

// csvSource reads a delimited text file with a header row. Column types are
// inferred from a sample: the narrowest of bool → int64 → float64 → date →
// timestamp → string that fits every sampled value. Empty fields are NULL.
type csvSource struct {
	path   string
	comma  rune
	schema Schema
}

// OpenCSV opens a CSV/TSV file as a Source, inferring column types.
func OpenCSV(path string, comma rune) (Source, error) {
	cs := &csvSource{path: path, comma: comma}
	if err := cs.inferSchema(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cs, nil
}

func (cs *csvSource) newReader() (*csv.Reader, *os.File, error) {
	f, err := os.Open(cs.path)
	if err != nil {
		return nil, nil, err
	}
	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	r.Comma = cs.comma
	r.ReuseRecord = true
	return r, f, nil
}

// candidate type lattice, narrowest first
const (
	candBool = 1 << iota
	candInt
	candFloat
	candDate
	candTimestamp
)

func (cs *csvSource) inferSchema() error {
	r, f, err := cs.newReader()
	if err != nil {
		return err
	}
	defer f.Close()

	header, err := r.Read()
	if err == io.EOF {
		return fmt.Errorf("empty file")
	}
	if err != nil {
		return err
	}
	names := make([]string, len(header))
	copy(names, header)

	cand := make([]int, len(names))
	for i := range cand {
		cand[i] = candBool | candInt | candFloat | candDate | candTimestamp
	}
	nullable := make([]bool, len(names))
	nonEmpty := make([]bool, len(names))

	for n := 0; n < inferSampleRows; n++ {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for i, s := range rec {
			if s == "" {
				nullable[i] = true
				continue
			}
			nonEmpty[i] = true
			c := cand[i]
			if c == 0 {
				continue
			}
			if c&candBool != 0 && !isCSVBool(s) {
				c &^= candBool
			}
			if c&candInt != 0 {
				if _, err := strconv.ParseInt(s, 10, 64); err != nil {
					c &^= candInt
				}
			}
			if c&candFloat != 0 {
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					c &^= candFloat
				}
			}
			if c&candDate != 0 {
				if _, ok := ParseDate(s); !ok {
					c &^= candDate
				}
			}
			if c&candTimestamp != 0 {
				if _, ok := ParseTimestamp(s); !ok {
					c &^= candTimestamp
				}
			}
			cand[i] = c
		}
	}

	for i, name := range names {
		typ := TypeString
		if nonEmpty[i] {
			switch c := cand[i]; {
			case c&candBool != 0:
				typ = TypeBool
			case c&candInt != 0:
				typ = TypeInt64
			case c&candFloat != 0:
				typ = TypeFloat64
			case c&candDate != 0:
				typ = TypeDate
			case c&candTimestamp != 0:
				typ = TypeTimestamp
			}
		}
		cs.schema.Columns = append(cs.schema.Columns, Column{
			Name:         name,
			Type:         typ,
			Nullable:     nullable[i],
			PhysicalType: "csv:" + typ.String(),
		})
	}
	return nil
}

func isCSVBool(s string) bool {
	switch s {
	case "true", "false", "True", "False", "TRUE", "FALSE":
		return true
	}
	return false
}

func (cs *csvSource) Schema() Schema { return cs.schema }
func (cs *csvSource) Close() error   { return nil }

func (cs *csvSource) Rows() (RowIter, error) {
	r, f, err := cs.newReader()
	if err != nil {
		return nil, err
	}
	if _, err := r.Read(); err != nil { // skip header
		f.Close()
		return nil, err
	}
	return &csvRowIter{cs: cs, r: r, f: f, line: 1}, nil
}

type csvRowIter struct {
	cs   *csvSource
	r    *csv.Reader
	f    *os.File
	line int64
}

func (it *csvRowIter) Next(dst []Value) (bool, error) {
	rec, err := it.r.Read()
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	it.line++
	if len(rec) != len(dst) {
		return false, fmt.Errorf("%s:%d: %d fields, want %d", it.cs.path, it.line, len(rec), len(dst))
	}
	for i, s := range rec {
		col := &it.cs.schema.Columns[i]
		out := &dst[i]
		out.Type = col.Type
		out.Str = ""
		if s == "" && col.Type != TypeString {
			out.Null = true
			continue
		}
		out.Null = false
		switch col.Type {
		case TypeBool:
			out.Int = 0
			if s == "true" || s == "True" || s == "TRUE" {
				out.Int = 1
			} else if !isCSVBool(s) {
				return false, it.parseErr(i, s, "bool")
			}
		case TypeInt64:
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return false, it.parseErr(i, s, "int64")
			}
			out.Int = v
		case TypeFloat64:
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return false, it.parseErr(i, s, "float64")
			}
			out.Float = v
		case TypeDate:
			d, ok := ParseDate(s)
			if !ok {
				return false, it.parseErr(i, s, "date")
			}
			out.Int = int64(d)
		case TypeTimestamp:
			us, ok := ParseTimestamp(s)
			if !ok {
				return false, it.parseErr(i, s, "timestamp")
			}
			out.Int = us
		default: // TypeString
			out.Str = string([]byte(s)) // copy: csv reader reuses the record buffer
		}
	}
	return true, nil
}

func (it *csvRowIter) parseErr(col int, s, typ string) error {
	return fmt.Errorf("%s:%d: column %q: %q is not a valid %s (type inferred from first %d rows; mixed-type columns are read as string only if the sample shows them — consider cleaning the column)",
		it.cs.path, it.line, it.cs.schema.Columns[col].Name, s, typ, inferSampleRows)
}

func (it *csvRowIter) Close() error { return it.f.Close() }
