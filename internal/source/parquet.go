package source

import (
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// parquetSource reads flat (non-nested) parquet files.
type parquetSource struct {
	file    *os.File
	pf      *parquet.File
	schema  Schema
	convert []parquetConv // per leaf column
}

type parquetConv struct {
	typ    Type
	tsUnit int64 // µs multiplier/divisor for timestamps: value*mulNum/mulDen
	mulNum int64
	mulDen int64
}

// OpenParquet opens a parquet file as a Source.
func OpenParquet(path string) (Source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	ps := &parquetSource{file: f, pf: pf}
	if err := ps.buildSchema(); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ps, nil
}

func (ps *parquetSource) buildSchema() error {
	root := ps.pf.Schema()
	for _, field := range root.Fields() {
		if !field.Leaf() {
			return fmt.Errorf("nested column %q: nested schemas are not supported yet", field.Name())
		}
		conv, phys, err := parquetFieldConv(field)
		if err != nil {
			return fmt.Errorf("column %q: %w", field.Name(), err)
		}
		ps.convert = append(ps.convert, conv)
		ps.schema.Columns = append(ps.schema.Columns, Column{
			Name:         field.Name(),
			Type:         conv.typ,
			Nullable:     field.Optional(),
			PhysicalType: phys,
		})
	}
	return nil
}

func parquetFieldConv(field parquet.Field) (parquetConv, string, error) {
	t := field.Type()
	lt := t.LogicalType()
	phys := t.String()
	conv := parquetConv{mulNum: 1, mulDen: 1}

	if lt != nil && lt.Value != nil {
		switch v := lt.Value.(type) {
		case *format.StringType:
			conv.typ = TypeString
			return conv, phys, nil
		case *format.TimestampType:
			conv.typ = TypeTimestamp
			switch v.Unit.Value.(type) {
			case *format.MilliSeconds:
				conv.mulNum = 1000
			case *format.MicroSeconds:
				// already µs
			case *format.NanoSeconds:
				conv.mulDen = 1000
			}
			return conv, phys, nil
		case *format.DateType:
			conv.typ = TypeDate
			return conv, phys, nil
		case *format.IntType:
			conv.typ = TypeInt64
			return conv, phys, nil
		case *format.DecimalType:
			return conv, phys, fmt.Errorf("DECIMAL logical type is not supported yet")
		}
	}

	switch t.Kind() {
	case parquet.Boolean:
		conv.typ = TypeBool
	case parquet.Int32, parquet.Int64:
		conv.typ = TypeInt64
	case parquet.Float, parquet.Double:
		conv.typ = TypeFloat64
	case parquet.ByteArray, parquet.FixedLenByteArray:
		conv.typ = TypeBytes
	default:
		return conv, phys, fmt.Errorf("unsupported parquet type %s", phys)
	}
	return conv, phys, nil
}

func (ps *parquetSource) Schema() Schema { return ps.schema }

func (ps *parquetSource) Close() error { return ps.file.Close() }

func (ps *parquetSource) Rows() (RowIter, error) {
	return &parquetRowIter{ps: ps, groups: ps.pf.RowGroups(), buf: make([]parquet.Row, 256)}, nil
}

type parquetRowIter struct {
	ps     *parquetSource
	groups []parquet.RowGroup
	gi     int
	rows   parquet.Rows
	buf    []parquet.Row
	n, i   int
}

func (it *parquetRowIter) Next(dst []Value) (bool, error) {
	for it.i >= it.n {
		if it.rows == nil {
			if it.gi >= len(it.groups) {
				return false, nil
			}
			it.rows = it.groups[it.gi].Rows()
			it.gi++
		}
		n, err := it.rows.ReadRows(it.buf)
		it.n, it.i = n, 0
		if n == 0 {
			if err != nil && err != io.EOF {
				return false, err
			}
			it.rows.Close()
			it.rows = nil
		}
	}
	row := it.buf[it.i]
	it.i++
	return true, it.ps.convertRow(row, dst)
}

func (ps *parquetSource) convertRow(row parquet.Row, dst []Value) error {
	if len(row) != len(dst) {
		return fmt.Errorf("row has %d values, schema has %d columns", len(row), len(dst))
	}
	for _, pv := range row {
		ci := pv.Column()
		conv := &ps.convert[ci]
		out := &dst[ci]
		out.Type = conv.typ
		out.Str = ""
		if pv.IsNull() {
			out.Null = true
			continue
		}
		out.Null = false
		switch conv.typ {
		case TypeBool:
			if pv.Boolean() {
				out.Int = 1
			} else {
				out.Int = 0
			}
		case TypeInt64:
			out.Int = pv.Int64()
		case TypeFloat64:
			out.Float = pv.Double()
		case TypeString, TypeBytes:
			out.Str = string(pv.ByteArray())
		case TypeTimestamp:
			out.Int = pv.Int64() * conv.mulNum / conv.mulDen
		case TypeDate:
			out.Int = int64(pv.Int32())
		}
	}
	return nil
}

func (it *parquetRowIter) Close() error {
	if it.rows != nil {
		return it.rows.Close()
	}
	return nil
}
