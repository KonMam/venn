package source

import (
	"fmt"
	"io"
	"os"
	"sync"
	"unsafe"

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
	pf, err := parquet.OpenFile(f, st.Size(),
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
		parquet.ReadBufferSize(256<<10),
	)
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
	for i := range row {
		pv := &row[i]
		ci := pv.Column()
		ps.convertValue(pv, &ps.convert[ci], &dst[ci])
	}
	return nil
}

func (ps *parquetSource) convertValue(pv *parquet.Value, conv *parquetConv, out *Value) {
	out.Type = conv.typ
	out.Str = ""
	if pv.IsNull() {
		out.Null = true
		return
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
		// zero-copy view into the page buffer; valid until the next
		// Next() per the RowIter contract
		if b := pv.ByteArray(); len(b) > 0 {
			out.Str = unsafe.String(&b[0], len(b))
		}
	case TypeTimestamp:
		out.Int = pv.Int64() * conv.mulNum / conv.mulDen
	case TypeDate:
		out.Int = int64(pv.Int32())
	}
}

func (it *parquetRowIter) Close() error {
	if it.rows != nil {
		return it.rows.Close()
	}
	return nil
}

// NumRows reports the file's total row count (used to presize hash tables).
func (ps *parquetSource) NumRows() int64 { return ps.pf.NumRows() }

// ScanBatches decodes row groups concurrently, one worker per goroutine
// pulling whole row groups off a queue, emitting column-major batches.
func (ps *parquetSource) ScanBatches(n int, makeWorker func() (BatchFunc, error)) error {
	groups := ps.pf.RowGroups()
	if n > len(groups) {
		n = len(groups)
	}
	if n < 1 {
		n = 1
	}

	work := make(chan parquet.RowGroup)
	errc := make(chan error, n)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn, err := makeWorker()
			if err != nil {
				errc <- err
				return
			}
			for rg := range work {
				if err := ps.scanRowGroup(rg, fn); err != nil {
					errc <- err
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
	for _, rg := range groups {
		select {
		case work <- rg:
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

// scanChunkRows is how many rows each column is bulk-decoded ahead when
// zipping column chunks back into rows.
const scanChunkRows = 4096

// colCursor streams one column chunk's values across page boundaries,
// producing canonical Values. Plain required pages take a typed bulk-read
// fast path (no parquet.Value boxing); everything else (optional columns,
// dictionary pages, byte arrays) falls back to generic ReadValues.
//
// Exhausted pages are kept until the next fill: values handed out (including
// zero-copy byte-array views) must stay valid until the whole chunk of rows
// has been consumed, only then may page buffers return to the pool.
type colCursor struct {
	ps    *parquetSource
	conv  *parquetConv
	pages parquet.Pages
	page  parquet.Page
	vr    parquet.ValueReader
	out   []Value
	n     int
	spent []parquet.Page
	// staging buffers for bulk reads
	i64 []int64
	i32 []int32
	f64 []float64
	f32 []float32
	bl  []bool
	pv  []parquet.Value
}

// readSegment bulk-reads up to want-n values from the current page reader
// into c.out. Returns the count read and any error (io.EOF = page done).
func (c *colCursor) readSegment(want int) (int, error) {
	m := want - c.n
	out := c.out[c.n : c.n+m]
	typ := c.conv.typ

	switch typ {
	case TypeInt64, TypeTimestamp:
		if r, ok := c.vr.(parquet.Int64Reader); ok {
			if cap(c.i64) < m {
				c.i64 = make([]int64, m)
			}
			n, err := r.ReadInt64s(c.i64[:m])
			mulNum, mulDen := c.conv.mulNum, c.conv.mulDen
			for i := 0; i < n; i++ {
				v := c.i64[i]
				if mulNum != 1 {
					v *= mulNum
				} else if mulDen != 1 {
					v /= mulDen
				}
				out[i] = Value{Type: typ, Int: v}
			}
			return n, err
		}
		if r, ok := c.vr.(parquet.Int32Reader); ok { // INT32 widened to int64
			if cap(c.i32) < m {
				c.i32 = make([]int32, m)
			}
			n, err := r.ReadInt32s(c.i32[:m])
			for i := 0; i < n; i++ {
				out[i] = Value{Type: typ, Int: int64(c.i32[i])}
			}
			return n, err
		}
	case TypeDate:
		if r, ok := c.vr.(parquet.Int32Reader); ok {
			if cap(c.i32) < m {
				c.i32 = make([]int32, m)
			}
			n, err := r.ReadInt32s(c.i32[:m])
			for i := 0; i < n; i++ {
				out[i] = Value{Type: typ, Int: int64(c.i32[i])}
			}
			return n, err
		}
	case TypeFloat64:
		if r, ok := c.vr.(parquet.DoubleReader); ok {
			if cap(c.f64) < m {
				c.f64 = make([]float64, m)
			}
			n, err := r.ReadDoubles(c.f64[:m])
			for i := 0; i < n; i++ {
				out[i] = Value{Type: typ, Float: c.f64[i]}
			}
			return n, err
		}
		if r, ok := c.vr.(parquet.FloatReader); ok { // FLOAT widened to float64
			if cap(c.f32) < m {
				c.f32 = make([]float32, m)
			}
			n, err := r.ReadFloats(c.f32[:m])
			for i := 0; i < n; i++ {
				out[i] = Value{Type: typ, Float: float64(c.f32[i])}
			}
			return n, err
		}
	case TypeBool:
		if r, ok := c.vr.(parquet.BooleanReader); ok {
			if cap(c.bl) < m {
				c.bl = make([]bool, m)
			}
			n, err := r.ReadBooleans(c.bl[:m])
			for i := 0; i < n; i++ {
				b := int64(0)
				if c.bl[i] {
					b = 1
				}
				out[i] = Value{Type: typ, Int: b}
			}
			return n, err
		}
	}

	// generic fallback: handles nulls, dictionary pages, byte arrays
	if cap(c.pv) < m {
		c.pv = make([]parquet.Value, m)
	}
	n, err := c.vr.ReadValues(c.pv[:m])
	for i := 0; i < n; i++ {
		c.ps.convertValue(&c.pv[i], c.conv, &out[i])
	}
	return n, err
}

// fill reads up to want values into c.out (c.n = count actually read).
func (c *colCursor) fill(want int) error {
	for _, p := range c.spent {
		parquet.Release(p)
	}
	c.spent = c.spent[:0]
	if cap(c.out) < want {
		c.out = make([]Value, want)
	}
	c.out = c.out[:want]
	c.n = 0
	for c.n < want {
		if c.vr == nil {
			p, err := c.pages.ReadPage()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			c.page = p
			c.vr = p.Values()
		}
		n, err := c.readSegment(want)
		c.n += n
		if err == io.EOF || (err == nil && n == 0) {
			c.spent = append(c.spent, c.page)
			c.page = nil
			c.vr = nil
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *colCursor) close() {
	for _, p := range c.spent {
		parquet.Release(p)
	}
	c.spent = nil
	if c.page != nil {
		parquet.Release(c.page)
		c.page = nil
	}
	if c.pages != nil {
		c.pages.Close()
	}
}

// scanRowGroup decodes each column chunk independently in bulk and hands the
// decoded column slices to fn as one column-major batch per chunk of rows.
// No row assembly happens at all.
func (ps *parquetSource) scanRowGroup(rg parquet.RowGroup, fn BatchFunc) error {
	chunks := rg.ColumnChunks()
	cursors := make([]colCursor, len(chunks))
	for i, ch := range chunks {
		cursors[i] = colCursor{ps: ps, conv: &ps.convert[i], pages: ch.Pages()}
		defer cursors[i].close()
	}
	b := &Batch{Cols: make([][]Value, len(chunks))}
	remaining := rg.NumRows()
	for remaining > 0 {
		want := scanChunkRows
		if int64(want) > remaining {
			want = int(remaining)
		}
		for i := range cursors {
			if err := cursors[i].fill(want); err != nil {
				return err
			}
			if cursors[i].n != want {
				return fmt.Errorf("column %d: short read: %d values, want %d", i, cursors[i].n, want)
			}
			b.Cols[i] = cursors[i].out[:want]
		}
		b.N = want
		if err := fn(b); err != nil {
			return err
		}
		remaining -= int64(want)
	}
	return nil
}
