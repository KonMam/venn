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
	path    string
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
	// Note: an mmap-backed reader was tried here and reverted — it shaved
	// only ~5% wall (reads overlap compute anyway) while the touched file
	// pages inflated peak RSS ~4×, which is a headline metric.
	pf, err := parquet.OpenFile(f, st.Size(),
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
		parquet.ReadBufferSize(256<<10),
	)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	ps := &parquetSource{path: path, file: f, pf: pf}
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

	work := make(chan int)
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
			st := &scanState{fast: make([]fastCursor, len(ps.schema.Columns))}
			if f, err := os.Open(ps.path); err == nil {
				st.file = f
			}
			defer st.close()
			for gi := range work {
				if err := ps.scanRowGroup(groups[gi], gi, st, fn); err != nil {
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
	for gi := range groups {
		select {
		case work <- gi:
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

// colCursor streams one column chunk's values across page boundaries into
// one typed Col of the batch. Plain required pages take a typed bulk-read
// fast path that lands directly in the Col's array (no parquet.Value boxing
// at all); everything else (optional columns, dictionary pages, byte arrays)
// falls back to generic ReadValues plus a typed scatter.
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
	col   *Col
	n     int
	spent []parquet.Page
	// staging buffers for reads that need widening/scatter
	i32 []int32
	f32 []float32
	bl  []bool
	pv  []parquet.Value
}

// readSegment bulk-reads up to want-n values from the current page reader
// into the target Col. Returns the count read and any error (io.EOF = page
// done).
func (c *colCursor) readSegment(want int) (int, error) {
	m := want - c.n
	col := c.col
	typ := c.conv.typ

	switch typ {
	case TypeInt64, TypeTimestamp:
		if r, ok := c.vr.(parquet.Int64Reader); ok {
			dst := col.I64[c.n : c.n+m]
			n, err := r.ReadInt64s(dst)
			if mul := c.conv.mulNum; mul != 1 {
				for i := 0; i < n; i++ {
					dst[i] *= mul
				}
			} else if div := c.conv.mulDen; div != 1 {
				for i := 0; i < n; i++ {
					dst[i] /= div
				}
			}
			return n, err
		}
		if r, ok := c.vr.(parquet.Int32Reader); ok { // INT32 widened to int64
			if cap(c.i32) < m {
				c.i32 = make([]int32, m)
			}
			n, err := r.ReadInt32s(c.i32[:m])
			dst := col.I64[c.n:]
			for i := 0; i < n; i++ {
				dst[i] = int64(c.i32[i])
			}
			return n, err
		}
	case TypeDate:
		if r, ok := c.vr.(parquet.Int32Reader); ok {
			if cap(c.i32) < m {
				c.i32 = make([]int32, m)
			}
			n, err := r.ReadInt32s(c.i32[:m])
			dst := col.I64[c.n:]
			for i := 0; i < n; i++ {
				dst[i] = int64(c.i32[i])
			}
			return n, err
		}
	case TypeFloat64:
		if r, ok := c.vr.(parquet.DoubleReader); ok {
			return r.ReadDoubles(col.F64[c.n : c.n+m])
		}
		if r, ok := c.vr.(parquet.FloatReader); ok { // FLOAT widened to float64
			if cap(c.f32) < m {
				c.f32 = make([]float32, m)
			}
			n, err := r.ReadFloats(c.f32[:m])
			dst := col.F64[c.n:]
			for i := 0; i < n; i++ {
				dst[i] = float64(c.f32[i])
			}
			return n, err
		}
	case TypeBool:
		if r, ok := c.vr.(parquet.BooleanReader); ok {
			if cap(c.bl) < m {
				c.bl = make([]bool, m)
			}
			n, err := r.ReadBooleans(c.bl[:m])
			dst := col.I64[c.n:]
			for i := 0; i < n; i++ {
				if c.bl[i] {
					dst[i] = 1
				} else {
					dst[i] = 0
				}
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
		pv := &c.pv[i]
		r := c.n + i
		if pv.IsNull() {
			col.setNull(r, want)
			continue
		}
		switch typ {
		case TypeBool:
			if pv.Boolean() {
				col.I64[r] = 1
			} else {
				col.I64[r] = 0
			}
		case TypeInt64:
			col.I64[r] = pv.Int64()
		case TypeTimestamp:
			col.I64[r] = pv.Int64() * c.conv.mulNum / c.conv.mulDen
		case TypeDate:
			col.I64[r] = int64(pv.Int32())
		case TypeFloat64:
			col.F64[r] = pv.Double()
		case TypeString, TypeBytes:
			col.Str[r] = ""
			if b := pv.ByteArray(); len(b) > 0 {
				col.Str[r] = unsafe.String(&b[0], len(b))
			}
		}
	}
	return n, err
}

// fill reads up to want values into col (returns the count read).
func (c *colCursor) fill(col *Col, want int) (int, error) {
	for _, p := range c.spent {
		parquet.Release(p)
	}
	c.spent = c.spent[:0]
	col.reset(c.conv.typ, want, false)
	c.col = col
	c.n = 0
	for c.n < want {
		if c.vr == nil {
			p, err := c.pages.ReadPage()
			if err == io.EOF {
				return c.n, nil
			}
			if err != nil {
				return c.n, err
			}
			c.page = p
			c.vr = p.Values()
		}
		n, err := c.readSegment(want)
		c.n += n
		if err == io.EOF || (err == nil && n == 0) {
			// Numeric values were copied into the Col's typed arrays, so the
			// page can go back to the pool right away; only string/bytes
			// columns hand out views into page memory and must defer the
			// release until the batch has been consumed.
			switch c.conv.typ {
			case TypeString, TypeBytes:
				c.spent = append(c.spent, c.page)
			default:
				parquet.Release(c.page)
			}
			c.page = nil
			c.vr = nil
			continue
		}
		if err != nil {
			return c.n, err
		}
	}
	return c.n, nil
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

// noFastPQ disables the custom decode kernels (debugging/benchmarking aid).
var noFastPQ = os.Getenv("VENN_NO_FASTPQ") != ""

// pqCursor decodes one column chunk into typed Col arrays, want rows at a
// time.
type pqCursor interface {
	fill(col *Col, want int) (int, error)
	close()
}

// scanState carries a worker's reusable per-column fast cursors (their
// internal buffers survive across row groups) and the worker's own file
// handle: concurrent preads on a shared descriptor serialize in the kernel
// (measured 3× slower on macOS), so every scan worker reads through its own.
type scanState struct {
	fast []fastCursor
	file *os.File
}

func (st *scanState) close() {
	if st.file != nil {
		st.file.Close()
		st.file = nil
	}
}

// scanRowGroup decodes each column chunk independently in bulk and hands the
// decoded column slices to fn as one column-major batch per chunk of rows.
// No row assembly happens at all. Chunks whose encoding/codec the custom
// kernels understand (see pqfast.go) bypass parquet-go's page machinery
// entirely; the rest use the generic cursor.
func (ps *parquetSource) scanRowGroup(rg parquet.RowGroup, gi int, st *scanState, fn BatchFunc) error {
	chunks := rg.ColumnChunks()
	meta := ps.pf.Metadata()
	b := &Batch{Cols: make([]Col, len(chunks))}
	cursors := make([]pqCursor, len(chunks))
	for i, ch := range chunks {
		md := &meta.RowGroups[gi].Columns[i].MetaData
		if !noFastPQ && len(md.PathInSchema) == 1 && md.PathInSchema[0] == ps.schema.Columns[i].Name &&
			fastEligible(md, &ps.convert[i], ps.schema.Columns[i].Nullable) {
			fc := &st.fast[i]
			file := st.file
			if file == nil {
				file = ps.file
			}
			if err := fc.reset(ps, file, i, md); err != nil {
				return err
			}
			cursors[i] = fc
			continue
		}
		gc := &colCursor{ps: ps, conv: &ps.convert[i], pages: ch.Pages()}
		defer gc.close()
		cursors[i] = gc
	}
	remaining := rg.NumRows()
	for remaining > 0 {
		want := scanChunkRows
		if int64(want) > remaining {
			want = int(remaining)
		}
		for i := range cursors {
			n, err := cursors[i].fill(&b.Cols[i], want)
			if err != nil {
				return err
			}
			if n != want {
				return fmt.Errorf("column %d: short read: %d values, want %d", i, n, want)
			}
		}
		b.N = want
		if err := fn(b); err != nil {
			return err
		}
		remaining -= int64(want)
	}
	return nil
}
