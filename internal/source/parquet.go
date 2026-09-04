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

// parquetSource reads flat parquet columns; nested columns are skipped with
// a warning rather than rejected.
type parquetSource struct {
	path     string
	file     *os.File     // nil for remote sources
	ra       io.ReaderAt  // the reader everything goes through
	remote   bool         // remote: larger windows, no per-worker reopen
	closeRA  func() error // optional extra closer for remote readers
	pf       *parquet.File
	schema   Schema
	convert  []parquetConv // per mapped column
	leafIdx  []int         // mapped column i → parquet leaf column index
	leafCol  map[int]int   // parquet leaf column index → mapped column i
	warnings []string
	// merge-on-read deletes applied to this file's rows during scanning
	// (Iceberg position/equality deletes, Delta deletion vectors)
	deletes     *fileDeletes
	groupStarts []int64 // first global row of each row group
}

// setDeletes attaches merge-on-read deletes; rows they name never leave the
// scan. Must be called before scanning starts.
func (ps *parquetSource) setDeletes(d *fileDeletes) error {
	if d.empty() {
		return nil
	}
	if _, err := resolveDeletes(d, &ps.schema, ps.path); err != nil {
		return err
	}
	ps.deletes = d
	groups := ps.pf.RowGroups()
	ps.groupStarts = make([]int64, len(groups))
	var at int64
	for i, g := range groups {
		ps.groupStarts[i] = at
		at += g.NumRows()
	}
	return nil
}

// Warnings reports columns that were skipped (nested, unsupported types).
func (ps *parquetSource) Warnings() []string { return ps.warnings }

type parquetConv struct {
	typ Type
	// µs multiplier/divisor for timestamps: value*mulNum/mulDen
	mulNum int64
	mulDen int64
	// decDiv > 0 marks an int-backed DECIMAL: value = unscaled / decDiv,
	// compared in the float64 domain.
	decDiv float64
	// int96 marks legacy 12-byte Spark/Impala timestamps.
	int96 bool
}

// recoverCorrupt converts panics from parsing corrupt/malicious files into
// errors. parquet-go (and, defensively, our own kernels) can panic on
// adversarial input; a diff tool must fail cleanly instead.
func recoverCorrupt(path string, err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("%s: corrupt parquet data: %v", path, r)
	}
}

// OpenParquet opens a parquet file as a Source.
func OpenParquet(path string) (src Source, err error) {
	defer recoverCorrupt(path, &err)
	return openParquet(path)
}

func openParquet(path string) (Source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	ps, err := newParquetSource(path, f, st.Size(), false, nil)
	if err != nil {
		f.Close()
		return nil, err
	}
	ps.file = f
	return ps, nil
}

// OpenParquetReaderAt opens parquet over any io.ReaderAt (remote objects).
func OpenParquetReaderAt(label string, ra io.ReaderAt, size int64, closer func() error) (src Source, err error) {
	defer recoverCorrupt(label, &err)
	return newParquetSource(label, ra, size, true, closer)
}

func newParquetSource(path string, ra io.ReaderAt, size int64, remote bool, closer func() error) (*parquetSource, error) {
	// Note: an mmap-backed reader was tried here and reverted — it shaved
	// only ~5% wall (reads overlap compute anyway) while the touched file
	// pages inflated peak RSS ~4×, which is a headline metric.
	pf, err := parquet.OpenFile(ra, size,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
		parquet.ReadBufferSize(256<<10),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	ps := &parquetSource{path: path, ra: ra, remote: remote, closeRA: closer, pf: pf}
	if err := ps.buildSchema(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ps, nil
}

func (ps *parquetSource) buildSchema() error {
	root := ps.pf.Schema()
	ps.leafCol = make(map[int]int)
	for _, field := range root.Fields() {
		if !field.Leaf() {
			ps.warnings = append(ps.warnings,
				fmt.Sprintf("column %q skipped: nested schemas are not compared", field.Name()))
			continue
		}
		leaf, ok := ps.pf.Schema().Lookup(field.Name())
		if !ok {
			return fmt.Errorf("column %q: not found in schema lookup", field.Name())
		}
		conv, phys, err := parquetFieldConv(field)
		if err != nil {
			ps.warnings = append(ps.warnings,
				fmt.Sprintf("column %q skipped: %v", field.Name(), err))
			continue
		}
		ps.leafCol[leaf.ColumnIndex] = len(ps.convert)
		ps.leafIdx = append(ps.leafIdx, leaf.ColumnIndex)
		ps.convert = append(ps.convert, conv)
		ps.schema.Columns = append(ps.schema.Columns, Column{
			Name:         field.Name(),
			Type:         conv.typ,
			Nullable:     field.Optional(),
			PhysicalType: phys,
		})
	}
	if len(ps.schema.Columns) == 0 {
		return fmt.Errorf("no diffable columns (all skipped: %v)", ps.warnings)
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
			switch t.Kind() {
			case parquet.Int32, parquet.Int64:
				conv.typ = TypeFloat64
				conv.decDiv = 1
				for i := int32(0); i < v.Scale; i++ {
					conv.decDiv *= 10
				}
				return conv, phys, nil
			default:
				return conv, phys, fmt.Errorf("DECIMAL(%d,%d) with %s storage is not supported yet (int32/int64-backed decimals are)", v.Precision, v.Scale, t.Kind())
			}
		}
	}

	switch t.Kind() {
	case parquet.Boolean:
		conv.typ = TypeBool
	case parquet.Int32, parquet.Int64:
		conv.typ = TypeInt64
	case parquet.Int96: // legacy Spark/Impala timestamp: julian day + nanos
		conv.typ = TypeTimestamp
		conv.int96 = true
	case parquet.Float, parquet.Double:
		conv.typ = TypeFloat64
	case parquet.ByteArray, parquet.FixedLenByteArray:
		conv.typ = TypeBytes
	default:
		return conv, phys, fmt.Errorf("unsupported parquet type %s", phys)
	}
	return conv, phys, nil
}

// int96Micros converts a legacy INT96 timestamp (nanos-of-day in the low 8
// bytes, julian day in the high 4) to µs since the Unix epoch.
func int96Micros(lo uint64, day uint32) int64 {
	const julianUnixEpoch = 2440588
	return (int64(day)-julianUnixEpoch)*86_400_000_000 + int64(lo/1000)
}

func (ps *parquetSource) Schema() Schema { return ps.schema }

func (ps *parquetSource) Close() error {
	var err error
	if ps.closeRA != nil {
		err = ps.closeRA()
	}
	if ps.file != nil {
		if ferr := ps.file.Close(); err == nil {
			err = ferr
		}
	}
	return err
}

func (ps *parquetSource) Rows() (RowIter, error) {
	it := &parquetRowIter{ps: ps, groups: ps.pf.RowGroups(), buf: make([]parquet.Row, 256)}
	if ps.deletes != nil {
		del, err := resolveDeletes(ps.deletes, &ps.schema, ps.path)
		if err != nil {
			return nil, err
		}
		it.del = del
	}
	return it, nil
}

type parquetRowIter struct {
	ps     *parquetSource
	groups []parquet.RowGroup
	gi     int
	rows   parquet.Rows
	buf    []parquet.Row
	n, i   int
	del    *deleteState
	rowPos int64 // global row position of the next row
}

func (it *parquetRowIter) Next(dst []Value) (ok bool, err error) {
	defer recoverCorrupt(it.ps.path, &err)
	return it.next(dst)
}

func (it *parquetRowIter) next(dst []Value) (bool, error) {
	for {
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
		if it.del == nil {
			return true, it.ps.convertRow(row, dst)
		}
		pos := it.rowPos
		it.rowPos++
		if err := it.ps.convertRow(row, dst); err != nil {
			return false, err
		}
		if !it.del.deletedValueRow(dst, pos) {
			return true, nil
		}
	}
}

func (ps *parquetSource) convertRow(row parquet.Row, dst []Value) error {
	for i := range row {
		pv := &row[i]
		ci, ok := ps.leafCol[pv.Column()]
		if !ok {
			continue // skipped (nested/unsupported) column
		}
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
		if conv.decDiv > 0 {
			switch pv.Kind() {
			case parquet.Int32:
				out.Float = float64(pv.Int32()) / conv.decDiv
			default:
				out.Float = float64(pv.Int64()) / conv.decDiv
			}
			return
		}
		out.Float = pv.Double()
	case TypeString, TypeBytes:
		// zero-copy view into the page buffer; valid until the next
		// Next() per the RowIter contract
		if b := pv.ByteArray(); len(b) > 0 {
			out.Str = unsafe.String(&b[0], len(b))
		}
	case TypeTimestamp:
		if conv.int96 {
			i96 := pv.Int96()
			out.Int = int96Micros(uint64(i96[0])|uint64(i96[1])<<32, i96[2])
			return
		}
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
			st := &scanState{fast: make([]fastCursor, len(ps.schema.Columns)), ra: ps.ra}
			if !ps.remote {
				if f, err := os.Open(ps.path); err == nil {
					st.file = f
					st.ra = f
				}
			}
			defer st.close()
			for gi := range work {
				err := func() (err error) {
					defer recoverCorrupt(ps.path, &err)
					return ps.scanRowGroup(groups[gi], gi, st, fn)
				}()
				if err != nil {
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

	// Typed bulk-reader branches only apply when the logical type maps 1:1
	// onto the physical page values — never for decimals (unscaled ints) or
	// INT96 timestamps, which need per-value conversion in the generic path.
	if c.conv.decDiv > 0 || c.conv.int96 {
		return c.readGeneric(want)
	}

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
	return c.readGeneric(want)
}

// readGeneric reads boxed values through parquet-go and converts each cell
// (nulls, dictionary pages, byte arrays, decimals, INT96 timestamps).
func (c *colCursor) readGeneric(want int) (int, error) {
	m := want - c.n
	col := c.col
	typ := c.conv.typ
	if cap(c.pv) < m {
		c.pv = make([]parquet.Value, m)
	}
	n, err := c.vr.ReadValues(c.pv[:m])
	if n > m { // corrupt pages can over-report; never index past the buffer
		n = m
	}
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
			if c.conv.int96 {
				i96 := pv.Int96()
				col.I64[r] = int96Micros(uint64(i96[0])|uint64(i96[1])<<32, i96[2])
				continue
			}
			col.I64[r] = pv.Int64() * c.conv.mulNum / c.conv.mulDen
		case TypeDate:
			col.I64[r] = int64(pv.Int32())
		case TypeFloat64:
			if div := c.conv.decDiv; div > 0 {
				if pv.Kind() == parquet.Int32 {
					col.F64[r] = float64(pv.Int32()) / div
				} else {
					col.F64[r] = float64(pv.Int64()) / div
				}
				continue
			}
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
var noFastPQ = os.Getenv("TDIFF_NO_FASTPQ") != ""

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
	file *os.File // per-worker descriptor (local files only)
	ra   io.ReaderAt
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
	var del *deleteState
	var pos int64
	if ps.deletes != nil {
		var err error
		if del, err = resolveDeletes(ps.deletes, &ps.schema, ps.path); err != nil {
			return err
		}
		pos = ps.groupStarts[gi]
	}
	chunks := rg.ColumnChunks()
	meta := ps.pf.Metadata()
	ncols := len(ps.schema.Columns)
	b := &Batch{Cols: make([]Col, ncols)}
	cursors := make([]pqCursor, ncols)
	for i := 0; i < ncols; i++ {
		li := ps.leafIdx[i]
		md := &meta.RowGroups[gi].Columns[li].MetaData
		if !noFastPQ && len(md.PathInSchema) == 1 && md.PathInSchema[0] == ps.schema.Columns[i].Name &&
			fastEligible(md, &ps.convert[i], ps.schema.Columns[i].Nullable) {
			fc := &st.fast[i]
			ra := st.ra
			if ra == nil {
				ra = ps.ra
			}
			if err := fc.reset(ps, ra, i, md); err != nil {
				return err
			}
			cursors[i] = fc
			continue
		}
		gc := &colCursor{ps: ps, conv: &ps.convert[i], pages: chunks[li].Pages()}
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
		if del != nil {
			del.filterBatch(b, pos)
			pos += int64(want)
		}
		if b.N > 0 {
			if err := fn(b); err != nil {
				return err
			}
		}
		remaining -= int64(want)
	}
	return nil
}
