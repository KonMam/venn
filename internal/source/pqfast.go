package source

// Custom column-chunk decode kernels. parquet-go's generic page machinery
// (page objects, buffer pools, refcounts, CRC checks, per-page abstractions)
// costs more than the actual decoding for the flat schemas tdiff reads. This
// path reads a whole column chunk with one pread, walks the page headers
// itself, snappy-decodes each page, and materializes the typed column
// arrays directly:
//
//   - PLAIN INT64/DOUBLE (and µs timestamps): the decompressed bytes ARE the
//     values on little-endian machines — the column aliases the buffer,
//     zero decode work.
//   - PLAIN INT32/FLOAT/BOOLEAN: single widening/unpack loop.
//   - DELTA_LENGTH_BYTE_ARRAY strings: delta-unpacked lengths, then
//     zero-copy string views into the decompressed buffer.
//   - Optional columns: RLE definition levels → null mask, dense values
//     expanded to row-aligned slots.
//
// Anything else (dictionary encoding, nested schemas, other codecs, v1
// oddities) is detected from the chunk metadata up front and falls back to
// the parquet-go-based cursor. Page CRCs are not verified on this path
// (like most engines' defaults).

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"unsafe"

	"github.com/klauspost/compress/s2"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

var thriftCompact thrift.CompactProtocol

// fastEligible reports whether the kernel path can decode this column chunk.
func fastEligible(md *format.ColumnMetaData, conv *parquetConv, nullable bool) bool {
	switch md.Codec {
	case format.Snappy, format.Uncompressed:
	default:
		return false
	}
	if md.DictionaryPageOffset > 0 {
		return false
	}
	switch conv.typ {
	case TypeBool, TypeInt64, TypeFloat64, TypeTimestamp, TypeDate:
	case TypeString, TypeBytes:
	default:
		return false
	}
	for _, e := range md.Encoding {
		switch e {
		case format.Plain, format.RLE, format.DeltaLengthByteArray:
		default:
			return false
		}
	}
	switch md.Type {
	case format.Boolean, format.Int32, format.Int64, format.Float, format.Double, format.ByteArray:
		return true
	}
	return false
}

// fastCursor decodes one column chunk of one row group.
type fastCursor struct {
	conv     *parquetConv
	typ      Type
	physType format.Type
	nullable bool

	// windowed chunk reader state: the chunk is streamed through win with
	// pread, never held whole
	file     *os.File
	fpos     int64 // next file offset to read
	chunkEnd int64
	win      []byte
	winOff   int

	// current decoded page, handed out as slices by fill
	i64    []int64
	f64    []float64
	str    []string
	nulls  []bool // nil when the page has no nulls
	n, off int    // rows decoded in page / rows already handed out

	// owned, reused buffers backing the arrays above
	scratch  []byte // decompression target (also backs aliased values/strings)
	lenbuf   []int32
	i64own   []int64
	f64own   []float64
	strown   []string
	nullsOwn []bool

	// reused buffers for rows materialized at page crossings
	pendI64   []int64
	pendF64   []float64
	pendStr   []string
	pendNulls []bool
	pendBytes []byte
}

// fastWindow is the sliding read window over a column chunk.
const fastWindow = 512 << 10

// reset points the cursor at one column chunk. The chunk is streamed through
// a reused window buffer rather than read whole (peak-RSS matters).
func (c *fastCursor) reset(ps *parquetSource, ci int, md *format.ColumnMetaData) error {
	start := md.DataPageOffset
	if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
		start = md.DictionaryPageOffset
	}
	if c.win == nil {
		c.win = make([]byte, 0, fastWindow)
	}
	c.file = ps.file
	c.fpos = start
	c.chunkEnd = start + md.TotalCompressedSize
	c.win = c.win[:0]
	c.winOff = 0
	c.conv = &ps.convert[ci]
	c.typ = ps.convert[ci].typ
	c.physType = md.Type
	c.nullable = ps.schema.Columns[ci].Nullable
	c.n, c.off = 0, 0
	c.nulls = nil
	return nil
}

// ensure makes at least n unparsed bytes available at win[winOff:] (fewer
// only at end of chunk), shifting and refilling the window as needed.
func (c *fastCursor) ensure(n int) error {
	avail := len(c.win) - c.winOff
	if avail >= n {
		return nil
	}
	if c.winOff > 0 {
		copy(c.win, c.win[c.winOff:])
		c.win = c.win[:avail]
		c.winOff = 0
	}
	if n > cap(c.win) {
		grown := make([]byte, avail, n+4096)
		copy(grown, c.win)
		c.win = grown
	}
	want := cap(c.win) - len(c.win)
	if rem := int(c.chunkEnd - c.fpos); want > rem {
		want = rem
	}
	if want <= 0 {
		return nil
	}
	m, err := c.file.ReadAt(c.win[len(c.win):len(c.win)+want], c.fpos)
	c.win = c.win[:len(c.win)+m]
	c.fpos += int64(m)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// exhausted reports whether the whole chunk has been parsed.
func (c *fastCursor) exhausted() bool {
	return c.fpos >= c.chunkEnd && c.winOff >= len(c.win)
}

func (c *fastCursor) close() {}

// fill hands out up to want rows, decoding the next page when the current
// one is exhausted.
func (c *fastCursor) fill(col *Col, want int) (int, error) {
	filled := 0
	col.Type = c.typ
	for filled < want {
		if c.off == c.n {
			if c.exhausted() {
				break
			}
			if filled > 0 {
				// The rows already handed out alias buffers the next
				// decodePage will overwrite — move them to owned storage
				// first (happens once per page boundary, not per fill).
				c.materialize(col, filled, want)
			}
			if err := c.decodePage(); err != nil {
				return filled, err
			}
		}
		k := min(want-filled, c.n-c.off)
		if filled == 0 {
			c.slice(col, c.off, c.off+k)
		} else {
			// page boundary inside one fill: fall back to appending copies
			c.appendRows(col, filled, c.off, c.off+k)
		}
		c.off += k
		filled += k
	}
	return filled, nil
}

// slice points the Col at [lo,hi) of the current page (zero copy).
func (c *fastCursor) slice(col *Col, lo, hi int) {
	col.I64, col.F64, col.Str, col.Nulls = nil, nil, nil, nil
	switch c.typ {
	case TypeFloat64:
		col.F64 = c.f64[lo:hi]
	case TypeString, TypeBytes:
		col.Str = c.str[lo:hi]
	default:
		col.I64 = c.i64[lo:hi]
	}
	if c.nulls != nil {
		col.Nulls = c.nulls[lo:hi]
	}
}

// materialize clones the first n rows of col (currently views into cursor
// buffers) into fresh arrays with room for cap rows. String bytes are cloned
// too — they alias the decompression scratch that is about to be reused.
func (c *fastCursor) materialize(col *Col, n, capacity int) {
	switch c.typ {
	case TypeFloat64:
		if cap(c.pendF64) < capacity {
			c.pendF64 = make([]float64, capacity)
		}
		out := c.pendF64[:n]
		copy(out, col.F64[:n])
		col.F64 = out
	case TypeString, TypeBytes:
		if cap(c.pendStr) < capacity {
			c.pendStr = make([]string, capacity)
		}
		// clone string bytes into one reused arena: the originals alias the
		// scratch buffer that the next page decode overwrites
		total := 0
		for _, s := range col.Str[:n] {
			total += len(s)
		}
		if cap(c.pendBytes) < total {
			c.pendBytes = make([]byte, total)
		}
		arena := c.pendBytes[:0]
		out := c.pendStr[:n]
		for i, s := range col.Str[:n] {
			if len(s) == 0 {
				out[i] = ""
				continue
			}
			start := len(arena)
			arena = append(arena, s...)
			out[i] = unsafe.String(&arena[start], len(s))
		}
		col.Str = out
	default:
		if cap(c.pendI64) < capacity {
			c.pendI64 = make([]int64, capacity)
		}
		out := c.pendI64[:n]
		copy(out, col.I64[:n])
		col.I64 = out
	}
	if col.Nulls != nil {
		if cap(c.pendNulls) < capacity {
			c.pendNulls = make([]bool, capacity)
		}
		out := c.pendNulls[:n]
		copy(out, col.Nulls[:n])
		col.Nulls = out
	}
}

// appendRows copies rows [lo,hi) of the current page onto the end of col
// (used only when a fill crosses a page boundary; col was materialized).
func (c *fastCursor) appendRows(col *Col, dst, lo, hi int) {
	n := hi - lo
	switch c.typ {
	case TypeFloat64:
		col.F64 = append(col.F64[:dst], c.f64[lo:hi]...)
	case TypeString, TypeBytes:
		col.Str = append(col.Str[:dst], c.str[lo:hi]...)
	default:
		col.I64 = append(col.I64[:dst], c.i64[lo:hi]...)
	}
	switch {
	case col.Nulls == nil && c.nulls == nil:
	case col.Nulls == nil && c.nulls != nil:
		nn := make([]bool, dst, dst+n)
		col.Nulls = append(nn, c.nulls[lo:hi]...)
	case c.nulls == nil:
		col.Nulls = append(col.Nulls[:dst], make([]bool, n)...)
	default:
		col.Nulls = append(col.Nulls[:dst], c.nulls[lo:hi]...)
	}
}

// decodePage parses the next page header and decodes the page into the
// cursor's arrays.
func (c *fastCursor) decodePage() error {
	// page headers (incl. statistics) are small; 16KB is a generous bound
	if err := c.ensure(16384); err != nil {
		return err
	}
	pr := thriftCompact.NewReaderFromBytes(c.win[c.winOff:])
	var hdr format.PageHeader
	if err := thrift.NewDecoder(pr).Decode(&hdr); err != nil {
		return fmt.Errorf("page header: %w", err)
	}
	c.winOff += pr.BytesRead()
	if err := c.ensure(int(hdr.CompressedPageSize)); err != nil {
		return err
	}
	if len(c.win)-c.winOff < int(hdr.CompressedPageSize) {
		return fmt.Errorf("truncated page body")
	}
	body := c.win[c.winOff : c.winOff+int(hdr.CompressedPageSize)]
	c.winOff += int(hdr.CompressedPageSize)

	switch hdr.Type {
	case format.DataPageV2:
		h := hdr.DataPageHeaderV2.V
		if h.RepetitionLevelsByteLength != 0 {
			return fmt.Errorf("unexpected repetition levels in flat column")
		}
		defBytes := body[:h.DefinitionLevelsByteLength]
		values := body[h.DefinitionLevelsByteLength:]
		uncompressedValues := int(hdr.UncompressedPageSize) - int(h.DefinitionLevelsByteLength)
		if compressed, ok := h.IsCompressed.Get(); !ok || compressed {
			var err error
			values, err = c.decompress(values, uncompressedValues)
			if err != nil {
				return err
			}
		}
		return c.decodeValues(values, int(h.NumValues), int(h.NumNulls), defBytes, format.Encoding(h.Encoding))
	case format.DataPage:
		h := hdr.DataPageHeader.V
		data, err := c.decompress(body, int(hdr.UncompressedPageSize))
		if err != nil {
			return err
		}
		var defBytes []byte
		numNulls := -1 // unknown for v1: count from levels
		if c.nullable {
			if len(data) < 4 {
				return fmt.Errorf("truncated v1 levels")
			}
			ln := int(binary.LittleEndian.Uint32(data))
			defBytes = data[4 : 4+ln]
			data = data[4+ln:]
		}
		return c.decodeValues(data, int(h.NumValues), numNulls, defBytes, format.Encoding(h.Encoding))
	default:
		return fmt.Errorf("unsupported page type %d on fast path", hdr.Type)
	}
}

func (c *fastCursor) decompress(src []byte, uncompressedSize int) ([]byte, error) {
	if cap(c.scratch) < uncompressedSize {
		c.scratch = make([]byte, uncompressedSize+64)
	}
	out, err := s2.Decode(c.scratch[:uncompressedSize], src)
	if err != nil {
		return nil, fmt.Errorf("snappy: %w", err)
	}
	c.scratch = out[:cap(out)]
	return out, nil
}

// decodeValues turns one page's raw (decompressed) values plus definition
// levels into row-aligned typed arrays.
func (c *fastCursor) decodeValues(values []byte, numRows, numNulls int, defBytes []byte, enc format.Encoding) error {
	c.n, c.off = numRows, 0
	c.nulls = nil
	if c.nullable && len(defBytes) > 0 {
		nulls, cnt, err := decodeDefLevels(defBytes, numRows, c.nullsBuf(numRows))
		if err != nil {
			return err
		}
		if numNulls >= 0 && cnt != numNulls {
			return fmt.Errorf("definition levels disagree with header: %d nulls vs %d", cnt, numNulls)
		}
		numNulls = cnt
		if numNulls > 0 {
			c.nulls = nulls
		}
	} else if numNulls > 0 {
		return fmt.Errorf("nulls in a column without definition levels")
	}
	dense := numRows - max(numNulls, 0)

	switch c.physType {
	case format.Int64:
		if enc != format.Plain {
			return fmt.Errorf("unsupported INT64 encoding %v", enc)
		}
		vals, err := aliasInt64(values, dense, &c.i64own)
		if err != nil {
			return err
		}
		if mul := c.conv.mulNum; mul != 1 {
			for i := range vals {
				vals[i] *= mul
			}
		} else if div := c.conv.mulDen; div != 1 {
			for i := range vals {
				vals[i] /= div
			}
		}
		c.i64 = c.expandI64(vals, numRows)
	case format.Double:
		if enc != format.Plain {
			return fmt.Errorf("unsupported DOUBLE encoding %v", enc)
		}
		vals, err := aliasFloat64(values, dense, &c.f64own)
		if err != nil {
			return err
		}
		c.f64 = c.expandF64(vals, numRows)
	case format.Int32:
		if enc != format.Plain {
			return fmt.Errorf("unsupported INT32 encoding %v", enc)
		}
		if len(values) < 4*dense {
			return fmt.Errorf("short INT32 page")
		}
		c.i64 = grow(c.i64own[:0], numRows)
		c.i64own = c.i64
		out := c.i64
		if c.nulls == nil {
			for i := 0; i < numRows; i++ {
				out[i] = int64(int32(binary.LittleEndian.Uint32(values[4*i:])))
			}
		} else {
			vi := 0
			for i := 0; i < numRows; i++ {
				if c.nulls[i] {
					out[i] = 0
					continue
				}
				out[i] = int64(int32(binary.LittleEndian.Uint32(values[4*vi:])))
				vi++
			}
		}
	case format.Float:
		if enc != format.Plain {
			return fmt.Errorf("unsupported FLOAT encoding %v", enc)
		}
		if len(values) < 4*dense {
			return fmt.Errorf("short FLOAT page")
		}
		c.f64 = growF(c.f64own[:0], numRows)
		c.f64own = c.f64
		out := c.f64
		if c.nulls == nil {
			for i := 0; i < numRows; i++ {
				out[i] = float64(bitsToFloat32(binary.LittleEndian.Uint32(values[4*i:])))
			}
		} else {
			vi := 0
			for i := 0; i < numRows; i++ {
				if c.nulls[i] {
					out[i] = 0
					continue
				}
				out[i] = float64(bitsToFloat32(binary.LittleEndian.Uint32(values[4*vi:])))
				vi++
			}
		}
	case format.Boolean:
		if enc != format.Plain {
			return fmt.Errorf("unsupported BOOLEAN encoding %v", enc)
		}
		c.i64 = grow(c.i64own[:0], numRows)
		c.i64own = c.i64
		out := c.i64
		vi := 0
		for i := 0; i < numRows; i++ {
			if c.nulls != nil && c.nulls[i] {
				out[i] = 0
				continue
			}
			if vi/8 >= len(values) {
				return fmt.Errorf("short BOOLEAN page")
			}
			out[i] = int64(values[vi/8]>>(vi%8)) & 1
			vi++
		}
	case format.ByteArray:
		switch enc {
		case format.DeltaLengthByteArray:
			return c.decodeDeltaLengthByteArray(values, numRows, dense)
		case format.Plain:
			return c.decodePlainByteArray(values, numRows, dense)
		default:
			return fmt.Errorf("unsupported BYTE_ARRAY encoding %v", enc)
		}
	default:
		return fmt.Errorf("unsupported physical type %v on fast path", c.physType)
	}
	return nil
}

// expandI64 turns dense (non-null-only) values into row-aligned values.
// With no nulls the dense slice is returned as-is (often a zero-copy alias).
func (c *fastCursor) expandI64(dense []int64, numRows int) []int64 {
	if c.nulls == nil {
		return dense
	}
	out := grow(c.i64own[:0], numRows)
	c.i64own = out
	vi := 0
	for i := 0; i < numRows; i++ {
		if c.nulls[i] {
			out[i] = 0
			continue
		}
		out[i] = dense[vi]
		vi++
	}
	return out
}

func (c *fastCursor) expandF64(dense []float64, numRows int) []float64 {
	if c.nulls == nil {
		return dense
	}
	out := growF(c.f64own[:0], numRows)
	c.f64own = out
	vi := 0
	for i := 0; i < numRows; i++ {
		if c.nulls[i] {
			out[i] = 0
			continue
		}
		out[i] = dense[vi]
		vi++
	}
	return out
}

func (c *fastCursor) decodeDeltaLengthByteArray(values []byte, numRows, dense int) error {
	if cap(c.lenbuf) < dense {
		c.lenbuf = make([]int32, dense)
	}
	lengths := c.lenbuf[:dense]
	consumed, err := decodeDeltaBinaryPackedInt32(values, lengths)
	if err != nil {
		return err
	}
	data := values[consumed:]
	c.str = growS(c.strown[:0], numRows)
	c.strown = c.str
	out := c.str
	off := 0
	vi := 0
	for i := 0; i < numRows; i++ {
		if c.nulls != nil && c.nulls[i] {
			out[i] = ""
			continue
		}
		ln := int(lengths[vi])
		vi++
		if off+ln > len(data) {
			return fmt.Errorf("byte array overruns page")
		}
		if ln == 0 {
			out[i] = ""
		} else {
			out[i] = unsafe.String(&data[off], ln)
		}
		off += ln
	}
	return nil
}

func (c *fastCursor) decodePlainByteArray(values []byte, numRows, dense int) error {
	c.str = growS(c.strown[:0], numRows)
	c.strown = c.str
	out := c.str
	pos := 0
	vi := 0
	for i := 0; i < numRows; i++ {
		if c.nulls != nil && c.nulls[i] {
			out[i] = ""
			continue
		}
		if pos+4 > len(values) {
			return fmt.Errorf("short BYTE_ARRAY page")
		}
		ln := int(binary.LittleEndian.Uint32(values[pos:]))
		pos += 4
		if pos+ln > len(values) {
			return fmt.Errorf("byte array overruns page")
		}
		if ln == 0 {
			out[i] = ""
		} else {
			out[i] = unsafe.String(&values[pos], ln)
		}
		pos += ln
		vi++
	}
	_ = vi
	_ = dense
	return nil
}

func (c *fastCursor) nullsBuf(n int) []bool {
	if cap(c.nullsOwn) < n {
		c.nullsOwn = make([]bool, n)
	}
	return c.nullsOwn[:n]
}
