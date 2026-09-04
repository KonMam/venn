package source

// Custom column-chunk decode kernels. parquet-go's generic page machinery
// (page objects, buffer pools, refcounts, CRC checks, per-page abstractions)
// costs more than the actual decoding for the flat schemas tdiff reads. This
// path reads a whole column chunk with one pread, walks the page headers
// itself, snappy-decodes each page, and materializes the typed column
// arrays directly:
//
//   - PLAIN INT64/DOUBLE (and µs timestamps): the decompressed bytes ARE the
//     values on little-endian machines: the column aliases the buffer,
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

	"unsafe"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

var thriftCompact thrift.CompactProtocol

// zstdDecoder is shared: DecodeAll on a zero-concurrency decoder is safe for
// concurrent use and allocation-free once warmed.
var zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

// fastEligible reports whether the kernel path can decode this column chunk.
func fastEligible(md *format.ColumnMetaData, conv *parquetConv, nullable bool) bool {
	switch md.Codec {
	case format.Snappy, format.Uncompressed, format.Zstd:
	default:
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
		// PlainDictionary is deprecated for writers; files in the wild
		// still carry it, so the reader must keep accepting it.
		case format.Plain, format.RLE, format.DeltaLengthByteArray,
			format.PlainDictionary, format.RLEDictionary: //nolint:staticcheck
		default:
			return false
		}
	}
	switch md.Type {
	case format.Boolean, format.Int32, format.Int64, format.Int96,
		format.Float, format.Double, format.ByteArray:
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
	codec    format.CompressionCodec

	// windowed chunk reader state: the chunk is streamed through win with
	// ReadAt, never held whole
	file     io.ReaderAt
	fpos     int64 // next file offset to read
	chunkEnd int64
	win      []byte
	winOff   int

	// current decoded page, handed out as slices by fill
	i64    []int64
	f64    []float64
	str    []string
	idx    []int32 // dictionary indices (string dict pages), row-aligned
	nulls  []bool  // nil when the page has no nulls
	n, off int     // rows decoded in page / rows already handed out

	// owned, reused buffers backing the arrays above
	scratch  []byte // decompression target (also backs aliased values/strings)
	lenbuf   []int32
	i64own   []int64
	f64own   []float64
	strown   []string
	nullsOwn []bool
	idxOwn   []int32
	idxBuf   []int32

	// dictionary state for the current chunk (dictScratch owns the bytes the
	// dict strings alias, so it must survive the whole chunk)
	dictI64     []int64
	dictF64     []float64
	dictStr     []string
	dictScratch []byte
	hasDict     bool

	// reused buffers for rows materialized at page crossings
	pendI64   []int64
	pendF64   []float64
	pendStr   []string
	pendNulls []bool
	pendBytes []byte
}

// fastWindow is the sliding read window over a column chunk; remoteWindow is
// its size for remote objects, where each refill is a range request and
// per-request latency dominates small reads.
const (
	fastWindow   = 512 << 10
	remoteWindow = 8 << 20
)

// reset points the cursor at one column chunk, reading through the given
// (per-worker) reader. The chunk is streamed through a reused window buffer
// rather than read whole (peak-RSS matters). Remote sources use a window
// large enough to fetch most chunks in one range request.
func (c *fastCursor) reset(ps *parquetSource, file io.ReaderAt, ci int, md *format.ColumnMetaData) error {
	start := md.DataPageOffset
	if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
		start = md.DictionaryPageOffset
	}
	if c.win == nil {
		win := fastWindow
		if ps.remote {
			win = remoteWindow
		}
		c.win = make([]byte, 0, win)
	}
	c.file = file
	c.fpos = start
	c.chunkEnd = start + md.TotalCompressedSize
	c.win = c.win[:0]
	c.winOff = 0
	c.conv = &ps.convert[ci]
	c.typ = ps.convert[ci].typ
	c.physType = md.Type
	c.nullable = ps.schema.Columns[ci].Nullable
	c.codec = md.Codec
	c.n, c.off = 0, 0
	c.nulls = nil
	c.idx = nil
	c.hasDict = false
	c.dictI64, c.dictF64, c.dictStr = nil, nil, nil
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
				// decodePage will overwrite, so move them to owned storage
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
	col.Dict, col.Idx = nil, nil
	switch c.typ {
	case TypeFloat64:
		col.F64 = c.f64[lo:hi]
	case TypeString, TypeBytes:
		if c.idx != nil {
			col.Dict = c.dictStr
			col.Idx = c.idx[lo:hi]
		} else {
			col.Str = c.str[lo:hi]
		}
	default:
		col.I64 = c.i64[lo:hi]
	}
	if c.nulls != nil {
		col.Nulls = c.nulls[lo:hi]
	}
}

// materialize clones the first n rows of col (currently views into cursor
// buffers, possibly dictionary-backed) into fresh owned arrays with room for
// cap rows. String bytes are cloned too, since they alias buffers that the next
// page decode may reuse.
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
		out := c.pendStr[:n]
		// resolve dictionary indices to strings, then clone all bytes into
		// one reused arena (the originals alias scratch/dict buffers)
		total := 0
		for i := 0; i < n; i++ {
			total += len(c.rowStr(col, i))
		}
		if cap(c.pendBytes) < total {
			c.pendBytes = make([]byte, total)
		}
		arena := c.pendBytes[:0]
		for i := 0; i < n; i++ {
			sv := c.rowStr(col, i)
			if len(sv) == 0 {
				out[i] = ""
				continue
			}
			startOff := len(arena)
			arena = append(arena, sv...)
			out[i] = unsafe.String(&arena[startOff], len(sv))
		}
		col.Str = out
		col.Dict, col.Idx = nil, nil
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

// rowStr reads row i of a string col view (plain or dictionary-backed).
func (c *fastCursor) rowStr(col *Col, i int) string {
	if col.Nulls != nil && col.Nulls[i] {
		return ""
	}
	if col.Idx != nil {
		return col.Dict[col.Idx[i]]
	}
	return col.Str[i]
}

// appendRows copies rows [lo,hi) of the current page onto the end of col
// (used only when a fill crosses a page boundary; col was materialized).
func (c *fastCursor) appendRows(col *Col, dst, lo, hi int) {
	n := hi - lo
	switch c.typ {
	case TypeFloat64:
		col.F64 = append(col.F64[:dst], c.f64[lo:hi]...)
	case TypeString, TypeBytes:
		col.Str = col.Str[:dst]
		for r := lo; r < hi; r++ {
			var sv string
			if c.nulls == nil || !c.nulls[r] {
				if c.idx != nil {
					sv = c.dictStr[c.idx[r]]
				} else {
					sv = c.str[r]
				}
			}
			col.Str = append(col.Str, sv)
		}
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
	if hdr.CompressedPageSize < 0 || hdr.UncompressedPageSize < 0 ||
		hdr.UncompressedPageSize > 1<<30 {
		return fmt.Errorf("implausible page sizes: compressed=%d uncompressed=%d",
			hdr.CompressedPageSize, hdr.UncompressedPageSize)
	}
	if err := c.ensure(int(hdr.CompressedPageSize)); err != nil {
		return err
	}
	if len(c.win)-c.winOff < int(hdr.CompressedPageSize) {
		return fmt.Errorf("truncated page body")
	}
	body := c.win[c.winOff : c.winOff+int(hdr.CompressedPageSize)]
	c.winOff += int(hdr.CompressedPageSize)

	switch hdr.Type {
	case format.DictionaryPage:
		data, err := c.decompressDict(body, int(hdr.UncompressedPageSize))
		if err != nil {
			return err
		}
		if nv := hdr.DictionaryPageHeader.V.NumValues; nv < 0 || nv > 1<<27 {
			return fmt.Errorf("implausible dictionary size %d", nv)
		}
		if err := c.decodeDictionary(data, int(hdr.DictionaryPageHeader.V.NumValues)); err != nil {
			return err
		}
		if c.exhausted() {
			return fmt.Errorf("dictionary page without data pages")
		}
		return c.decodePage() // continue to the first data page
	case format.DataPageV2:
		h := hdr.DataPageHeaderV2.V
		if h.RepetitionLevelsByteLength != 0 {
			return fmt.Errorf("unexpected repetition levels in flat column")
		}
		if h.NumValues < 0 || h.NumValues > 1<<28 || h.NumNulls < 0 || h.NumNulls > h.NumValues ||
			h.DefinitionLevelsByteLength < 0 || int(h.DefinitionLevelsByteLength) > len(body) {
			return fmt.Errorf("implausible v2 page header: values=%d nulls=%d defLen=%d",
				h.NumValues, h.NumNulls, h.DefinitionLevelsByteLength)
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
		if h.NumValues < 0 || h.NumValues > 1<<28 {
			return fmt.Errorf("implausible v1 page header: values=%d", h.NumValues)
		}
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
			if ln < 0 || ln > len(data)-4 {
				return fmt.Errorf("v1 level length %d overruns page (%d bytes)", ln, len(data))
			}
			defBytes = data[4 : 4+ln]
			data = data[4+ln:]
		}
		return c.decodeValues(data, int(h.NumValues), numNulls, defBytes, format.Encoding(h.Encoding))
	default:
		return fmt.Errorf("unsupported page type %d on fast path", hdr.Type)
	}
}

func (c *fastCursor) decompress(src []byte, uncompressedSize int) ([]byte, error) {
	out, err := decompressInto(c.codec, &c.scratch, src, uncompressedSize)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// decompressDict decompresses into the dictionary scratch buffer, which must
// outlive the whole chunk (dict strings alias it).
func (c *fastCursor) decompressDict(src []byte, uncompressedSize int) ([]byte, error) {
	return decompressInto(c.codec, &c.dictScratch, src, uncompressedSize)
}

func decompressInto(codec format.CompressionCodec, scratch *[]byte, src []byte, uncompressedSize int) ([]byte, error) {
	if cap(*scratch) < uncompressedSize {
		*scratch = make([]byte, uncompressedSize+64)
	}
	dst := (*scratch)[:uncompressedSize]
	switch codec {
	case format.Uncompressed:
		// copy: src aliases the sliding read window, which shifts on the
		// next page read while dictionaries (and batch views) must survive
		dst = dst[:len(src)]
		copy(dst, src)
		return dst, nil
	case format.Snappy:
		out, err := s2.Decode(dst, src)
		if err != nil {
			return nil, fmt.Errorf("snappy: %w", err)
		}
		*scratch = out[:cap(out)]
		return out, nil
	case format.Zstd:
		out, err := zstdDecoder.DecodeAll(src, dst[:0])
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		if cap(out) > cap(*scratch) {
			*scratch = out[:cap(out)]
		}
		return out, nil
	default:
		return nil, fmt.Errorf("codec %v not handled on fast path", codec)
	}
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

	c.idx = nil
	if enc == format.RLEDictionary || enc == format.PlainDictionary { //nolint:staticcheck // legacy files still use it
		return c.decodeDictIndices(values, numRows, dense)
	}

	switch c.physType {
	case format.Int64:
		if enc != format.Plain {
			return fmt.Errorf("unsupported INT64 encoding %v", enc)
		}
		vals, err := aliasInt64(values, dense, &c.i64own)
		if err != nil {
			return err
		}
		if div := c.conv.decDiv; div > 0 {
			c.f64 = growF(c.f64own[:0], numRows)
			c.f64own = c.f64
			c.expandDecimalI64(vals, numRows, div)
			return nil
		}
		if mul := c.conv.mulNum; mul != 1 {
			for i := range vals {
				vals[i] *= mul
			}
		} else if divi := c.conv.mulDen; divi != 1 {
			for i := range vals {
				vals[i] /= divi
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
		if div := c.conv.decDiv; div > 0 {
			c.f64 = growF(c.f64own[:0], numRows)
			c.f64own = c.f64
			out := c.f64
			vi := 0
			for i := 0; i < numRows; i++ {
				if c.nulls != nil && c.nulls[i] {
					out[i] = 0
					continue
				}
				out[i] = float64(int32(binary.LittleEndian.Uint32(values[4*vi:]))) / div
				vi++
			}
			return nil
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
	case format.Int96:
		if enc != format.Plain {
			return fmt.Errorf("unsupported INT96 encoding %v", enc)
		}
		if len(values) < 12*dense {
			return fmt.Errorf("short INT96 page")
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
			lo := binary.LittleEndian.Uint64(values[12*vi:])
			day := binary.LittleEndian.Uint32(values[12*vi+8:])
			out[i] = int96Micros(lo, day)
			vi++
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
		if enc == format.RLE {
			// boolean values as RLE/bit-packed hybrid, 4-byte length prefix
			if len(values) < 4 {
				return fmt.Errorf("short RLE BOOLEAN page")
			}
			ln := int(binary.LittleEndian.Uint32(values))
			if 4+ln > len(values) {
				return fmt.Errorf("RLE BOOLEAN length overruns page")
			}
			if cap(c.idxBuf) < dense {
				c.idxBuf = make([]int32, dense)
			}
			di := c.idxBuf[:dense]
			if err := decodeRLEHybrid32(values[4:4+ln], 1, di); err != nil {
				return err
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
				out[i] = int64(di[vi])
				vi++
			}
			return nil
		}
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
	// expand BACKWARD: dense may alias out (aliasInt64's copy path reuses
	// i64own), and back-to-front never overwrites unread dense entries
	vi := len(dense) - 1
	for i := numRows - 1; i >= 0; i-- {
		if c.nulls[i] {
			out[i] = 0
			continue
		}
		out[i] = dense[vi]
		vi--
	}
	return out
}

func (c *fastCursor) expandF64(dense []float64, numRows int) []float64 {
	if c.nulls == nil {
		return dense
	}
	out := growF(c.f64own[:0], numRows)
	c.f64own = out
	vi := len(dense) - 1
	for i := numRows - 1; i >= 0; i-- {
		if c.nulls[i] {
			out[i] = 0
			continue
		}
		out[i] = dense[vi]
		vi--
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
		// lengths come from DELTA_BINARY_PACKED, which is a signed
		// encoding: a corrupt page can hand back a negative length, and
		// off+ln would then pass the overrun check below and reach
		// unsafe.String with a negative length
		ln := int(lengths[vi])
		vi++
		if ln < 0 || off+ln > len(data) {
			return fmt.Errorf("byte array length %d overruns page", ln)
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

// expandDecimalI64 fills c.f64 (already sized to numRows) with dense unscaled
// int64 decimal values divided by div, expanded over the null mask.
func (c *fastCursor) expandDecimalI64(dense []int64, numRows int, div float64) {
	out := c.f64
	if c.nulls == nil {
		for i, v := range dense {
			out[i] = float64(v) / div
		}
		return
	}
	vi := 0
	for i := 0; i < numRows; i++ {
		if c.nulls[i] {
			out[i] = 0
			continue
		}
		out[i] = float64(dense[vi]) / div
		vi++
	}
}

// decodeDictionary decodes a PLAIN dictionary page into typed dict arrays,
// applying all value conversions (timestamp units, INT96, decimal scale)
// once per dictionary entry instead of once per row.
func (c *fastCursor) decodeDictionary(data []byte, n int) error {
	c.hasDict = true
	switch c.physType {
	case format.Int64:
		if len(data) < 8*n {
			return fmt.Errorf("short INT64 dictionary: %d bytes for %d values", len(data), n)
		}
		vals, err := aliasInt64(data, n, &c.dictI64)
		if err != nil {
			return err
		}
		if div := c.conv.decDiv; div > 0 {
			c.dictF64 = growF(c.dictF64[:0], n)
			for i, v := range vals {
				c.dictF64[i] = float64(v) / div
			}
			return nil
		}
		if mul := c.conv.mulNum; mul != 1 {
			for i := range vals {
				vals[i] *= mul
			}
		} else if divi := c.conv.mulDen; divi != 1 {
			for i := range vals {
				vals[i] /= divi
			}
		}
		c.dictI64 = vals
	case format.Int32:
		if len(data) < 4*n {
			return fmt.Errorf("short INT32 dictionary")
		}
		if div := c.conv.decDiv; div > 0 {
			c.dictF64 = growF(c.dictF64[:0], n)
			for i := 0; i < n; i++ {
				c.dictF64[i] = float64(int32(binary.LittleEndian.Uint32(data[4*i:]))) / div
			}
			return nil
		}
		c.dictI64 = grow(c.dictI64[:0], n)
		for i := 0; i < n; i++ {
			c.dictI64[i] = int64(int32(binary.LittleEndian.Uint32(data[4*i:])))
		}
	case format.Int96:
		if len(data) < 12*n {
			return fmt.Errorf("short INT96 dictionary")
		}
		c.dictI64 = grow(c.dictI64[:0], n)
		for i := 0; i < n; i++ {
			lo := binary.LittleEndian.Uint64(data[12*i:])
			day := binary.LittleEndian.Uint32(data[12*i+8:])
			c.dictI64[i] = int96Micros(lo, day)
		}
	case format.Double:
		vals, err := aliasFloat64(data, n, &c.dictF64)
		if err != nil {
			return err
		}
		c.dictF64 = vals
	case format.Float:
		if len(data) < 4*n {
			return fmt.Errorf("short FLOAT dictionary")
		}
		c.dictF64 = growF(c.dictF64[:0], n)
		for i := 0; i < n; i++ {
			c.dictF64[i] = float64(bitsToFloat32(binary.LittleEndian.Uint32(data[4*i:])))
		}
	case format.ByteArray:
		c.dictStr = growS(c.dictStr[:0], n)
		pos := 0
		for i := 0; i < n; i++ {
			if pos+4 > len(data) {
				return fmt.Errorf("short BYTE_ARRAY dictionary")
			}
			ln := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+ln > len(data) {
				return fmt.Errorf("dictionary entry overruns page")
			}
			if ln == 0 {
				c.dictStr[i] = ""
			} else {
				c.dictStr[i] = unsafe.String(&data[pos], ln)
			}
			pos += ln
		}
	default:
		return fmt.Errorf("dictionary for %v not handled on fast path", c.physType)
	}
	return nil
}

// decodeDictIndices decodes an RLE_DICTIONARY data page: one bit-width byte,
// then RLE/bit-packed indices. Numeric columns materialize through the dict
// (conversions were pre-applied); string columns stay as dict+indices so the
// engine can hash each dictionary entry once.
func (c *fastCursor) decodeDictIndices(values []byte, numRows, dense int) error {
	if !c.hasDict {
		return fmt.Errorf("dictionary-encoded page before dictionary page")
	}
	if len(values) < 1 {
		return fmt.Errorf("empty dictionary index page")
	}
	w := int(values[0])
	if cap(c.idxBuf) < dense {
		c.idxBuf = make([]int32, dense)
	}
	di := c.idxBuf[:dense]
	if err := decodeRLEHybrid32(values[1:], w, di); err != nil {
		return err
	}
	dictLen := len(c.dictStr) + len(c.dictI64) + len(c.dictF64)
	for _, ix := range di {
		if int(ix) >= dictLen || ix < 0 {
			return fmt.Errorf("dictionary index %d out of range (dict size %d)", ix, dictLen)
		}
	}

	switch c.typ {
	case TypeString, TypeBytes:
		// row-aligned indices; engine hashes dict entries once per batch
		c.idx = growI32(c.idxOwn[:0], numRows)
		c.idxOwn = c.idx
		out := c.idx
		vi := 0
		for i := 0; i < numRows; i++ {
			if c.nulls != nil && c.nulls[i] {
				out[i] = 0
				continue
			}
			out[i] = di[vi]
			vi++
		}
		c.str = nil
	case TypeFloat64:
		c.f64 = growF(c.f64own[:0], numRows)
		c.f64own = c.f64
		out := c.f64
		vi := 0
		for i := 0; i < numRows; i++ {
			if c.nulls != nil && c.nulls[i] {
				out[i] = 0
				continue
			}
			out[i] = c.dictF64[di[vi]]
			vi++
		}
	default:
		c.i64 = grow(c.i64own[:0], numRows)
		c.i64own = c.i64
		out := c.i64
		vi := 0
		for i := 0; i < numRows; i++ {
			if c.nulls != nil && c.nulls[i] {
				out[i] = 0
				continue
			}
			out[i] = c.dictI64[di[vi]]
			vi++
		}
	}
	return nil
}
