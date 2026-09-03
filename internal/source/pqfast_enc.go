package source

// Low-level decoders for the fast parquet path: definition levels
// (RLE/bit-packed hybrid at bit width 1), DELTA_BINARY_PACKED int32 (string
// lengths), and zero-copy aliasing of PLAIN numeric pages.

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

func bitsToFloat32(b uint32) float32 { return math.Float32frombits(b) }

func grow(s []int64, n int) []int64 {
	if cap(s) < n {
		return make([]int64, n)
	}
	return s[:n]
}

func growF(s []float64, n int) []float64 {
	if cap(s) < n {
		return make([]float64, n)
	}
	return s[:n]
}

func growS(s []string, n int) []string {
	if cap(s) < n {
		return make([]string, n)
	}
	return s[:n]
}

// aliasInt64 reinterprets b as n little-endian int64s. On aligned input this
// is a zero-copy cast; otherwise the values are copied into *own.
func aliasInt64(b []byte, n int, own *[]int64) ([]int64, error) {
	if len(b) < 8*n {
		return nil, fmt.Errorf("short INT64 page: %d bytes for %d values", len(b), n)
	}
	if n == 0 {
		return nil, nil
	}
	if uintptr(unsafe.Pointer(&b[0]))%8 == 0 {
		return unsafe.Slice((*int64)(unsafe.Pointer(&b[0])), n), nil
	}
	out := grow((*own)[:0], n)
	*own = out
	for i := range out {
		out[i] = int64(binary.LittleEndian.Uint64(b[8*i:]))
	}
	return out, nil
}

// aliasFloat64 is aliasInt64 for DOUBLE pages.
func aliasFloat64(b []byte, n int, own *[]float64) ([]float64, error) {
	if len(b) < 8*n {
		return nil, fmt.Errorf("short DOUBLE page: %d bytes for %d values", len(b), n)
	}
	if n == 0 {
		return nil, nil
	}
	if uintptr(unsafe.Pointer(&b[0]))%8 == 0 {
		return unsafe.Slice((*float64)(unsafe.Pointer(&b[0])), n), nil
	}
	out := growF((*own)[:0], n)
	*own = out
	for i := range out {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[8*i:]))
	}
	return out, nil
}

// decodeDefLevels decodes RLE/bit-packed hybrid definition levels at max
// level 1 into a null mask (level 0 = NULL). Returns the mask and the null
// count.
func decodeDefLevels(src []byte, numRows int, out []bool) ([]bool, int, error) {
	pos, row, nulls := 0, 0, 0
	for row < numRows {
		if pos >= len(src) {
			return nil, 0, fmt.Errorf("truncated definition levels: %d of %d rows", row, numRows)
		}
		h, k := binary.Uvarint(src[pos:])
		if k <= 0 {
			return nil, 0, fmt.Errorf("bad level varint")
		}
		pos += k
		if h&1 == 0 { // RLE run
			run := int(h >> 1)
			if pos >= len(src) {
				return nil, 0, fmt.Errorf("truncated RLE run")
			}
			v := src[pos] & 1
			pos++
			if row+run > numRows {
				run = numRows - row
			}
			if v == 0 {
				for i := 0; i < run; i++ {
					out[row+i] = true
				}
				nulls += run
			} else {
				for i := 0; i < run; i++ {
					out[row+i] = false
				}
			}
			row += run
		} else { // bit-packed groups of 8
			groups := int(h >> 1)
			for g := 0; g < groups; g++ {
				if pos >= len(src) {
					return nil, 0, fmt.Errorf("truncated bit-packed levels")
				}
				b := src[pos]
				pos++
				for bit := 0; bit < 8 && row < numRows; bit++ {
					isNull := b&(1<<bit) == 0
					out[row] = isNull
					if isNull {
						nulls++
					}
					row++
				}
			}
		}
	}
	return out, nulls, nil
}

// decodeDeltaBinaryPackedInt32 decodes a DELTA_BINARY_PACKED stream into
// out (whose length is the expected value count). Returns bytes consumed.
func decodeDeltaBinaryPackedInt32(src []byte, out []int32) (int, error) {
	pos := 0
	uv := func() (uint64, error) {
		v, k := binary.Uvarint(src[pos:])
		if k <= 0 {
			return 0, fmt.Errorf("bad varint in delta header")
		}
		pos += k
		return v, nil
	}
	sv := func() (int64, error) {
		v, k := binary.Varint(src[pos:])
		if k <= 0 {
			return 0, fmt.Errorf("bad zigzag varint in delta header")
		}
		pos += k
		return v, nil
	}

	blockSize, err := uv()
	if err != nil {
		return 0, err
	}
	numMini, err := uv()
	if err != nil {
		return 0, err
	}
	total, err := uv()
	if err != nil {
		return 0, err
	}
	first, err := sv()
	if err != nil {
		return 0, err
	}
	if int(total) != len(out) {
		return 0, fmt.Errorf("delta stream has %d values, want %d", total, len(out))
	}
	if numMini == 0 || blockSize%numMini != 0 {
		return 0, fmt.Errorf("bad delta block config %d/%d", blockSize, numMini)
	}
	perMini := int(blockSize / numMini)
	if perMini%8 != 0 {
		return 0, fmt.Errorf("miniblock size %d not a multiple of 8", perMini)
	}

	if len(out) == 0 {
		return pos, nil
	}
	out[0] = int32(first)
	value := first
	written := 1

	for written < len(out) {
		minDelta, err := sv()
		if err != nil {
			return 0, err
		}
		if pos+int(numMini) > len(src) {
			return 0, fmt.Errorf("truncated delta bit widths")
		}
		widths := src[pos : pos+int(numMini)]
		pos += int(numMini)
		for _, w := range widths {
			bytesLen := perMini * int(w) / 8
			if pos+bytesLen > len(src) {
				return 0, fmt.Errorf("truncated delta miniblock")
			}
			data := src[pos : pos+bytesLen]
			pos += bytesLen
			if written >= len(out) {
				continue // padding miniblocks after the last value
			}
			if w == 0 {
				for i := 0; i < perMini && written < len(out); i++ {
					value += minDelta
					out[written] = int32(value)
					written++
				}
				continue
			}
			// unpack perMini deltas of w bits, LSB-first
			var acc uint64
			var nbits uint
			di := 0
			mask := uint64(1)<<w - 1
			for i := 0; i < perMini && written < len(out); i++ {
				for nbits < uint(w) {
					acc |= uint64(data[di]) << nbits
					di++
					nbits += 8
				}
				d := acc & mask
				acc >>= w
				nbits -= uint(w)
				value += minDelta + int64(d)
				out[written] = int32(value)
				written++
			}
		}
	}
	return pos, nil
}
