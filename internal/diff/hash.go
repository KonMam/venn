package diff

import (
	"encoding/binary"
	"math"

	"github.com/cespare/xxhash/v2"

	"tdiff/internal/source"
)

// compareMode is the canonical comparison domain chosen per column pair.
// It resolves the cross-format problem once, up front: a parquet INT64 and a
// CSV-inferred float64 column both hash and compare in modeFloat, so equal
// logical values collide regardless of the physical source type.
type compareMode uint8

const (
	modeInt compareMode = iota // int64, timestamp (µs), date (days), bool (0/1)
	modeFloat
	modeBytes // string and raw bytes, compared as raw bytes
)

// resolveMode picks the comparison domain for a column present in both
// sources with (possibly different but comparable) logical types.
func resolveMode(a, b source.Type) compareMode {
	if a == source.TypeFloat64 || b == source.TypeFloat64 {
		return modeFloat
	}
	switch a {
	case source.TypeString, source.TypeBytes:
		return modeBytes
	}
	return modeInt
}

// canonFloat collapses representations that must compare equal: -0 → +0 and
// every NaN → one quiet NaN bit pattern (identical inputs must diff as
// identical, so NaN == NaN here).
func canonFloat(f float64) uint64 {
	if f == 0 {
		return 0
	}
	if math.IsNaN(f) {
		return 0x7ff8000000000001
	}
	return math.Float64bits(f)
}

// valueBits returns the canonical 64-bit payload of a non-null value under
// the given mode (modeBytes values are hashed from Str instead).
func valueBits(v *source.Value, mode compareMode) uint64 {
	if mode == modeFloat {
		if v.Type == source.TypeFloat64 {
			return canonFloat(v.Float)
		}
		return canonFloat(float64(v.Int)) // int64 column coerced to float domain
	}
	return uint64(v.Int)
}

// hasher accumulates one row's canonical bytes and produces a 64-bit hash.
type hasher struct {
	d   xxhash.Digest
	buf [10]byte
}

func (h *hasher) reset() { h.d.Reset() }

func (h *hasher) writeValue(v *source.Value, mode compareMode) {
	if v.Null {
		h.buf[0] = 0xFF
		h.d.Write(h.buf[:1])
		return
	}
	switch mode {
	case modeBytes:
		binary.LittleEndian.PutUint64(h.buf[1:9], uint64(len(v.Str)))
		h.buf[0] = 0x01
		h.d.Write(h.buf[:9])
		h.d.WriteString(v.Str)
	default:
		h.buf[0] = 0x02
		binary.LittleEndian.PutUint64(h.buf[1:9], valueBits(v, mode))
		h.d.Write(h.buf[:9])
	}
}

func (h *hasher) sum() uint64 { return h.d.Sum64() }

// mixKeyHash post-processes a key hash so that 0 (the table's empty-slot
// sentinel) never appears.
func mixKeyHash(h uint64) uint64 {
	if h == 0 {
		return 1
	}
	return h
}

// valuesEqual compares two non-null canonical values under a mode.
func valuesEqual(a, b *source.Value, mode compareMode) bool {
	if a.Null || b.Null {
		return a.Null == b.Null
	}
	if mode == modeBytes {
		return a.Str == b.Str
	}
	return valueBits(a, mode) == valueBits(b, mode)
}
