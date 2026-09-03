package diff

import (
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
// the given mode (modeBytes values hash their string payload instead).
func valueBits(v *source.Value, mode compareMode) uint64 {
	if mode == modeFloat {
		if v.Type == source.TypeFloat64 {
			return canonFloat(v.Float)
		}
		return canonFloat(float64(v.Int)) // int64 column coerced to float domain
	}
	return uint64(v.Int)
}

// mix64 is the splitmix64 finalizer: a fast, high-quality 64-bit permutation.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

const nullSentinel = 0x9e3779b97f4a7c15 // hashed in place of a NULL's payload

// hashValue produces the canonical 64-bit hash of one value under a mode.
func hashValue(v *source.Value, mode compareMode) uint64 {
	if v.Null {
		return nullSentinel
	}
	if mode == modeBytes {
		return xxhash.Sum64String(v.Str)
	}
	return valueBits(v, mode)
}

// combineHashes folds per-column value hashes into one row hash. Each column
// gets a distinct salt, the salted value hash is passed through mix64, and
// the results are summed: addition commutes, but the salts pin each value to
// its column, so "a,b" and "b,a" still hash differently.
func combineHashes(row []source.Value, idx []int, modes []compareMode, salts []uint64) uint64 {
	var h uint64
	for i, ci := range idx {
		h += mix64(hashValue(&row[ci], modes[i]) ^ salts[i])
	}
	return h
}

// accumulateColumn adds the salted, mixed hash of each value of one column
// into the per-row accumulator lane. Mode-specialized inner loops keep the
// common numeric cases branch-free per value.
func accumulateColumn(col []source.Value, mode compareMode, salt uint64, acc []uint64) {
	switch mode {
	case modeInt:
		for r := range col {
			v := &col[r]
			bits := uint64(v.Int)
			if v.Null {
				bits = nullSentinel
			}
			acc[r] += mix64(bits ^ salt)
		}
	case modeFloat:
		for r := range col {
			v := &col[r]
			var bits uint64
			switch {
			case v.Null:
				bits = nullSentinel
			case v.Type == source.TypeFloat64:
				bits = canonFloat(v.Float)
			default:
				bits = canonFloat(float64(v.Int))
			}
			acc[r] += mix64(bits ^ salt)
		}
	default: // modeBytes
		for r := range col {
			v := &col[r]
			var bits uint64 = nullSentinel
			if !v.Null {
				bits = xxhash.Sum64String(v.Str)
			}
			acc[r] += mix64(bits ^ salt)
		}
	}
}

// makeSalts derives one salt per column from a domain tag.
func makeSalts(n int, domain uint64) []uint64 {
	salts := make([]uint64, n)
	x := domain*0x9e3779b97f4a7c15 + 0x243f6a8885a308d3
	for i := range salts {
		x += 0x9e3779b97f4a7c15
		salts[i] = mix64(x)
	}
	return salts
}

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
