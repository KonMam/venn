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

// floatQuantizer rounds floats to a decimal grid before hashing and
// comparing. Hash joins cannot honor an epsilon (equal-within-eps values
// must produce equal hashes), so tdiff quantizes instead: both sides round
// to the same grid, making the semantics exact and hash-consistent.
type floatQuantizer struct{ scale float64 }

func newFloatQuantizer(digits int) *floatQuantizer {
	return &floatQuantizer{scale: math.Pow(10, float64(digits))}
}

func (q *floatQuantizer) quantize(f float64) float64 {
	if q == nil {
		return f
	}
	r := math.RoundToEven(f*q.scale) / q.scale
	if math.IsInf(r, 0) || math.IsNaN(r) {
		return f // out-of-range values compare exactly
	}
	return r
}

func (q *floatQuantizer) canon(f float64) uint64 {
	if q != nil {
		f = q.quantize(f)
	}
	return canonFloat(f)
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
	return combineHashesQ(row, idx, modes, salts, nil)
}

func combineHashesQ(row []source.Value, idx []int, modes []compareMode, salts []uint64, quant *floatQuantizer) uint64 {
	var h uint64
	for i, ci := range idx {
		v := &row[ci]
		var bits uint64
		switch {
		case v.Null:
			bits = nullSentinel
		case modes[i] == modeBytes:
			bits = xxhash.Sum64String(v.Str)
		case modes[i] == modeFloat:
			f := v.Float
			if v.Type != source.TypeFloat64 {
				f = float64(v.Int)
			}
			bits = quant.canon(f)
		default:
			bits = uint64(v.Int)
		}
		h += mix64(bits ^ salts[i])
	}
	return h
}

// accumulateColumn adds the salted, mixed hash of each value of one column
// into the per-row accumulator lane. The loops read the typed column arrays
// directly; the no-nulls variants are branch-free per value and vectorize
// well. acc must have exactly the column's row count.
// dictMemo caches the salted-mixed hash of every dictionary entry for the
// dictionary currently flowing through one worker (batches from the same
// column chunk share the same Dict slice, so the hit rate is high).
type dictMemo struct {
	dict   *string // identity of the memoized dictionary (first element)
	salt   uint64
	hashes []uint64
}

func (m *dictMemo) hashesFor(dict []string, salt uint64) []uint64 {
	if len(dict) == 0 {
		return nil
	}
	if m.dict == &dict[0] && m.salt == salt {
		return m.hashes
	}
	if cap(m.hashes) < len(dict) {
		m.hashes = make([]uint64, len(dict))
	}
	m.hashes = m.hashes[:len(dict)]
	for i, s := range dict {
		m.hashes[i] = mix64(xxhash.Sum64String(s) ^ salt)
	}
	m.dict, m.salt = &dict[0], salt
	return m.hashes
}

func accumulateColumn(col *source.Col, mode compareMode, salt uint64, acc []uint64) {
	accumulateColumnMemo(col, mode, salt, acc, nil, nil)
}

func accumulateColumnMemo(col *source.Col, mode compareMode, salt uint64, acc []uint64, memo *dictMemo, quant *floatQuantizer) {
	nulls := col.Nulls
	switch mode {
	case modeInt:
		vals := col.I64
		if nulls == nil {
			for r, v := range vals {
				acc[r] += mix64(uint64(v) ^ salt)
			}
			return
		}
		for r, v := range vals {
			bits := uint64(v)
			if nulls[r] {
				bits = nullSentinel
			}
			acc[r] += mix64(bits ^ salt)
		}
	case modeFloat:
		if col.Type == source.TypeFloat64 {
			vals := col.F64
			if nulls == nil {
				if quant == nil {
					for r, v := range vals {
						acc[r] += mix64(canonFloat(v) ^ salt)
					}
					return
				}
				for r, v := range vals {
					acc[r] += mix64(quant.canon(v) ^ salt)
				}
				return
			}
			for r, v := range vals {
				bits := quant.canon(v)
				if nulls[r] {
					bits = nullSentinel
				}
				acc[r] += mix64(bits ^ salt)
			}
			return
		}
		// int64 column coerced into the float comparison domain
		vals := col.I64
		if nulls == nil {
			for r, v := range vals {
				acc[r] += mix64(quant.canon(float64(v)) ^ salt)
			}
			return
		}
		for r, v := range vals {
			bits := quant.canon(float64(v))
			if nulls[r] {
				bits = nullSentinel
			}
			acc[r] += mix64(bits ^ salt)
		}
	default: // modeBytes
		if col.Idx != nil {
			// dictionary column: hash each dict entry once, rows are lookups
			var local dictMemo
			if memo == nil {
				memo = &local
			}
			dh := memo.hashesFor(col.Dict, salt)
			nullMixed := mix64(nullSentinel ^ salt)
			if nulls == nil {
				for r, ix := range col.Idx {
					acc[r] += dh[ix]
				}
				return
			}
			for r, ix := range col.Idx {
				if nulls[r] {
					acc[r] += nullMixed
					continue
				}
				acc[r] += dh[ix]
			}
			return
		}
		vals := col.Str
		if nulls == nil {
			for r := range vals {
				acc[r] += mix64(xxhash.Sum64String(vals[r]) ^ salt)
			}
			return
		}
		for r := range vals {
			var bits uint64 = nullSentinel
			if !nulls[r] {
				bits = xxhash.Sum64String(vals[r])
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
	return valuesEqualQ(a, b, mode, nil)
}

// valuesEqualQ is valuesEqual with float quantization.
func valuesEqualQ(a, b *source.Value, mode compareMode, quant *floatQuantizer) bool {
	if a.Null || b.Null {
		return a.Null == b.Null
	}
	if mode == modeBytes {
		return a.Str == b.Str
	}
	if mode == modeFloat && quant != nil {
		fa, fb := a.Float, b.Float
		if a.Type != source.TypeFloat64 {
			fa = float64(a.Int)
		}
		if b.Type != source.TypeFloat64 {
			fb = float64(b.Int)
		}
		return quant.canon(fa) == quant.canon(fb)
	}
	return valueBits(a, mode) == valueBits(b, mode)
}
