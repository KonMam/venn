package diff

import (
	"math"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/KonMam/venn/internal/source"
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
// must produce equal hashes), so venn quantizes instead: both sides round
// to the same grid, making the semantics exact and hash-consistent.
type floatQuantizer struct {
	scale  float64
	digits int
}

func newFloatQuantizer(digits int) *floatQuantizer {
	return &floatQuantizer{scale: math.Pow(10, float64(digits)), digits: digits}
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
func valueBits(v *source.Value, mode compareMode, n *normalizer) uint64 {
	if mode == modeFloat {
		if v.Type == source.TypeFloat64 {
			return n.canonF(v.Float)
		}
		return n.canonF(float64(v.Int)) // int64 column coerced to float domain
	}
	return uint64(n.canonI(v.Type, v.Int))
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

// combineHashes folds per-column value hashes into one row hash. Each
// column gets a distinct salt, the salted value hash is passed through
// mix64, and the results are summed: addition commutes, but the salts pin
// each value to its column, so "a,b" and "b,a" still hash differently.
// norm, when non-nil, canonicalizes values before hashing (see normalize.go).
func combineHashes(row []source.Value, idx []int, modes []compareMode, salts []uint64, norm *normalizer) uint64 {
	var h uint64
	for i, ci := range idx {
		v := &row[ci]
		var bits uint64
		switch {
		case v.Null:
			bits = nullSentinel
		case modes[i] == modeBytes:
			bits = norm.hashString(v.Str)
		default:
			bits = valueBits(v, modes[i], norm)
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

func (m *dictMemo) hashesFor(dict []string, salt uint64, norm *normalizer) []uint64 {
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
		m.hashes[i] = mix64(norm.hashString(s) ^ salt)
	}
	m.dict, m.salt = &dict[0], salt
	return m.hashes
}

func accumulateColumnMemo(col *source.Col, mode compareMode, salt uint64, acc []uint64, memo *dictMemo, norm *normalizer) {
	nulls := col.Nulls
	switch mode {
	case modeInt:
		vals := col.I64
		if norm != nil && norm.tsUnit > 1 && col.Type == source.TypeTimestamp {
			unit := norm.tsUnit
			for r, v := range vals {
				bits := uint64(floorDiv(v, unit) * unit)
				if nulls != nil && nulls[r] {
					bits = nullSentinel
				}
				acc[r] += mix64(bits ^ salt)
			}
			return
		}
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
		quant := norm.quantizer()
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
			dh := memo.hashesFor(col.Dict, salt, norm)
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
		// the normalized paths are specialized rather than routed through
		// norm.hashString: this loop runs once per value of the hottest
		// column type, and the per-value branch on which normalization is
		// active is pure overhead once the run has started
		if norm != nil && norm.fold {
			trim := norm.trim
			for r := range vals {
				var bits uint64 = nullSentinel
				if nulls == nil || !nulls[r] {
					s := vals[r]
					if trim {
						s = strings.TrimSpace(s)
					}
					bits = foldedHash(s)
				}
				acc[r] += mix64(bits ^ salt)
			}
			return
		}
		if norm != nil && norm.trim {
			for r := range vals {
				var bits uint64 = nullSentinel
				if nulls == nil || !nulls[r] {
					bits = xxhash.Sum64String(strings.TrimSpace(vals[r]))
				}
				acc[r] += mix64(bits ^ salt)
			}
			return
		}
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

// quantizer returns the float quantizer, if any (nil-safe).
func (n *normalizer) quantizer() *floatQuantizer {
	if n == nil {
		return nil
	}
	return n.quant
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

// valuesEqual compares two canonical values under a mode, applying the same
// normalization the hash path applies (see normalize.go). It must agree with
// combineHashes: the hash decides the join, this decides the attribution.
func valuesEqual(a, b *source.Value, mode compareMode, norm *normalizer) bool {
	if a.Null || b.Null {
		return a.Null == b.Null
	}
	if mode == modeBytes {
		if norm == nil {
			return a.Str == b.Str
		}
		return norm.canonStr(a.Str) == norm.canonStr(b.Str)
	}
	return valueBits(a, mode, norm) == valueBits(b, mode, norm)
}
