package diff

import (
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/KonMam/venn/internal/source"
)

// Micro-benchmarks for the engine hot paths, tracked with benchstat across
// optimizations (full hyperfine suite runs only at release points).

func BenchmarkKeyTableInsert(b *testing.B) {
	t := newKeyTable(b.N)
	r := rand.New(rand.NewPCG(1, 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.insert(mixKeyHash(r.Uint64()), r.Uint64())
	}
}

func BenchmarkKeyTableProbe(b *testing.B) {
	const n = 1 << 20
	t := newKeyTable(n)
	r := rand.New(rand.NewPCG(1, 2))
	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = mixKeyHash(r.Uint64())
		t.insert(keys[i], uint64(i))
	}
	t.seal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.probe(keys[i&(n-1)])
	}
}

func benchColumn(n int, mode compareMode) *source.Col {
	r := rand.New(rand.NewPCG(3, 4))
	col := &source.Col{}
	switch mode {
	case modeInt:
		col.Type = source.TypeInt64
		col.I64 = make([]int64, n)
		for i := range col.I64 {
			col.I64[i] = int64(r.Uint64() % 1e6)
		}
	case modeFloat:
		col.Type = source.TypeFloat64
		col.F64 = make([]float64, n)
		for i := range col.F64 {
			col.F64[i] = r.Float64() * 1e6
		}
	default:
		col.Type = source.TypeString
		col.Str = make([]string, n)
		for i := range col.Str {
			col.Str[i] = "somewhat-long-string-value-12345"
		}
	}
	return col
}

func BenchmarkAccumulateInt(b *testing.B)    { benchAccumulate(b, modeInt) }
func BenchmarkAccumulateFloat(b *testing.B)  { benchAccumulate(b, modeFloat) }
func BenchmarkAccumulateString(b *testing.B) { benchAccumulate(b, modeBytes) }

func benchAccumulate(b *testing.B, mode compareMode) {
	const rows = 4096
	col := benchColumn(rows, mode)
	acc := make([]uint64, rows)
	b.SetBytes(rows * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		accumulateColumnMemo(col, mode, 0x9e3779b97f4a7c15, acc, nil, nil)
	}
}

// Normalized string hashing is the one enabled-path cost worth tracking:
// --ignore-case and --trim run per value, on the hottest column type.
func BenchmarkAccumulateStringNorm(b *testing.B) {
	variants := []struct {
		name string
		norm *normalizer
	}{
		{"exact", nil},
		{"trim", &normalizer{trim: true}},
		{"fold", &normalizer{fold: true}},
		{"trim+fold", &normalizer{trim: true, fold: true}},
	}
	const rows = 4096
	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			col := benchColumn(rows, modeBytes)
			acc := make([]uint64, rows)
			b.SetBytes(rows * 8)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				accumulateColumnMemo(col, modeBytes, 0x9e3779b97f4a7c15, acc, nil, v.norm)
			}
		})
	}
}

// BenchmarkFoldedHash isolates the fold itself across the shapes that take
// different paths: all-lower ASCII (the common case), mixed-case ASCII, and
// non-ASCII (which allocates).
func BenchmarkFoldedHash(b *testing.B) {
	cases := []struct{ name, s string }{
		{"lower", "somewhat-long-string-value-12345"},
		{"mixed", "Somewhat-Long-String-Value-12345"},
		{"unicode", "sömewhat-löng-string-value-12345"},
		{"short", "abc"},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(int64(len(c.s)))
			var sink uint64
			for i := 0; i < b.N; i++ {
				sink += foldedHash(c.s)
			}
			runtime.KeepAlive(sink)
		})
	}
}
