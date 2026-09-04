package source

import (
	"os"
	"path/filepath"
	"testing"
)

// The kernels parse untrusted bytes with unsafe casts; every decoder must be
// panic-free on arbitrary input (errors are fine, crashes are not).

func FuzzDecodeRLEHybrid32(f *testing.F) {
	f.Add([]byte{0x03, 0x01}, 1, 8)
	f.Add([]byte{0x02, 0xFF, 0x01}, 8, 3)
	f.Fuzz(func(t *testing.T, src []byte, bitWidth, n int) {
		if n < 0 || n > 1<<16 || bitWidth < 0 || bitWidth > 64 {
			return
		}
		out := make([]int32, n)
		_ = decodeRLEHybrid32(src, bitWidth, out)
	})
}

func FuzzDecodeDefLevels(f *testing.F) {
	f.Add([]byte{0x03, 0x01}, 8)
	f.Fuzz(func(t *testing.T, src []byte, n int) {
		if n < 0 || n > 1<<16 {
			return
		}
		_, _, _ = decodeDefLevels(src, n, make([]bool, n))
	})
}

func FuzzDecodeDeltaBinaryPacked(f *testing.F) {
	f.Add([]byte{128, 1, 4, 5, 2, 2, 0, 0, 0, 0}, 5)
	f.Fuzz(func(t *testing.T, src []byte, n int) {
		if n < 0 || n > 1<<16 {
			return
		}
		out := make([]int32, n)
		_, _ = decodeDeltaBinaryPackedInt32(src, out)
	})
}

// FuzzOpenParquet feeds whole files (seeded with the interop corpus) through
// open + full scan. No input may panic.
func FuzzOpenParquet(f *testing.F) {
	seeds, _ := filepath.Glob("../../testdata/interop/*.parquet")
	for _, s := range seeds {
		if b, err := os.ReadFile(s); err == nil && len(b) < 4<<20 {
			f.Add(b)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "f.parquet")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Skip()
		}
		src, err := OpenParquet(path)
		if err != nil {
			return
		}
		defer src.Close()
		_ = src.(*parquetSource).ScanBatches(2, func() (BatchFunc, error) {
			return func(b *Batch) error {
				for ci := range b.Cols {
					for r := 0; r < b.N; r++ {
						_ = b.Cols[ci].Value(r)
					}
				}
				return nil
			}, nil
		})
	})
}

// FuzzOpenCSV feeds arbitrary bytes through CSV inference + both scan paths.
func FuzzOpenCSV(f *testing.F) {
	f.Add([]byte("id,a,b\n1,2.5,x\n2,,y\n"))
	f.Add([]byte("id,s\n1,\"a,b\"\n2,\"q\"\"q\"\n"))
	f.Add([]byte("id\r\n1\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "f.csv")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Skip()
		}
		src, err := OpenCSV(path, ',')
		if err != nil {
			return
		}
		defer src.Close()
		_ = src.(*csvSource).ScanBatches(2, func() (BatchFunc, error) {
			return func(b *Batch) error {
				for ci := range b.Cols {
					for r := 0; r < b.N; r++ {
						_ = b.Cols[ci].Value(r)
					}
				}
				return nil
			}, nil
		})
	})
}
