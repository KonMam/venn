package source

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

// dvPart is one bitmap of the portable DV format: the high-32-bits key and
// the low-32-bit positions it holds.
type dvPart struct {
	key  uint32
	bits []uint32
}

// buildDVData serializes bitmaps in the Delta portable format parsed by
// parseDeltaDVData: int32 LE magic, int64 LE bitmap count, then per bitmap an
// int32 LE key plus a standard 32-bit roaring bitmap.
func buildDVData(parts []dvPart) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(deltaDVMagic))
	binary.Write(&buf, binary.LittleEndian, uint64(len(parts)))
	for _, p := range parts {
		binary.Write(&buf, binary.LittleEndian, p.key)
		bm := roaring.New()
		bm.AddMany(p.bits)
		b, err := bm.ToBytes()
		if err != nil {
			panic(err)
		}
		buf.Write(b)
	}
	return buf.Bytes()
}

func TestZ85Decode(t *testing.T) {
	// The ZeroMQ Z85 reference vector: "HelloWorld" is these 8 bytes.
	got, err := z85Decode("HelloWorld")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B}
	if !bytes.Equal(got, want) {
		t.Errorf("z85Decode(HelloWorld) = %x, want %x", got, want)
	}

	// All-zero group.
	got, err = z85Decode("00000")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte{0, 0, 0, 0}) {
		t.Errorf("z85Decode(00000) = %x, want 00000000", got)
	}

	// Length not a multiple of 5.
	if _, err := z85Decode("abcd"); err == nil {
		t.Error("z85Decode accepted length 4")
	}
	// Character outside the alphabet.
	if _, err := z85Decode("abcd~"); err == nil {
		t.Error("z85Decode accepted '~'")
	}
	// Group value above 2^32-1 ('#' is the largest digit).
	if _, err := z85Decode("#####"); err == nil {
		t.Error("z85Decode accepted overflowing group")
	}
}

func TestParseDeltaDVData(t *testing.T) {
	data := buildDVData([]dvPart{
		{key: 0, bits: []uint32{1, 5, 100}},
		{key: 1, bits: []uint32{3}},
	})
	bm, err := parseDeltaDVData(data, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got := bm.GetCardinality(); got != 4 {
		t.Fatalf("cardinality = %d, want 4", got)
	}
	for _, pos := range []uint64{1, 5, 100, 1<<32 | 3} {
		if !bm.Contains(pos) {
			t.Errorf("missing position %d", pos)
		}
	}
	if bm.Contains(3) {
		t.Error("position 3 leaked from the key=1 bitmap into the low range")
	}
}

func TestParseDeltaDVDataErrors(t *testing.T) {
	valid := buildDVData([]dvPart{{key: 0, bits: []uint32{1, 2, 3}}})

	// Too short.
	if _, err := parseDeltaDVData(valid[:8], -1); err == nil {
		t.Error("accepted 8-byte input")
	}
	// Bad magic.
	bad := append([]byte(nil), valid...)
	bad[0] ^= 0xFF
	if _, err := parseDeltaDVData(bad, -1); err == nil {
		t.Error("accepted bad magic")
	}
	// Truncated bitmap payload.
	if _, err := parseDeltaDVData(valid[:len(valid)-3], -1); err == nil {
		t.Error("accepted truncated bitmap")
	}
	// Declared bitmap missing entirely.
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(deltaDVMagic))
	binary.Write(&buf, binary.LittleEndian, uint64(1))
	if _, err := parseDeltaDVData(buf.Bytes(), -1); err == nil {
		t.Error("accepted missing bitmap")
	}
}

func TestParseDeltaDVDataMaxCard(t *testing.T) {
	bits := make([]uint32, 10)
	for i := range bits {
		bits[i] = uint32(i * 7)
	}
	data := buildDVData([]dvPart{{key: 0, bits: bits}})

	// Exactly at the bound: fine.
	if _, err := parseDeltaDVData(data, 10); err != nil {
		t.Errorf("maxCard=10 rejected 10 positions: %v", err)
	}
	// Above the bound: rejected before materializing.
	_, err := parseDeltaDVData(data, 5)
	if err == nil {
		t.Fatal("maxCard=5 accepted 10 positions")
	}
	if !strings.Contains(err.Error(), "positions") {
		t.Errorf("unexpected error: %v", err)
	}
	// -1 disables the bound.
	if _, err := parseDeltaDVData(data, -1); err != nil {
		t.Errorf("maxCard=-1 rejected valid data: %v", err)
	}

	// A run container declares many positions in a few bytes; the bound must
	// trip on declared cardinality, not input size.
	bm := roaring.New()
	bm.AddRange(0, 1_000_000)
	bm.RunOptimize()
	b, err := bm.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(deltaDVMagic))
	binary.Write(&buf, binary.LittleEndian, uint64(1))
	binary.Write(&buf, binary.LittleEndian, uint32(0))
	buf.Write(b)
	if _, err := parseDeltaDVData(buf.Bytes(), 1000); err == nil {
		t.Error("maxCard=1000 accepted a 1M-position run container")
	}
}
