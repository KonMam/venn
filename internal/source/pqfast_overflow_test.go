package source

import (
	"encoding/binary"
	"strings"
	"testing"
)

// The parquet fast path decodes attacker-controlled sizes. Both cases below
// used to reach a runtime panic that the reader's recover boundary then
// laundered into "corrupt parquet data: runtime error: ...", which is not a
// clean error, and is exactly what the torture suite forbids. They are pinned
// here so a soak run is not what catches them.

// TestDefLevelRunOverflow: an RLE run length near 2^63 made row+run overflow
// the clamp, so the fill loop walked off the end of out.
func TestDefLevelRunOverflow(t *testing.T) {
	var src []byte
	src = binary.AppendUvarint(src, 1<<1) // RLE, run of 1: advances row to 1
	src = append(src, 1)
	src = binary.AppendUvarint(src, 1<<64-2) // RLE, run of 2^63-1
	src = append(src, 0)

	_, _, err := decodeDefLevels(src, 8, make([]bool, 8))
	if err == nil {
		t.Fatal("an over-declared definition-level run must be rejected")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want an over-declared-run error", err)
	}
}

// TestDefLevelGroupsOverflow: a bit-packed group count is bounded by the
// bytes actually present, so a huge varint errors instead of spinning.
func TestDefLevelGroupsOverflow(t *testing.T) {
	var src []byte
	src = binary.AppendUvarint(src, (1<<62)|1) // bit-packed, absurd group count
	src = append(src, 0xff)

	if _, _, err := decodeDefLevels(src, 8, make([]bool, 8)); err == nil {
		t.Fatal("an over-declared bit-packed group count must be rejected")
	}
}

// TestDefLevelsStillDecode keeps the guards from swallowing valid pages.
func TestDefLevelsStillDecode(t *testing.T) {
	// one RLE run of 4 non-null rows, then 4 nulls
	var src []byte
	src = binary.AppendUvarint(src, 4<<1)
	src = append(src, 1)
	src = binary.AppendUvarint(src, 4<<1)
	src = append(src, 0)

	nulls, n, err := decodeDefLevels(src, 8, make([]bool, 8))
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("nulls = %d, want 4", n)
	}
	for i := 0; i < 4; i++ {
		if nulls[i] {
			t.Fatalf("row %d marked null", i)
		}
	}
	for i := 4; i < 8; i++ {
		if !nulls[i] {
			t.Fatalf("row %d not marked null", i)
		}
	}
}

// TestDeltaLengthByteArrayNegativeLength: DELTA_BINARY_PACKED is a signed
// encoding, so a corrupt page can declare a negative string length. That
// passed the overrun check (off+ln stays inside the page) and reached
// unsafe.String, which panics on a negative length.
func TestDeltaLengthByteArrayNegativeLength(t *testing.T) {
	c := &fastCursor{}
	// DELTA_BINARY_PACKED header: block size 128, 1 miniblock, 2 values,
	// first value -4 (zigzag 7), then a miniblock of zero deltas
	var page []byte
	page = binary.AppendUvarint(page, 128) // block size
	page = binary.AppendUvarint(page, 1)   // miniblocks per block
	page = binary.AppendUvarint(page, 2)   // total values
	page = binary.AppendVarint(page, -4)   // first value
	page = binary.AppendVarint(page, 0)    // min delta
	page = append(page, 0)                 // bit width 0 for the miniblock
	page = append(page, "abcd"...)         // the byte-array data

	err := c.decodeDeltaLengthByteArray(page, 2, 2)
	if err == nil {
		t.Fatal("a negative byte-array length must be rejected")
	}
	if !strings.Contains(err.Error(), "overruns page") {
		t.Fatalf("err = %v, want an overrun error", err)
	}
}
