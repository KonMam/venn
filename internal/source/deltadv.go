package source

// Delta deletion vector loading. A DV names deleted row positions of one
// data file as a roaring bitmap array (Delta protocol "portable" format:
// int32 LE magic, int64 LE bitmap count, then per bitmap an int32 LE key of
// the high 32 bits plus a standard 32-bit roaring bitmap). Storage: inline
// in the log (z85), or in a .bin file (version byte, then per DV an int32 BE
// length, the bitmap data, and an int32 BE CRC32).

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const deltaDVMagic = 1681511377

// z85 is the ZeroMQ base85 alphabet used by Delta for UUIDs and inline DVs.
const z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

var z85Dec = func() (t [256]int16) {
	for i := range t {
		t[i] = -1
	}
	for i := 0; i < len(z85Alphabet); i++ {
		t[z85Alphabet[i]] = int16(i)
	}
	return
}()

func z85Decode(s string) ([]byte, error) {
	if len(s)%5 != 0 {
		return nil, fmt.Errorf("z85: length %d not a multiple of 5", len(s))
	}
	out := make([]byte, 0, len(s)/5*4)
	for i := 0; i < len(s); i += 5 {
		var v uint64
		for j := 0; j < 5; j++ {
			d := z85Dec[s[i+j]]
			if d < 0 {
				return nil, fmt.Errorf("z85: bad character %q", s[i+j])
			}
			v = v*85 + uint64(d)
		}
		if v > 0xFFFFFFFF {
			return nil, fmt.Errorf("z85: group overflow")
		}
		out = binary.BigEndian.AppendUint32(out, uint32(v))
	}
	return out, nil
}

// parseDeltaDVData decodes the bitmap data (magic + roaring bitmap array).
// maxCard bounds the total number of positions accepted (the descriptor's
// cardinality): a hostile run container can declare billions of positions in
// a few bytes, so the bound is enforced per bitmap, before materializing.
func parseDeltaDVData(data []byte, maxCard int64) (*roaring64.Bitmap, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("deletion vector too short (%d bytes)", len(data))
	}
	if m := binary.LittleEndian.Uint32(data); m != deltaDVMagic {
		return nil, fmt.Errorf("bad deletion vector magic %d", m)
	}
	rd := bytes.NewReader(data[4:])
	var n uint64
	if err := binary.Read(rd, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	out := roaring64.New()
	var total uint64
	for i := uint64(0); i < n; i++ {
		var key uint32
		if err := binary.Read(rd, binary.LittleEndian, &key); err != nil {
			return nil, fmt.Errorf("deletion vector bitmap %d: %w", i, err)
		}
		bm := roaring.New()
		if _, err := bm.ReadFrom(rd); err != nil {
			return nil, fmt.Errorf("deletion vector bitmap %d: %w", i, err)
		}
		// ReadFrom accepts some internally inconsistent containers (e.g. a
		// malformed run container) that panic when iterated; Validate is the
		// library's post-deserialization guard.
		if err := bm.Validate(); err != nil {
			return nil, fmt.Errorf("deletion vector bitmap %d: %w", i, err)
		}
		total += bm.GetCardinality()
		if maxCard >= 0 && total > uint64(maxCard) {
			return nil, fmt.Errorf("deletion vector declares more than %d positions (descriptor cardinality)", maxCard)
		}
		hi := uint64(key) << 32
		it := bm.Iterator()
		for it.HasNext() {
			out.Add(hi | uint64(it.Next()))
		}
	}
	return out, nil
}

// loadDeltaDV materializes one deletion vector descriptor.
func loadDeltaDV(fs tableFS, dir string, dv *deltaDV) (*roaring64.Bitmap, error) {
	var data []byte
	switch dv.StorageType {
	case "i": // inline
		raw, err := z85Decode(dv.PathOrInlineDv)
		if err != nil {
			return nil, err
		}
		data = raw
	case "u", "p":
		var path string
		if dv.StorageType == "p" {
			path = dv.PathOrInlineDv
			if !isRemote(path) && !bytes.HasPrefix([]byte(path), []byte("/")) {
				path = joinPath(dir, path)
			}
		} else {
			// pathOrInlineDv = <random prefix><z85 of 16-byte uuid>
			enc := dv.PathOrInlineDv
			if len(enc) < 20 {
				return nil, fmt.Errorf("bad DV uuid encoding %q", enc)
			}
			prefix, uuidEnc := enc[:len(enc)-20], enc[len(enc)-20:]
			raw, err := z85Decode(uuidEnc)
			if err != nil {
				return nil, err
			}
			u := fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
			name := "deletion_vector_" + u + ".bin"
			if prefix != "" {
				path = joinPath(dir, prefix, name)
			} else {
				path = joinPath(dir, name)
			}
		}
		raw, err := fs.ReadFile(path)
		if err != nil {
			return nil, err
		}
		off := dv.Offset
		if off < 0 || off+4 > int64(len(raw)) {
			return nil, fmt.Errorf("%s: DV offset %d beyond file (%d bytes)", path, off, len(raw))
		}
		size := int64(binary.BigEndian.Uint32(raw[off:]))
		if size != dv.SizeInBytes {
			return nil, fmt.Errorf("%s: DV size %d != descriptor sizeInBytes %d", path, size, dv.SizeInBytes)
		}
		if off+4+size+4 > int64(len(raw)) {
			return nil, fmt.Errorf("%s: DV data truncated", path)
		}
		data = raw[off+4 : off+4+size]
		want := binary.BigEndian.Uint32(raw[off+4+size:])
		if got := crc32.ChecksumIEEE(data); got != want {
			return nil, fmt.Errorf("%s: DV checksum mismatch", path)
		}
	default:
		return nil, fmt.Errorf("unknown DV storage type %q", dv.StorageType)
	}
	bm, err := parseDeltaDVData(data, dv.Cardinality)
	if err != nil {
		return nil, err
	}
	if got := int64(bm.GetCardinality()); got != dv.Cardinality {
		return nil, fmt.Errorf("DV cardinality %d != descriptor %d", got, dv.Cardinality)
	}
	return bm, nil
}
