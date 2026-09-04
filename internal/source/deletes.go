package source

// Merge-on-read delete filtering. Iceberg position/equality deletes and
// Delta deletion vectors both reduce to: while scanning one parquet data
// file, drop rows by file-relative position (a roaring bitmap) and/or by
// value match on a column subset (equality deletes). Filtering happens
// inside the parquet scan where row positions are known, batch-at-a-time;
// batches with no deleted rows pass through untouched (zero cost for the
// overwhelmingly common case).

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// readPositionDeletes reads an Iceberg position-delete parquet file
// (file_path, pos columns) into per-data-file row bitmaps, with recorded
// paths passed through resolve.
func readPositionDeletes(path string, resolve func(string) string) (map[string]*roaring64.Bitmap, error) {
	src, err := OpenWith(path, Options{})
	if err != nil {
		return nil, err
	}
	defer src.Close()
	sch := src.Schema()
	fpIdx, posIdx := sch.ColumnIndex("file_path"), sch.ColumnIndex("pos")
	if fpIdx < 0 || posIdx < 0 {
		return nil, fmt.Errorf("%s: not a position delete file (missing file_path/pos columns)", path)
	}
	out := map[string]*roaring64.Bitmap{}
	var mu sync.Mutex
	err = Scan(src, 2, func() (BatchFunc, error) {
		return func(b *Batch) error {
			fp, pc := &b.Cols[fpIdx], &b.Cols[posIdx]
			mu.Lock()
			defer mu.Unlock()
			for r := 0; r < b.N; r++ {
				// batch strings may alias reused buffers, so the map key
				// must own its bytes
				p := strings.Clone(resolve(fp.Value(r).Str))
				bm := out[p]
				if bm == nil {
					bm = roaring64.New()
					out[p] = bm
				}
				bm.Add(uint64(pc.Value(r).Int))
			}
			return nil
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return out, nil
}

// readEqualityDeletes reads an Iceberg equality-delete parquet file into a
// value-match set over the named columns.
func readEqualityDeletes(path string, cols []string) (*eqSet, error) {
	src, err := OpenWith(path, Options{})
	if err != nil {
		return nil, err
	}
	defer src.Close()
	sch := src.Schema()
	idx := make([]int, len(cols))
	for i, c := range cols {
		if idx[i] = sch.ColumnIndex(c); idx[i] < 0 {
			return nil, fmt.Errorf("%s: equality delete file lacks column %q", path, c)
		}
	}
	set := map[string]struct{}{}
	var mu sync.Mutex
	err = Scan(src, 2, func() (BatchFunc, error) {
		var buf []byte
		return func(b *Batch) error {
			mu.Lock()
			defer mu.Unlock()
			for r := 0; r < b.N; r++ {
				buf = buf[:0]
				for _, ci := range idx {
					buf = appendEqKey(buf, &b.Cols[ci], r)
				}
				set[string(buf)] = struct{}{}
			}
			return nil
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &eqSet{cols: cols, keys: set}, nil
}

// eqSet is one equality-delete constraint: rows whose values in cols match
// any key in the set are deleted.
type eqSet struct {
	cols []string // column names (resolved to indices per file schema)
	keys map[string]struct{}
}

// fileDeletes is everything deleted from one data file.
type fileDeletes struct {
	pos *roaring64.Bitmap // file-relative row positions; nil = none
	eq  []*eqSet
	// key identifies the delete set for same-table snapshot pruning: two
	// snapshots sharing a data file may still differ in what's deleted
	// from it. Built from the contributing delete-file paths + sequence
	// numbers.
	key string
}

func (d *fileDeletes) empty() bool {
	return d == nil || ((d.pos == nil || d.pos.IsEmpty()) && len(d.eq) == 0)
}

// posCount is the exact number of position-deleted rows.
func (d *fileDeletes) posCount() int64 {
	if d == nil || d.pos == nil {
		return 0
	}
	return int64(d.pos.GetCardinality())
}

// deleteState is fileDeletes resolved against one parquet schema.
type deleteState struct {
	pos    *roaring64.Bitmap
	eq     []*eqSet
	eqCols [][]int // per eq set, column indices into the schema
	keep   []int32 // scratch
	keybuf []byte  // scratch
}

// resolveDeletes binds delete metadata to a schema (equality column names →
// indices). Missing columns are an error: silently not filtering would
// resurrect deleted rows.
func resolveDeletes(d *fileDeletes, schema *Schema, path string) (*deleteState, error) {
	if d.empty() {
		return nil, nil
	}
	st := &deleteState{pos: d.pos, eq: d.eq}
	for _, e := range d.eq {
		idx := make([]int, len(e.cols))
		for i, name := range e.cols {
			ci := schema.ColumnIndex(name)
			if ci < 0 {
				return nil, fmt.Errorf("%s: equality delete on column %q not present in the file", path, name)
			}
			idx[i] = ci
		}
		st.eqCols = append(st.eqCols, idx)
	}
	return st, nil
}

// appendEqKey encodes one cell into a deterministic byte key. The same
// encoding builds delete-set keys and probe keys, so equality is exact:
// no hash collisions to reason about.
func appendEqKey(dst []byte, c *Col, r int) []byte {
	if c.Nulls != nil && c.Nulls[r] {
		return append(dst, 0)
	}
	switch c.Type {
	case TypeFloat64:
		f := c.F64[r]
		if f == 0 {
			f = 0 // -0 folds to +0
		}
		dst = append(dst, 2)
		return binary.LittleEndian.AppendUint64(dst, math.Float64bits(f))
	case TypeString, TypeBytes:
		var s string
		if c.Idx != nil {
			s = c.Dict[c.Idx[r]]
		} else {
			s = c.Str[r]
		}
		dst = append(dst, 3)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(s)))
		return append(dst, s...)
	default: // int64-backed: bool, int, timestamp, date
		dst = append(dst, 1)
		return binary.LittleEndian.AppendUint64(dst, uint64(c.I64[r]))
	}
}

// appendEqKeyValue encodes a canonical Value with the same scheme as
// appendEqKey (used when building sets from delete-file rows).
func appendEqKeyValue(dst []byte, v Value) []byte {
	if v.Null {
		return append(dst, 0)
	}
	switch v.Type {
	case TypeFloat64:
		f := v.Float
		if f == 0 {
			f = 0
		}
		dst = append(dst, 2)
		return binary.LittleEndian.AppendUint64(dst, math.Float64bits(f))
	case TypeString, TypeBytes:
		dst = append(dst, 3)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(v.Str)))
		return append(dst, v.Str...)
	default:
		dst = append(dst, 1)
		return binary.LittleEndian.AppendUint64(dst, uint64(v.Int))
	}
}

// deletedValueRow reports whether a converted row at global position pos is
// deleted (serial RowIter path).
func (st *deleteState) deletedValueRow(row []Value, pos int64) bool {
	if st.pos != nil && st.pos.Contains(uint64(pos)) {
		return true
	}
	for ei, e := range st.eq {
		st.keybuf = st.keybuf[:0]
		for _, ci := range st.eqCols[ei] {
			st.keybuf = appendEqKeyValue(st.keybuf, row[ci])
		}
		if _, hit := e.keys[string(st.keybuf)]; hit {
			return true
		}
	}
	return false
}

// deletedRow reports whether row r of the batch is deleted.
func (st *deleteState) deletedRow(b *Batch, base int64, r int) bool {
	if st.pos != nil && st.pos.Contains(uint64(base)+uint64(r)) {
		return true
	}
	for ei, e := range st.eq {
		st.keybuf = st.keybuf[:0]
		for _, ci := range st.eqCols[ei] {
			st.keybuf = appendEqKey(st.keybuf, &b.Cols[ci], r)
		}
		if _, hit := e.keys[string(st.keybuf)]; hit {
			return true
		}
	}
	return false
}

// filterBatch drops deleted rows from b (rows [base, base+b.N) of the file).
// Returns fast when nothing in the range is deleted.
func (st *deleteState) filterBatch(b *Batch, base int64) {
	// cheap range check for pure position deletes
	if len(st.eq) == 0 {
		if st.pos == nil {
			return
		}
		lo, hi := uint64(base), uint64(base+int64(b.N))
		it := st.pos.Iterator()
		it.AdvanceIfNeeded(lo)
		if !it.HasNext() || it.PeekNext() >= hi {
			return
		}
	}
	keep := st.keep[:0]
	for r := 0; r < b.N; r++ {
		if !st.deletedRow(b, base, r) {
			keep = append(keep, int32(r))
		}
	}
	st.keep = keep
	if len(keep) == b.N {
		return
	}
	for ci := range b.Cols {
		compactCol(&b.Cols[ci], keep)
	}
	b.N = len(keep)
}

// compactCol keeps only the listed rows, in order (in-place: dst ≤ src).
func compactCol(c *Col, keep []int32) {
	for dst, src := range keep {
		if c.I64 != nil {
			c.I64[dst] = c.I64[src]
		}
		if c.F64 != nil {
			c.F64[dst] = c.F64[src]
		}
		if c.Str != nil {
			c.Str[dst] = c.Str[src]
		}
		if c.Idx != nil {
			c.Idx[dst] = c.Idx[src]
		}
		if c.Nulls != nil {
			c.Nulls[dst] = c.Nulls[src]
		}
	}
	c.truncate(len(keep))
}
