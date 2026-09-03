package diff

// keyTable is an open-addressing (linear probing) hash table from a 64-bit
// key hash to a 64-bit row hash plus a matched flag. It exists instead of a
// Go map to keep the build side of the hash join at ~17 bytes/row instead of
// ~50, which is the dominant memory cost of in-memory diffing.
//
// Key hash 0 is remapped by the caller (see mixKeyHash); slot key 0 means
// empty.
type keyTable struct {
	keys  []uint64
	rows  []uint64
	flags []uint8 // bit0: matched
	mask  uint64
	len   int
}

const tableMaxLoad = 0.75

func newKeyTable(sizeHint int) *keyTable {
	n := uint64(16)
	for float64(sizeHint) > float64(n)*tableMaxLoad {
		n *= 2
	}
	return &keyTable{
		keys:  make([]uint64, n),
		rows:  make([]uint64, n),
		flags: make([]uint8, n),
		mask:  n - 1,
	}
}

// insert adds keyHash → rowHash. Returns false if the key hash is already
// present (duplicate key).
func (t *keyTable) insert(keyHash, rowHash uint64) bool {
	if float64(t.len+1) > float64(len(t.keys))*tableMaxLoad {
		t.grow()
	}
	i := keyHash & t.mask
	for {
		k := t.keys[i]
		if k == 0 {
			t.keys[i] = keyHash
			t.rows[i] = rowHash
			t.len++
			return true
		}
		if k == keyHash {
			return false
		}
		i = (i + 1) & t.mask
	}
}

// probe looks up keyHash. found=false: not present. Otherwise it reports the
// stored row hash and marks the slot matched.
func (t *keyTable) probe(keyHash uint64) (rowHash uint64, found bool) {
	i := keyHash & t.mask
	for {
		k := t.keys[i]
		if k == 0 {
			return 0, false
		}
		if k == keyHash {
			t.flags[i] |= 1
			return t.rows[i], true
		}
		i = (i + 1) & t.mask
	}
}

// matched reports whether keyHash was probed successfully at least once.
func (t *keyTable) matched(keyHash uint64) bool {
	i := keyHash & t.mask
	for {
		k := t.keys[i]
		if k == 0 {
			return false
		}
		if k == keyHash {
			return t.flags[i]&1 != 0
		}
		i = (i + 1) & t.mask
	}
}

// unmatchedCount counts entries never matched by a probe.
func (t *keyTable) unmatchedCount() int64 {
	var n int64
	for i, k := range t.keys {
		if k != 0 && t.flags[i]&1 == 0 {
			n++
		}
	}
	return n
}

func (t *keyTable) grow() {
	oldKeys, oldRows, oldFlags := t.keys, t.rows, t.flags
	n := uint64(len(oldKeys)) * 2
	t.keys = make([]uint64, n)
	t.rows = make([]uint64, n)
	t.flags = make([]uint8, n)
	t.mask = n - 1
	for i, k := range oldKeys {
		if k == 0 {
			continue
		}
		j := k & t.mask
		for t.keys[j] != 0 {
			j = (j + 1) & t.mask
		}
		t.keys[j] = k
		t.rows[j] = oldRows[i]
		t.flags[j] = oldFlags[i]
	}
}
