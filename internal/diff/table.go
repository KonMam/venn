package diff

import "sync/atomic"

// keyTable is an open-addressing (linear probing) hash table from a 64-bit
// key hash to a 64-bit row hash. It exists instead of a Go map to keep the
// build side of the hash join at ~17 bytes/row instead of ~50, which is the
// dominant memory cost of in-memory diffing.
//
// Lifecycle: writes (insert) happen in pass 1 under the stripe lock; seal()
// then freezes the layout, after which probe/matched/markMatched are safe
// from any number of goroutines with no lock (probes are pure reads, match
// marking is an atomic bitset).
//
// Key hash 0 is remapped by the caller (see mixKeyHash); slot key 0 means
// empty.
type keyTable struct {
	keys    []uint64
	rows    []uint64
	matched []atomic.Uint32 // 1 bit per slot, allocated by seal()
	mask    uint64
	len     int
}

const tableMaxLoad = 0.85

func newKeyTable(sizeHint int) *keyTable {
	t := &keyTable{}
	t.reuse(sizeHint)
	return t
}

// reuse resizes/clears the table for a fresh build, keeping allocations when
// capacity suffices (join workers cycle through many partitions; reusing the
// large arrays avoids churning multi-MB spans through the heap).
func (t *keyTable) reuse(sizeHint int) {
	n := uint64(16)
	for float64(sizeHint) > float64(n)*tableMaxLoad {
		n *= 2
	}
	if uint64(cap(t.keys)) >= n {
		t.keys = t.keys[:n]
		t.rows = t.rows[:n]
		clear(t.keys)
		clear(t.rows)
	} else {
		t.keys = make([]uint64, n)
		t.rows = make([]uint64, n)
	}
	t.matched = nil
	t.mask = n - 1
	t.len = 0
}

// insert adds keyHash → rowHash. Returns false if the key hash is already
// present (duplicate key). Callers must hold the stripe lock.
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

// seal freezes the table layout and allocates the matched bitset. Must be
// called after the last insert and before the first probe.
func (t *keyTable) seal() {
	n := (len(t.keys) + 31) / 32
	if cap(t.matched) >= n {
		t.matched = t.matched[:n]
		for i := range t.matched {
			t.matched[i].Store(0)
		}
	} else {
		t.matched = make([]atomic.Uint32, n)
	}
}

// probe looks up keyHash without mutating anything. slot is only meaningful
// when found.
func (t *keyTable) probe(keyHash uint64) (rowHash uint64, slot uint64, found bool) {
	i := keyHash & t.mask
	for {
		k := t.keys[i]
		if k == 0 {
			return 0, 0, false
		}
		if k == keyHash {
			return t.rows[i], i, true
		}
		i = (i + 1) & t.mask
	}
}

// markMatched records that slot was matched by a probe (atomic, idempotent)
// and reports whether it was already matched — i.e. this key was consumed by
// an earlier right-side row.
func (t *keyTable) markMatched(slot uint64) bool {
	bit := uint32(1) << (slot % 32)
	old := t.matched[slot/32].Or(bit)
	return old&bit != 0
}

// isMatched reports whether the slot was marked matched.
func (t *keyTable) isMatched(slot uint64) bool {
	return t.matched[slot/32].Load()&(1<<(slot%32)) != 0
}

// matchedKey reports whether keyHash is present and was matched.
func (t *keyTable) matchedKey(keyHash uint64) bool {
	_, slot, found := t.probe(keyHash)
	return found && t.isMatched(slot)
}

// unmatchedCount counts entries never matched by a probe.
func (t *keyTable) unmatchedCount() int64 {
	var n int64
	for i, k := range t.keys {
		if k != 0 && !t.isMatched(uint64(i)) {
			n++
		}
	}
	return n
}

func (t *keyTable) grow() {
	oldKeys, oldRows := t.keys, t.rows
	n := uint64(len(oldKeys)) * 2
	t.keys = make([]uint64, n)
	t.rows = make([]uint64, n)
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
	}
}
