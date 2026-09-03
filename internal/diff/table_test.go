package diff

import "testing"

func TestKeyTable(t *testing.T) {
	tab := newKeyTable(0) // force growth
	const n = 100_000
	for i := uint64(1); i <= n; i++ {
		if !tab.insert(i, i*3) {
			t.Fatalf("insert %d reported duplicate", i)
		}
	}
	if tab.insert(5, 99) {
		t.Fatal("duplicate insert not detected")
	}
	tab.seal()
	for i := uint64(1); i <= n; i += 2 {
		rh, slot, ok := tab.probe(i)
		if !ok || rh != i*3 {
			t.Fatalf("probe %d: ok=%v rh=%d", i, ok, rh)
		}
		tab.markMatched(slot)
	}
	if _, _, ok := tab.probe(n + 1); ok {
		t.Fatal("probe of absent key succeeded")
	}
	if got := tab.unmatchedCount(); got != n/2 {
		t.Fatalf("unmatched: got %d want %d", got, n/2)
	}
	if !tab.matchedKey(1) || tab.matchedKey(2) {
		t.Fatal("matched flags wrong")
	}
}
