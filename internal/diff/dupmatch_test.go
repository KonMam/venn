package diff_test

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/KonMam/venn/internal/diff"
	"github.com/KonMam/venn/internal/source"
)

// dupFixture is the canonical duplicate-key case, covering every shape the
// multiset rule has to get right:
//
//	k1  L{A,A}    R{A,A}    2 unchanged
//	k2  L{A,B}    R{A,C}    1 unchanged, then 1:1 leftovers -> 1 changed
//	k3  L{A}      R{A,A}    1 unchanged, 1 added (right-side duplicate)
//	k4  L{A,B,C}  R{A}      1 unchanged, 2 removed
//	k5  L{A,A}    R{}        2 removed
//	k6  L{A,B}    R{C,D}    2 removed, 2 added (leftovers are not 1:1)
func dupFixture(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"),
		"id,v\nk1,A\nk1,A\nk2,A\nk2,B\nk3,A\nk4,A\nk4,B\nk4,C\nk5,A\nk5,A\nk6,A\nk6,B\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"),
		"id,v\nk1,A\nk1,A\nk2,A\nk2,C\nk3,A\nk3,A\nk4,A\nk6,C\nk6,D\n")
	return left, right
}

func TestOnDupMatch(t *testing.T) {
	left, right := dupFixture(t)
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory", Limit: 100,
	})
	if got.Unchanged != 5 {
		t.Errorf("unchanged=%d want 5", got.Unchanged)
	}
	if got.Changed != 1 {
		t.Errorf("changed=%d want 1 (only k2's 1:1 leftovers)", got.Changed)
	}
	if got.Added != 3 {
		t.Errorf("added=%d want 3 (k3 x1, k6 x2)", got.Added)
	}
	if got.Removed != 6 {
		t.Errorf("removed=%d want 6 (k4 x2, k5 x2, k6 x2)", got.Removed)
	}
	if got.LeftRows != 12 || got.RightRows != 9 {
		t.Errorf("rows %d/%d want 12/9", got.LeftRows, got.RightRows)
	}
	// k1, k2, k4, k5, k6 are duplicated on the left; k3 only on the right
	if got.DupKeys != 5 || got.DupRows != 11 {
		t.Errorf("dup keys/rows = %d/%d want 5/11", got.DupKeys, got.DupRows)
	}
	// the one changed row is k2, attributed to v
	if len(got.ChangedExamples) != 1 || got.ChangedExamples[0].Key != "k2" {
		t.Fatalf("changed examples = %+v want one for k2", got.ChangedExamples)
	}
	cols := got.ChangedExamples[0].Columns
	if len(cols) != 1 || cols[0].Column != "v" || cols[0].Left != "B" || cols[0].Right != "C" {
		t.Errorf("attribution = %+v want v: B -> C", cols)
	}
	if got.ColumnChanges["v"] != 1 {
		t.Errorf("column changes = %v want v:1", got.ColumnChanges)
	}
	// examples name the right keys, with the right multiplicity
	assertKeyBag(t, "added", got.AddedExamples, []string{"k3", "k6", "k6"})
	assertKeyBag(t, "removed", got.RemovedExamples, []string{"k4", "k4", "k5", "k5", "k6", "k6"})
}

func assertKeyBag(t *testing.T, kind string, got, want []string) {
	t.Helper()
	g, w := append([]string{}, got...), append([]string{}, want...)
	sort.Strings(g)
	sort.Strings(w)
	if strings.Join(g, ",") != strings.Join(w, ",") {
		t.Errorf("%s examples = %v want %v", kind, g, w)
	}
}

// TestOnDupMatchDeterministic is the reason the rule is a multiset match and
// not a rank-within-group pairing: the scan has no row order, so the answer
// must not depend on one. The same inputs must give the same counts every
// time, at every worker count.
func TestOnDupMatchDeterministic(t *testing.T) {
	left, right := dupFixture(t)
	var first string
	for _, threads := range []int{1, 2, 4, 8} {
		for run := 0; run < 4; run++ {
			got := runDiff(t, left, right, diff.Options{
				Keys: []string{"id"}, OnDup: "match", Mode: "memory",
				Threads: threads, Limit: 100,
			})
			key := fmt.Sprintf("+%d -%d ~%d =%d", got.Added, got.Removed, got.Changed, got.Unchanged)
			if first == "" {
				first = key
				continue
			}
			if key != first {
				t.Fatalf("threads=%d run=%d gave %s, first run gave %s", threads, run, key, first)
			}
		}
	}
}

func TestOnDupMatchExport(t *testing.T) {
	left, right := dupFixture(t)
	sink := &statusCollector{}
	if _, err := diff.Run(mustOpen(t, left), mustOpen(t, right), diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory", Sink: sink,
	}); err != nil {
		t.Fatal(err)
	}
	// the export must carry exactly the differing rows: 3 added, 6 removed,
	// 1 changed, no row of a cancelled pair
	if sink.counts['a'] != 3 || sink.counts['r'] != 6 || sink.counts['c'] != 1 {
		t.Fatalf("exported a=%d r=%d c=%d want 3/6/1", sink.counts['a'], sink.counts['r'], sink.counts['c'])
	}
	// every exported row must carry a key
	for _, row := range sink.rows {
		if !strings.Contains(row, "k") {
			t.Fatalf("exported row without a key: %q", row)
		}
	}
}

// statusCollector counts the exported rows by status. WriteDiffRow is called
// from every scan worker, so the sink locks like a real one.
type statusCollector struct {
	mu     sync.Mutex
	counts map[byte]int
	rows   []string
}

func (c *statusCollector) WriteDiffRow(status byte, key, left, right []source.Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[byte]int{}
	}
	c.counts[status]++
	var sb strings.Builder
	sb.WriteByte(status)
	for i := range key {
		sb.WriteString("|" + key[i].Display())
	}
	c.rows = append(c.rows, sb.String())
	return nil
}

// TestOnDupMatchSummary pins that summary mode reaches the same counts
// without keeping any row values.
func TestOnDupMatchSummary(t *testing.T) {
	left, right := dupFixture(t)
	full := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory", Limit: 100,
	})
	sum := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory", Summary: true,
	})
	if sum.Added != full.Added || sum.Removed != full.Removed ||
		sum.Changed != full.Changed || sum.Unchanged != full.Unchanged {
		t.Fatalf("summary +%d -%d ~%d =%d, full +%d -%d ~%d =%d",
			sum.Added, sum.Removed, sum.Changed, sum.Unchanged,
			full.Added, full.Removed, full.Changed, full.Unchanged)
	}
	if len(sum.ChangedExamples) != 0 || len(sum.ColumnChanges) != 0 {
		t.Errorf("summary mode kept attribution: %+v / %v", sum.ChangedExamples, sum.ColumnChanges)
	}
}

// TestOnDupMatchIdenticalInputs pins the CI hot path: duplicated keys must
// not make identical inputs look different.
func TestOnDupMatchIdenticalInputs(t *testing.T) {
	dir := t.TempDir()
	f := writeFile(t, filepath.Join(dir, "a.csv"), "id,v\nk1,A\nk1,A\nk1,B\nk2,X\n")
	got := runDiff(t, f, f, diff.Options{Keys: []string{"id"}, OnDup: "match", Mode: "memory"})
	if !got.Same() {
		t.Fatalf("%+v want identical", got)
	}
	if got.Unchanged != 4 {
		t.Fatalf("unchanged=%d want 4", got.Unchanged)
	}
}

// TestOnDupMatchWithTolerance pins that a duplicate group's 1:1 leftovers go
// through the same tolerance reclassification as any other changed row.
func TestOnDupMatchWithTolerance(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,v\nk1,1.0\nk1,2.0\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,v\nk1,1.0\nk1,2.004\n")
	exact := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory",
	})
	if exact.Changed != 1 {
		t.Fatalf("exact: changed=%d want 1", exact.Changed)
	}
	tol := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory",
		Tolerance: &diff.Tolerance{Abs: 0.01},
	})
	if tol.Changed != 0 || tol.WithinTolerance != 1 {
		t.Fatalf("tolerance: changed=%d within=%d want 0/1", tol.Changed, tol.WithinTolerance)
	}
}

// TestOnDupMatchMasking pins that masking still hides values inside
// duplicate groups.
func TestOnDupMatchMasking(t *testing.T) {
	dir := t.TempDir()
	left := writeFile(t, filepath.Join(dir, "l.csv"), "id,email\nk1,a@x\nk1,b@x\n")
	right := writeFile(t, filepath.Join(dir, "r.csv"), "id,email\nk1,a@x\nk1,c@x\n")
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory",
		Mask: []string{"email"}, Limit: 10,
	})
	if got.Changed != 1 {
		t.Fatalf("changed=%d want 1", got.Changed)
	}
	all := fmt.Sprint(got.ChangedExamples)
	for _, secret := range []string{"b@x", "c@x"} {
		if strings.Contains(all, secret) {
			t.Fatalf("masked value %q leaked: %s", secret, all)
		}
	}
}

// TestOnDupMatchLargeGroup exercises the counting-map representation a group
// is promoted to past dupGroupPromote entries.
func TestOnDupMatchLargeGroup(t *testing.T) {
	dir := t.TempDir()
	var l, r strings.Builder
	l.WriteString("id,v\n")
	r.WriteString("id,v\n")
	const n = 200
	for i := 0; i < n; i++ {
		fmt.Fprintf(&l, "k,%d\n", i%7) // many repeats of a few values
		if i < n-3 {
			fmt.Fprintf(&r, "k,%d\n", i%7)
		}
	}
	left := writeFile(t, filepath.Join(dir, "l.csv"), l.String())
	right := writeFile(t, filepath.Join(dir, "r.csv"), r.String())
	got := runDiff(t, left, right, diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "memory", Limit: 100,
	})
	if got.Unchanged != n-3 || got.Removed != 3 || got.Added != 0 || got.Changed != 0 {
		t.Fatalf("%+v want %d unchanged, 3 removed", got, n-3)
	}
}

func TestOnDupMatchRejections(t *testing.T) {
	left, right := dupFixture(t)
	// streaming cannot identify which rows of a duplicated key were leftovers
	_, err := diff.Run(mustOpen(t, left), mustOpen(t, right), diff.Options{
		Keys: []string{"id"}, OnDup: "match", Mode: "stream",
	})
	if err == nil || !strings.Contains(err.Error(), "in-memory join") {
		t.Fatalf("err = %v, want a streaming-mode refusal", err)
	}
	// and an unknown mode is named rather than silently ignored
	if _, err := diff.Run(mustOpen(t, left), mustOpen(t, right), diff.Options{
		Keys: []string{"id"}, OnDup: "keep",
	}); err == nil || !strings.Contains(err.Error(), "on-dup") {
		t.Fatalf("err = %v, want an unknown --on-dup error", err)
	}
}

// TestOnDupErrorStillFails pins that the default is unchanged: a duplicate
// key is an error unless one of the duplicate modes is asked for.
func TestOnDupErrorStillFails(t *testing.T) {
	left, right := dupFixture(t)
	if _, err := diff.Run(mustOpen(t, left), mustOpen(t, right),
		diff.Options{Keys: []string{"id"}}); err == nil {
		t.Fatal("expected a duplicate-key error by default")
	}
}

// TestOnDupMatchAgainstReference is the real correctness proof: random
// duplicate-heavy datasets, checked against a straightforward multiset
// reference computed in the test. The reference is deliberately naive
// (maps of counts, no hashing, no concurrency) so it cannot share a bug with
// the engine.
func TestOnDupMatchAgainstReference(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			lRows, rRows := randomDupRows(seed)
			left := writeFile(t, filepath.Join(dir, "l.csv"), csvOf(lRows))
			right := writeFile(t, filepath.Join(dir, "r.csv"), csvOf(rRows))
			want := referenceMultisetDiff(lRows, rRows)
			got := runDiff(t, left, right, diff.Options{
				Keys: []string{"id"}, OnDup: "match", Mode: "memory", Limit: 1 << 20,
			})
			if got.Unchanged != want.unchanged || got.Changed != want.changed ||
				got.Added != want.added || got.Removed != want.removed {
				t.Fatalf("+%d -%d ~%d =%d, want +%d -%d ~%d =%d\nleft=%v\nright=%v",
					got.Added, got.Removed, got.Changed, got.Unchanged,
					want.added, want.removed, want.changed, want.unchanged, lRows, rRows)
			}
			// every reported row must be accounted for: the totals have to
			// close against both inputs
			if got.Unchanged+got.Changed+got.Removed != int64(len(lRows)) {
				t.Fatalf("left side does not close: %d+%d+%d != %d",
					got.Unchanged, got.Changed, got.Removed, len(lRows))
			}
			if got.Unchanged+got.Changed+got.Added != int64(len(rRows)) {
				t.Fatalf("right side does not close: %d+%d+%d != %d",
					got.Unchanged, got.Changed, got.Added, len(rRows))
			}
			// and the examples must have the multiplicity of the counts
			if int64(len(got.RemovedExamples)) != got.Removed {
				t.Fatalf("%d removed examples for %d removed rows", len(got.RemovedExamples), got.Removed)
			}
			if int64(len(got.AddedExamples)) != got.Added {
				t.Fatalf("%d added examples for %d added rows", len(got.AddedExamples), got.Added)
			}
			if int64(len(got.ChangedExamples)) != got.Changed {
				t.Fatalf("%d changed examples for %d changed rows", len(got.ChangedExamples), got.Changed)
			}
		})
	}
}

type dupRow struct{ key, val string }

// randomDupRows builds two row lists with heavy key duplication from a seed.
func randomDupRows(seed uint64) (left, right []dupRow) {
	mix := func(x uint64) uint64 {
		x += 0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		return x ^ (x >> 31)
	}
	s := mix(seed)
	next := func(n uint64) uint64 { s = mix(s); return s % n }
	const keys = 8
	for k := uint64(0); k < keys; k++ {
		key := fmt.Sprintf("k%d", k)
		nl, nr := next(4), next(4) // 0..3 rows per side, so empty sides occur
		for i := uint64(0); i < nl; i++ {
			left = append(left, dupRow{key, fmt.Sprintf("v%d", next(4))})
		}
		for i := uint64(0); i < nr; i++ {
			right = append(right, dupRow{key, fmt.Sprintf("v%d", next(4))})
		}
	}
	return left, right
}

func csvOf(rows []dupRow) string {
	var sb strings.Builder
	sb.WriteString("id,v\n")
	for _, r := range rows {
		fmt.Fprintf(&sb, "%s,%s\n", r.key, r.val)
	}
	return sb.String()
}

type refCounts struct{ unchanged, changed, added, removed int64 }

// referenceMultisetDiff is the specification, written the obvious way: per
// key, cancel identical values, then label leftovers of exactly one row per
// side "changed" and everything else added/removed.
func referenceMultisetDiff(left, right []dupRow) refCounts {
	lc := map[string]map[string]int{}
	rc := map[string]map[string]int{}
	add := func(m map[string]map[string]int, r dupRow) {
		if m[r.key] == nil {
			m[r.key] = map[string]int{}
		}
		m[r.key][r.val]++
	}
	for _, r := range left {
		add(lc, r)
	}
	for _, r := range right {
		add(rc, r)
	}
	keys := map[string]bool{}
	for k := range lc {
		keys[k] = true
	}
	for k := range rc {
		keys[k] = true
	}
	var out refCounts
	for k := range keys {
		l, r := lc[k], rc[k]
		var leftoverL, leftoverR int64
		for v, n := range l {
			m := r[v]
			cancel := n
			if m < cancel {
				cancel = m
			}
			out.unchanged += int64(cancel)
			leftoverL += int64(n - cancel)
		}
		for v, m := range r {
			n := l[v]
			cancel := m
			if n < cancel {
				cancel = n
			}
			leftoverR += int64(m - cancel)
		}
		if leftoverL == 1 && leftoverR == 1 {
			out.changed++
			continue
		}
		out.removed += leftoverL
		out.added += leftoverR
	}
	return out
}
