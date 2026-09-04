// Package torture is the robustness suite: it feeds the real venn binary
// deliberately broken inputs (bit flips, truncations, injected newlines,
// broken quotes, corrupted compression streams and snapshots) and asserts
// the SQLite malformed-database contract: errors are detected and reported
// cleanly, "without overflowing buffers, dereferencing NULL pointers, or
// performing other unwholesome actions". Concretely, for every mutated input
// venn must terminate quickly, exit 0/1/2 (never crash), print an error on
// exit 2, keep memory bounded, and emit valid JSON whenever it claims success.
//
// Mutations are seeded and enumerated (never time-based), so any failure
// reproduces from the subtest name alone. VENN_TORTURE_ROUNDS raises the
// seeds-per-mutator count for longer runs (default 3).
package torture

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/KonMam/venn/internal/fixture"
)

var (
	vennBin  string
	fixDir   string
	snapPath string
)

const fixtureRows = 5000

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "venn-torture-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "torture:", err)
		os.Exit(1)
	}
	code := func() int {
		defer os.RemoveAll(tmp)
		exe := ""
		if strings.HasPrefix(os.Getenv("GOOS"), "windows") || os.PathSeparator == '\\' {
			exe = ".exe"
		}
		vennBin = filepath.Join(tmp, "venn"+exe)
		build := exec.Command("go", "build", "-o", vennBin, "./cmd/venn")
		build.Dir = "../.."
		if out, err := build.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "torture: build: %v\n%s", err, out)
			return 1
		}
		fixDir = filepath.Join(tmp, "fix")
		if _, err := fixture.Generate(fixture.Config{
			Rows: fixtureRows, Cols: 8, Seed: 7,
			PctChanged: 0.01, PctAdded: 0.005, PctRemoved: 0.005,
			Out: fixDir, Formats: []string{"parquet", "csv", "ndjson", "csv.gz", "csv.zst"},
			Variant: "standard",
		}); err != nil {
			fmt.Fprintln(os.Stderr, "torture: fixtures:", err)
			return 1
		}
		snapPath = filepath.Join(tmp, "base.snap")
		snap := exec.Command(vennBin, "snapshot", filepath.Join(fixDir, "left.parquet"),
			"--key", "id", "--output", snapPath)
		if out, err := snap.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "torture: snapshot: %v\n%s", err, out)
			return 1
		}
		return m.Run()
	}()
	os.Exit(code)
}

func rounds() int {
	if v := os.Getenv("VENN_TORTURE_ROUNDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 3
}

// --- mutators -------------------------------------------------------------

type mutator struct {
	name string
	text bool // only meaningful for text formats
	fn   func(data []byte, r *rand.Rand) []byte
}

func randOff(r *rand.Rand, n int) int {
	if n == 0 {
		return 0
	}
	return r.IntN(n)
}

var mutators = []mutator{
	{name: "truncate", fn: func(d []byte, r *rand.Rand) []byte {
		return d[:randOff(r, len(d))]
	}},
	{name: "bitflips", fn: func(d []byte, r *rand.Rand) []byte {
		out := append([]byte(nil), d...)
		for i := 0; i < 8 && len(out) > 0; i++ {
			off := randOff(r, len(out))
			out[off] ^= 1 << r.IntN(8)
		}
		return out
	}},
	{name: "zero-region", fn: func(d []byte, r *rand.Rand) []byte {
		out := append([]byte(nil), d...)
		if len(out) == 0 {
			return out
		}
		off := randOff(r, len(out))
		n := min(len(out)-off, 1+r.IntN(256))
		for i := 0; i < n; i++ {
			out[off+i] = 0
		}
		return out
	}},
	{name: "delete-region", fn: func(d []byte, r *rand.Rand) []byte {
		if len(d) == 0 {
			return d
		}
		off := randOff(r, len(d))
		n := min(len(d)-off, 1+r.IntN(512))
		return append(append([]byte(nil), d[:off]...), d[off+n:]...)
	}},
	{name: "append-garbage", fn: func(d []byte, r *rand.Rand) []byte {
		g := make([]byte, 64+r.IntN(512))
		for i := range g {
			g[i] = byte(r.UintN(256))
		}
		return append(append([]byte(nil), d...), g...)
	}},
	{name: "inject-newlines", text: true, fn: func(d []byte, r *rand.Rand) []byte {
		return injectAt(d, r, 6, []byte("\n"))
	}},
	{name: "inject-crlf", text: true, fn: func(d []byte, r *rand.Rand) []byte {
		return injectAt(d, r, 6, []byte("\r\n"))
	}},
	{name: "inject-quote", text: true, fn: func(d []byte, r *rand.Rand) []byte {
		return injectAt(d, r, 4, []byte(`"`))
	}},
	{name: "inject-nul", text: true, fn: func(d []byte, r *rand.Rand) []byte {
		return injectAt(d, r, 4, []byte{0})
	}},
	{name: "inject-bad-utf8", text: true, fn: func(d []byte, r *rand.Rand) []byte {
		return injectAt(d, r, 4, []byte{0xff, 0xfe, 0xc0, 0xaf})
	}},
}

func injectAt(d []byte, r *rand.Rand, times int, ins []byte) []byte {
	out := append([]byte(nil), d...)
	for i := 0; i < times; i++ {
		off := randOff(r, len(out)+1)
		out = append(out[:off], append(append([]byte(nil), ins...), out[off:]...)...)
	}
	return out
}

// --- invariants -----------------------------------------------------------

type result struct {
	exit   int
	stdout string
	stderr string
	rssMB  int64
}

func runVenn(t *testing.T, args ...string) result {
	t.Helper()
	cmd := exec.Command(vennBin, args...)
	var out, errb strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &errb
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-timeAfter(t):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("HANG: venn did not terminate (args: %v)", args)
	}
	ps := cmd.ProcessState
	return result{
		exit:   ps.ExitCode(),
		stdout: out.String(),
		stderr: errb.String(),
		rssMB:  peakRSSMB(ps),
	}
}

// assertSurvives is the core contract for arbitrarily broken input.
func assertSurvives(t *testing.T, res result) {
	t.Helper()
	if res.exit != 0 && res.exit != 1 && res.exit != 2 {
		t.Errorf("exit code %d, want 0, 1 or 2\nstderr: %s", res.exit, tail(res.stderr))
	}
	for _, marker := range []string{"panic:", "runtime error", "goroutine 1 ["} {
		if strings.Contains(res.stderr, marker) || strings.Contains(res.stdout, marker) {
			t.Errorf("crash marker %q in output\nstderr: %s", marker, tail(res.stderr))
		}
	}
	if res.exit == 2 && strings.TrimSpace(res.stderr) == "" {
		t.Errorf("exit 2 with empty stderr: errors must be reported")
	}
	// Success claimed on JSON output must actually be JSON with sane counts.
	if res.exit == 0 || res.exit == 1 {
		var c struct{ Added, Removed, Changed int64 }
		if err := json.Unmarshal([]byte(res.stdout), &c); err != nil {
			t.Errorf("exit %d but stdout is not valid JSON: %v\nstdout: %s", res.exit, err, tail(res.stdout))
		} else if c.Added < 0 || c.Removed < 0 || c.Changed < 0 {
			t.Errorf("negative counts: %+v", c)
		}
	}
	// The inputs are a few hundred KB; memory must stay in the same universe.
	if res.rssMB > 1024 {
		t.Errorf("peak RSS %dMB on a tiny corrupt input, so allocation is unbounded", res.rssMB)
	}
}

func tail(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 500 {
		return "…" + s[len(s)-500:]
	}
	return s
}

// --- the matrix -----------------------------------------------------------

var formats = []struct {
	file string
	text bool
}{
	{"left.parquet", false},
	{"left.csv", true},
	{"left.ndjson", true},
	{"left.csv.gz", false},
	{"left.csv.zst", false},
}

// TestMutatedInputs: every format × every applicable mutator × seeds. The
// mutated file diffs against a pristine parquet right side (cross-format is
// supported, so one clean side exercises the full join path).
func TestMutatedInputs(t *testing.T) {
	right := filepath.Join(fixDir, "right.parquet")
	for _, f := range formats {
		f := f
		t.Run(f.file, func(t *testing.T) {
			t.Parallel()
			orig, err := os.ReadFile(filepath.Join(fixDir, f.file))
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			for _, mu := range mutators {
				if mu.text && !f.text {
					continue
				}
				for seed := 0; seed < rounds(); seed++ {
					name := fmt.Sprintf("%s/seed%d", mu.name, seed)
					t.Run(name, func(t *testing.T) {
						r := rand.New(rand.NewPCG(uint64(seed), 0x7d1ff))
						mutated := mu.fn(orig, r)
						p := filepath.Join(dir, fmt.Sprintf("%s-%d-%s", mu.name, seed, f.file))
						if err := os.WriteFile(p, mutated, 0o644); err != nil {
							t.Fatal(err)
						}
						res := runVenn(t, p, right, "--key", "id", "--format", "json")
						assertSurvives(t, res)
					})
				}
			}
		})
	}
}

// comparisonFlagSets are the value-normalization and tolerance flags. They
// add per-value code paths (case folding, whitespace trimming, timestamp
// truncation, epsilon reclassification) that must survive garbage input just
// like the exact paths do.
var comparisonFlagSets = [][]string{
	{"--trim", "--ignore-case"},
	{"--timestamp-precision", "s"},
	{"--tolerance", "0.01,rel=1e-6"},
	{"--trim", "--ignore-case", "--timestamp-precision", "ms", "--tolerance", "1e-9"},
	{"--on-dup", "match"},
	{"--mask", "id"},
	{"--where", "id >= 0"},
	{"--where", "id > 100 and id < 200"},
}

// TestMutatedInputsWithComparisonFlags reruns the mutation matrix under the
// comparison flags, one seed per mutator (the flags change how values are
// canonicalized, not how bytes are parsed, so the seed sweep buys little
// here; coverage of the new paths is the point).
func TestMutatedInputsWithComparisonFlags(t *testing.T) {
	right := filepath.Join(fixDir, "right.parquet")
	for _, f := range formats {
		f := f
		t.Run(f.file, func(t *testing.T) {
			t.Parallel()
			orig, err := os.ReadFile(filepath.Join(fixDir, f.file))
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			for _, mu := range mutators {
				if mu.text && !f.text {
					continue
				}
				for fi, flags := range comparisonFlagSets {
					t.Run(fmt.Sprintf("%s/flags%d", mu.name, fi), func(t *testing.T) {
						r := rand.New(rand.NewPCG(uint64(fi), 0x7d1ff))
						p := filepath.Join(dir, fmt.Sprintf("%s-f%d-%s", mu.name, fi, f.file))
						if err := os.WriteFile(p, mu.fn(orig, r), 0o644); err != nil {
							t.Fatal(err)
						}
						args := append([]string{p, right, "--key", "id", "--format", "json"}, flags...)
						assertSurvives(t, runVenn(t, args...))
					})
				}
			}
		})
	}
}

// TestMutatedInputsKeyless runs the keyless multiset diff over the mutation
// matrix. It has its own loop because it takes no --key, and it exercises a
// different engine path (a counted table rather than the keyed join).
func TestMutatedInputsKeyless(t *testing.T) {
	right := filepath.Join(fixDir, "right.parquet")
	for _, f := range formats {
		f := f
		t.Run(f.file, func(t *testing.T) {
			t.Parallel()
			orig, err := os.ReadFile(filepath.Join(fixDir, f.file))
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			for _, mu := range mutators {
				if mu.text && !f.text {
					continue
				}
				t.Run(mu.name, func(t *testing.T) {
					r := rand.New(rand.NewPCG(0, 0x7d1ff))
					p := filepath.Join(dir, "keyless-"+mu.name+"-"+f.file)
					if err := os.WriteFile(p, mu.fn(orig, r), 0o644); err != nil {
						t.Fatal(err)
					}
					assertSurvives(t, runVenn(t, p, right, "--keyless", "--format", "json"))
				})
			}
		})
	}
}

// TestTruncationLadder cuts each format at evenly spaced sizes from empty to
// full: the classic torn-write simulation (a partial upload, a full disk).
func TestTruncationLadder(t *testing.T) {
	right := filepath.Join(fixDir, "right.parquet")
	const steps = 16
	for _, f := range formats {
		f := f
		t.Run(f.file, func(t *testing.T) {
			t.Parallel()
			orig, err := os.ReadFile(filepath.Join(fixDir, f.file))
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			for i := 0; i <= steps; i++ {
				n := len(orig) * i / steps
				t.Run(fmt.Sprintf("%d of %d bytes", n, len(orig)), func(t *testing.T) {
					p := filepath.Join(dir, fmt.Sprintf("trunc-%d-%s", i, f.file))
					if err := os.WriteFile(p, orig[:n], 0o644); err != nil {
						t.Fatal(err)
					}
					res := runVenn(t, p, right, "--key", "id", "--format", "json")
					assertSurvives(t, res)
				})
			}
		})
	}
}

// TestCorruptSnapshot mutates the .snap baseline and diffs against it.
func TestCorruptSnapshot(t *testing.T) {
	left := filepath.Join(fixDir, "left.parquet")
	orig, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, mu := range mutators {
		if mu.text {
			continue
		}
		for seed := 0; seed < rounds(); seed++ {
			t.Run(fmt.Sprintf("%s/seed%d", mu.name, seed), func(t *testing.T) {
				r := rand.New(rand.NewPCG(uint64(seed), 0x54a9))
				p := filepath.Join(dir, fmt.Sprintf("%s-%d.snap", mu.name, seed))
				if err := os.WriteFile(p, mu.fn(orig, r), 0o644); err != nil {
					t.Fatal(err)
				}
				res := runVenn(t, left, "--against", p, "--format", "json")
				assertSurvives(t, res)
			})
		}
	}
}

// TestDegenerateFiles covers the structured edge cases mutation rarely hits.
func TestDegenerateFiles(t *testing.T) {
	right := filepath.Join(fixDir, "right.parquet")
	cases := []struct {
		name    string
		file    string
		content string
	}{
		{"empty", "empty.csv", ""},
		{"header-only", "header.csv", "id,a,b\n"},
		{"header-no-newline", "hdrnn.csv", "id,a,b"},
		{"only-newlines", "nl.csv", "\n\n\n\n"},
		{"bom-header", "bom.csv", "\xef\xbb\xbfid,a\n1,x\n2,y\n"},
		{"ragged-extra-col", "ragged1.csv", "id,a\n1,x\n2,y,EXTRA\n3,z\n"},
		{"ragged-missing-col", "ragged2.csv", "id,a,b\n1,x,q\n2,y\n3,z,w\n"},
		{"dup-header", "dup.csv", "id,a,a\n1,x,y\n"},
		{"unterminated-quote", "quote.csv", "id,a\n1,\"unclosed\n2,y\n"},
		{"crlf-mixed", "crlf.csv", "id,a\r\n1,x\n2,y\r\n"},
		{"empty-ndjson", "empty.ndjson", ""},
		{"garbage-json", "bad.ndjson", "{\"id\":1}\nNOT JSON AT ALL\n{\"id\":2}\n"},
		{"parquet-magic-only", "magic.parquet", "PAR1"},
		{"parquet-fake", "fake.parquet", "PAR1 this is not a parquet file PAR1"},
	}
	dir := t.TempDir()
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			p := filepath.Join(dir, c.file)
			if err := os.WriteFile(p, []byte(c.content), 0o644); err != nil {
				t.Fatal(err)
			}
			res := runVenn(t, p, right, "--key", "id", "--format", "json")
			assertSurvives(t, res)
		})
	}
}

// --- valid-but-nasty inputs: exact answers required ------------------------

func csvQuote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// TestNastyValidCSV builds legal CSV full of hostile content (embedded
// newlines, quotes, commas, unicode) and requires exact diff results, not
// mere survival: identical sides must report equal, a single edit must
// report exactly one changed row.
func TestNastyValidCSV(t *testing.T) {
	nasty := []string{
		"plain",
		"comma, inside",
		"line\nbreak",
		"crlf\r\nbreak",
		"quo\"te",
		"both\n\"and\", more",
		"ünïcødé ✓ 田中",
		"",
		"   padded   ",
		strings.Repeat("x", 9000), // > any internal line buffer
	}
	var b strings.Builder
	b.WriteString("id,payload,n\n")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, "%d,%s,%d\n", i, csvQuote(nasty[i%len(nasty)]+fmt.Sprint(i)), i*3)
	}
	dir := t.TempDir()
	a := filepath.Join(dir, "a.csv")
	if err := os.WriteFile(a, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("identical", func(t *testing.T) {
		res := runVenn(t, a, a, "--key", "id", "--format", "json")
		assertSurvives(t, res)
		if res.exit != 0 {
			t.Errorf("identical nasty CSVs: exit %d, want 0\nstderr: %s", res.exit, tail(res.stderr))
		}
	})

	t.Run("one-edit", func(t *testing.T) {
		edited := strings.Replace(b.String(), ",9\n", ",999\n", 1) // row id=3: n 9 -> 999
		c := filepath.Join(dir, "c.csv")
		if err := os.WriteFile(c, []byte(edited), 0o644); err != nil {
			t.Fatal(err)
		}
		res := runVenn(t, a, c, "--key", "id", "--format", "json")
		assertSurvives(t, res)
		var got struct{ Added, Removed, Changed int64 }
		if err := json.Unmarshal([]byte(res.stdout), &got); err != nil {
			t.Fatalf("json: %v", err)
		}
		if got.Added != 0 || got.Removed != 0 || got.Changed != 1 {
			t.Errorf("one edited row: got %+v, want changed=1 only", got)
		}
	})
}

// TestLongLinesAcrossBlocks builds a CSV whose ~8KB rows straddle the 1MB
// parallel-split block boundaries many times (a real corruption bug class in
// the block feeder), and requires exact results.
func TestLongLinesAcrossBlocks(t *testing.T) {
	var b strings.Builder
	b.WriteString("id,blob\n")
	const rows = 400 // ~3.2MB total: crosses several 1MB blocks
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&b, "%d,%s\n", i, strings.Repeat(fmt.Sprintf("v%d-", i), 2000))
	}
	dir := t.TempDir()
	a := filepath.Join(dir, "a.csv")
	if err := os.WriteFile(a, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Run("identical", func(t *testing.T) {
		res := runVenn(t, a, a, "--key", "id", "--format", "json")
		assertSurvives(t, res)
		if res.exit != 0 {
			t.Errorf("identical long-line CSVs: exit %d, want 0\nstderr: %s", res.exit, tail(res.stderr))
		}
	})
	t.Run("one-edit", func(t *testing.T) {
		edited := strings.Replace(b.String(), "v7-v7-", "v7-X7-", 1)
		c := filepath.Join(dir, "c.csv")
		if err := os.WriteFile(c, []byte(edited), 0o644); err != nil {
			t.Fatal(err)
		}
		res := runVenn(t, a, c, "--key", "id", "--format", "json")
		assertSurvives(t, res)
		var got struct{ Added, Removed, Changed int64 }
		if err := json.Unmarshal([]byte(res.stdout), &got); err != nil {
			t.Fatalf("json: %v", err)
		}
		if got.Added != 0 || got.Removed != 0 || got.Changed != 1 {
			t.Errorf("one edited long row: got %+v, want changed=1 only", got)
		}
	})
}
