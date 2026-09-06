package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KonMam/tdiff/internal/fixture"
)

// capture runs the CLI with os.Stdout and os.Stderr redirected to temp files,
// so a test can assert on which stream something landed on. run() writes
// through the package-level os handles, which is the same path a real
// invocation takes.
func capture(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	dir := t.TempDir()
	outF, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	errF, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	origOut, origErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outF, errF
	defer func() {
		os.Stdout, os.Stderr = origOut, origErr
		outF.Close()
		errF.Close()
	}()

	code = run(args)

	outF.Close()
	errF.Close()
	o, err := os.ReadFile(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := os.ReadFile(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	return code, string(o), string(e)
}

// csvPair generates a small fixture pair and returns the two paths plus the
// planted ground truth.
func csvPair(t *testing.T, rows int64) (left, right string, man *fixture.Manifest) {
	t.Helper()
	dir := t.TempDir()
	man, err := fixture.Generate(fixture.Config{
		Rows: rows, Seed: 11, Out: dir,
		PctChanged: 0.05, PctAdded: 0.02, PctRemoved: 0.02,
		Formats: []string{"csv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "left.csv"), filepath.Join(dir, "right.csv"), man
}

// Explicitly requested help goes to stdout and exits 0, so `tdiff --help |
// less` works. A usage error sends the same text to stderr instead.
func TestHelpGoesToStdout(t *testing.T) {
	for _, arg := range []string{"--help", "-h"} {
		code, stdout, stderr := capture(t, arg)
		if code != 0 {
			t.Errorf("%s: exit = %d, want 0", arg, code)
		}
		if !strings.Contains(stdout, "usage:") || !strings.Contains(stdout, "--key") {
			t.Errorf("%s: reference missing from stdout: %q", arg, stdout)
		}
		if stderr != "" {
			t.Errorf("%s: stderr not empty: %q", arg, stderr)
		}
	}
}

// The help header has to name what the tool actually reads; it drifted once
// already and left --help advertising a strict subset of the real formats.
func TestHelpHeaderNamesEveryInputKind(t *testing.T) {
	_, stdout, _ := capture(t, "--help")
	for _, want := range []string{"parquet", "csv", "ndjson", "Iceberg", "Delta", "s3://"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("--help header does not mention %q", want)
		}
	}
}

func TestVersion(t *testing.T) {
	code, stdout, _ := capture(t, "--version")
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(stdout, version) {
		t.Errorf("stdout = %q, want it to contain %q", stdout, version)
	}
}

// A bare invocation has nothing to act on, so the reference is the most
// useful thing to print.
func TestBareInvocationPrintsReference(t *testing.T) {
	code, _, stderr := capture(t)
	if code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr, "usage:") {
		t.Errorf("stderr = %q, want the reference", stderr)
	}
}

// Every malformed invocation reports the problem in one line and points at
// --help, rather than printing sixty lines of flags over the diagnostic.
func TestUsageErrorsAreOneLine(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"a.csv", "b.csv", "--keys", "id"}, "not defined"},
		{"one input", []string{"a.csv"}, "need two inputs"},
		{"three inputs", []string{"a.csv", "b.csv", "c.csv"}, "need two inputs"},
		{"snapshot without --output", []string{"snapshot", "a.csv"}, "usage: tdiff snapshot"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := capture(t, tc.args...)
			if code != 2 {
				t.Fatalf("exit = %d, want 2", code)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr, tc.want)
			}
			if !strings.Contains(stderr, "tdiff --help") {
				t.Errorf("stderr = %q, want a pointer to --help", stderr)
			}
			// the whole flag reference must not be dumped over the diagnostic
			if strings.Contains(stderr, "--timestamp-precision") {
				t.Errorf("stderr dumped the full reference:\n%s", stderr)
			}
			if n := strings.Count(stderr, "\n"); n > 2 {
				t.Errorf("stderr is %d lines, want at most 2:\n%s", n, stderr)
			}
		})
	}
}

// Malformed option values are rejected before any input is opened, so the
// error names the flag rather than a missing file.
func TestFlagValidationBeforeOpeningInputs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"on-dup", []string{"nonexistent-a.csv", "nonexistent-b.csv", "--on-dup", "sometimes"}, "--on-dup"},
		{"max-diff", []string{"nonexistent-a.csv", "nonexistent-b.csv", "--max-diff", "loads"}, "max-diff"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := capture(t, tc.args...)
			if code != 2 {
				t.Fatalf("exit = %d, want 2", code)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr = %q, want it to name %q", stderr, tc.want)
			}
			if strings.Contains(stderr, "no such file") {
				t.Errorf("opened an input before validating the flag: %q", stderr)
			}
		})
	}
}

// The combinations the engine cannot honor are refused with an explanation,
// not silently ignored.
func TestRefusedCombinations(t *testing.T) {
	left, right, _ := csvPair(t, 500)
	snap := filepath.Join(t.TempDir(), "base.snap")

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"tolerance with summary", []string{left, right, "--key", "id", "--summary", "--tolerance", "0.5"}, "--tolerance"},
		{"tolerance into a snapshot", []string{"snapshot", left, "--key", "id", "--output", snap, "--tolerance", "0.5"}, "--tolerance"},
		{"keyless snapshot", []string{"snapshot", left, "--key", "id", "--output", snap, "--keyless"}, "--keyless"},
		{"rename into a snapshot", []string{"snapshot", left, "--key", "id", "--output", snap, "--rename", "b=a"}, "--rename"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := capture(t, tc.args...)
			if code != 2 {
				t.Fatalf("exit = %d, want 2 (stderr: %s)", code, stderr)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Errorf("stderr = %q, want it to name %q", stderr, tc.want)
			}
		})
	}
}

// Exit codes are the CI contract: 0 equal or within budget, 1 differences,
// 2 error.
func TestExitCodes(t *testing.T) {
	left, right, man := csvPair(t, 2000)
	differing := man.Added + man.Removed + man.Changed

	t.Run("identical is 0", func(t *testing.T) {
		if code, _, e := capture(t, left, left, "--key", "id"); code != 0 {
			t.Errorf("exit = %d, want 0 (%s)", code, e)
		}
	})
	t.Run("differences are 1", func(t *testing.T) {
		if code, _, e := capture(t, left, right, "--key", "id"); code != 1 {
			t.Errorf("exit = %d, want 1 (%s)", code, e)
		}
	})
	t.Run("within budget is 0", func(t *testing.T) {
		big := strings.TrimSpace(strings.Repeat(" ", 0) + itoa(differing+1))
		if code, _, e := capture(t, left, right, "--key", "id", "--max-diff", big); code != 0 {
			t.Errorf("exit = %d, want 0 (%s)", code, e)
		}
	})
	t.Run("over budget is 1", func(t *testing.T) {
		if code, _, e := capture(t, left, right, "--key", "id", "--max-diff", "1"); code != 1 {
			t.Errorf("exit = %d, want 1 (%s)", code, e)
		}
	})
	t.Run("missing input is 2", func(t *testing.T) {
		if code, _, _ := capture(t, "nope.csv", right, "--key", "id"); code != 2 {
			t.Errorf("exit = %d, want 2", code)
		}
	})
	t.Run("unknown key column is 2", func(t *testing.T) {
		code, _, stderr := capture(t, left, right, "--key", "nosuchcol")
		if code != 2 {
			t.Errorf("exit = %d, want 2", code)
		}
		if !strings.Contains(stderr, "nosuchcol") {
			t.Errorf("stderr = %q, want it to name the missing column", stderr)
		}
	})
}

// Flags are accepted on either side of the positional arguments.
func TestFlagsBeforeAndAfterPositionals(t *testing.T) {
	left, right, _ := csvPair(t, 500)
	before, _, _ := capture(t, "--key", "id", "--format", "json", left, right)
	after, _, _ := capture(t, left, right, "--key", "id", "--format", "json")
	if before != after {
		t.Errorf("exit differs by flag position: before=%d after=%d", before, after)
	}
}

// --format json emits the counts the Action reads out of it, and they match
// the planted ground truth.
func TestJSONOutputMatchesManifest(t *testing.T) {
	left, right, man := csvPair(t, 3000)
	code, stdout, stderr := capture(t, left, right, "--key", "id", "--format", "json")
	if code != 1 {
		t.Fatalf("exit = %d, want 1 (%s)", code, stderr)
	}
	var got struct {
		Added     int64 `json:"added"`
		Removed   int64 `json:"removed"`
		Changed   int64 `json:"changed"`
		LeftRows  int64 `json:"left_rows"`
		RightRows int64 `json:"right_rows"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, stdout)
	}
	if got.Added != man.Added || got.Removed != man.Removed || got.Changed != man.Changed {
		t.Errorf("counts = +%d -%d ~%d, manifest = +%d -%d ~%d",
			got.Added, got.Removed, got.Changed, man.Added, man.Removed, man.Changed)
	}
	if got.LeftRows != man.RowsLeft || got.RightRows != man.RowsRight {
		t.Errorf("rows = %d/%d, manifest = %d/%d",
			got.LeftRows, got.RightRows, man.RowsLeft, man.RowsRight)
	}
}

// The diff summary belongs on stdout so it can be redirected; progress and
// advisories belong on stderr so they do not corrupt it.
func TestJSONOnStdoutOnly(t *testing.T) {
	left, right, _ := csvPair(t, 500)
	// no --key: the inferred-key advisory must not land in the JSON
	_, stdout, stderr := capture(t, left, right, "--format", "json")
	if !json.Valid([]byte(stdout)) {
		t.Errorf("stdout is not valid JSON:\n%s", stdout)
	}
	if !strings.Contains(stderr, "inferred key") {
		t.Errorf("inferred-key advisory missing from stderr: %q", stderr)
	}
}

// schema is its own command and stops after the schema comparison.
func TestSchemaCommand(t *testing.T) {
	left, right, _ := csvPair(t, 500)
	code, stdout, _ := capture(t, "schema", left, right)
	if code != 0 {
		t.Errorf("identical schemas: exit = %d, want 0", code)
	}
	if !strings.Contains(stdout, "identical") {
		t.Errorf("stdout = %q", stdout)
	}
}

// snapshot writes a baseline that --against then diffs without the original.
func TestSnapshotRoundTrip(t *testing.T) {
	left, right, man := csvPair(t, 2000)
	snap := filepath.Join(t.TempDir(), "base.snap")

	if code, _, stderr := capture(t, "snapshot", left, "--key", "id", "--output", snap); code != 0 {
		t.Fatalf("snapshot: exit = %d, want 0 (%s)", code, stderr)
	}
	if st, err := os.Stat(snap); err != nil || st.Size() == 0 {
		t.Fatalf("snapshot file not written: %v", err)
	}
	if code, _, stderr := capture(t, left, "--against", snap); code != 0 {
		t.Errorf("same file vs its own baseline: exit = %d, want 0 (%s)", code, stderr)
	}
	code, stdout, _ := capture(t, right, "--against", snap, "--format", "json")
	if code != 1 {
		t.Errorf("changed file vs baseline: exit = %d, want 1", code)
	}
	var got struct {
		Changed int64 `json:"changed"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if got.Changed != man.Changed {
		t.Errorf("changed = %d, manifest = %d", got.Changed, man.Changed)
	}
}

// --output writes the differing rows as data next to the report.
func TestOutputExport(t *testing.T) {
	left, right, _ := csvPair(t, 1000)
	out := filepath.Join(t.TempDir(), "diff.csv")
	if code, _, stderr := capture(t, left, right, "--key", "id", "--output", out); code != 1 {
		t.Fatalf("exit = %d, want 1 (%s)", code, stderr)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	header, _, _ := strings.Cut(string(b), "\n")
	for _, want := range []string{"id", "diff_status", "__left", "__right"} {
		if !strings.Contains(header, want) {
			t.Errorf("export header %q missing %q", header, want)
		}
	}
}

// --report dispatches on extension, and the HTML page must be self-contained:
// a CI artifact that phones out is not one.
func TestReportFiles(t *testing.T) {
	left, right, _ := csvPair(t, 500)
	dir := t.TempDir()
	md := filepath.Join(dir, "r.md")
	html := filepath.Join(dir, "r.html")
	if code, _, stderr := capture(t, left, right, "--key", "id", "--report", md, "--report", html); code != 1 {
		t.Fatalf("exit = %d, want 1 (%s)", code, stderr)
	}
	mdB, err := os.ReadFile(md)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mdB), "|") {
		t.Errorf("markdown report has no table:\n%s", mdB)
	}
	htmlB, err := os.ReadFile(html)
	if err != nil {
		t.Fatal(err)
	}
	page := string(htmlB)
	if !strings.Contains(page, "<html") && !strings.Contains(page, "<!DOCTYPE") {
		t.Errorf("html report is not a page:\n%.200s", page)
	}
	for _, bad := range []string{"src=\"http", "href=\"http", "@import url(http"} {
		if strings.Contains(page, bad) {
			t.Errorf("html report is not self-contained, found %q", bad)
		}
	}
}

// --report with no usable path is a usage error, not a silent no-op.
func TestReportRequiresPath(t *testing.T) {
	left, right, _ := csvPair(t, 200)
	code, _, stderr := capture(t, left, right, "--key", "id", "--report", "")
	if code != 2 {
		t.Errorf("exit = %d, want 2 (%s)", code, stderr)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
