package source

import "testing"

// parseDateFast must reject dates that are lexically well-formed but not on
// the calendar (Feb 30, Apr 31, Feb 29 outside leap years), not just bad
// digits and delimiters.

func TestParseDateValidity(t *testing.T) {
	cases := []struct {
		in string
		ok bool
	}{
		{"2023-02-28", true},
		{"2023-02-29", false}, // 2023 is not a leap year
		{"2023-02-30", false},
		{"2023-04-31", false}, // April has 30 days
		{"2023-04-30", true},
		{"2024-02-29", true}, // divisible by 4
		{"2000-02-29", true}, // divisible by 400
		{"1900-02-28", true},
		{"1900-02-29", false}, // divisible by 100 but not 400: not leap
		{"2023-12-31", true},
		{"2023-13-01", false},
		{"2023-00-15", false},
		{"2023-01-00", false},
		{"2023-01-32", false},
		{"2023/01/15", false},
		{"2023-1-15", false}, // wrong length
		{"20230115", false},
		{"", false},
	}
	for _, c := range cases {
		days, ok := ParseDate(c.in)
		if ok != c.ok {
			t.Errorf("ParseDate(%q) ok = %v, want %v", c.in, ok, c.ok)
			continue
		}
		// Every accepted date must render back to itself.
		if ok {
			if got := DateDaysToString(days); got != c.in {
				t.Errorf("ParseDate(%q) = %d days, renders as %q", c.in, days, got)
			}
		}
	}
}

func TestParseDateEpoch(t *testing.T) {
	cases := []struct {
		in   string
		days int32
	}{
		{"1970-01-01", 0},
		{"1970-01-02", 1},
		{"1969-12-31", -1},
		{"2000-03-01", 11017},
	}
	for _, c := range cases {
		days, ok := ParseDate(c.in)
		if !ok || days != c.days {
			t.Errorf("ParseDate(%q) = %d, %v; want %d, true", c.in, days, ok, c.days)
		}
	}
}

func TestParseTimestampDateValidity(t *testing.T) {
	// An invalid calendar date must not parse in any accepted layout.
	for _, in := range []string{
		"2023-02-30T00:00:00Z",
		"2023-02-30 00:00:00",
		"2023-04-31T12:00:00.000000Z",
		"2023-02-29T00:00:00Z",
	} {
		if us, ok := ParseTimestamp(in); ok {
			t.Errorf("ParseTimestamp(%q) = %d, want rejection", in, us)
		}
	}
	// Leap day parses and round-trips.
	us, ok := ParseTimestamp("2024-02-29T12:34:56Z")
	if !ok {
		t.Fatal("ParseTimestamp rejected 2024-02-29")
	}
	if got := TimestampMicrosToString(us); got != "2024-02-29T12:34:56.000000Z" {
		t.Errorf("round-trip = %q", got)
	}
}
