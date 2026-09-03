package source

import (
	"testing"
	"time"
)

func TestParseFastAgainstStdlib(t *testing.T) {
	cases := []string{
		"2020-01-01T00:00:00.000000Z", "2026-09-03T20:59:59.123456Z",
		"1999-12-31 23:59:59", "2000-02-29T12:00:00Z", "2024-02-29T00:00:00.5Z",
		"1970-01-01T00:00:00Z", "2038-01-19T03:14:07.999999Z",
	}
	for _, c := range cases {
		got, ok := ParseTimestamp(c)
		if !ok {
			t.Fatalf("%s: not parsed", c)
		}
		var want time.Time
		var err error
		for _, l := range []string{"2006-01-02T15:04:05.999999999Z", "2006-01-02 15:04:05"} {
			want, err = time.ParseInLocation(l, c, time.UTC)
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("stdlib cannot parse %s", c)
		}
		if got != want.UnixMicro() {
			t.Errorf("%s: got %d want %d", c, got, want.UnixMicro())
		}
	}
	for d := int32(-100000); d < 100000; d += 37 {
		s := DateDaysToString(d)
		got, ok := ParseDate(s)
		if !ok || got != d {
			t.Fatalf("date roundtrip %s: got %d ok=%v want %d", s, got, ok, d)
		}
	}
	if _, ok := ParseTimestamp("2020-13-01T00:00:00Z"); ok {
		t.Error("month 13 accepted")
	}
	if _, ok := ParseTimestamp("not a date"); ok {
		t.Error("garbage accepted")
	}
}
