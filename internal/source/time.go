package source

import "time"

// TimestampMicrosToString renders µs-since-epoch as RFC3339 with µs precision.
func TimestampMicrosToString(us int64) string {
	return time.UnixMicro(us).UTC().Format("2006-01-02T15:04:05.000000Z")
}

// DateDaysToString renders days-since-epoch as YYYY-MM-DD.
func DateDaysToString(days int32) string {
	return time.Unix(int64(days)*86400, 0).UTC().Format("2006-01-02")
}

// timestampLayouts are accepted when inferring/parsing CSV timestamps,
// tried in order.
var timestampLayouts = []string{
	"2006-01-02T15:04:05.000000Z",
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
}

// ParseTimestamp parses s into µs since epoch, UTC assumed when the layout
// carries no zone. The common fixed layouts are parsed by hand (time.Parse
// costs ~10× more and dominates CSV scans otherwise).
func ParseTimestamp(s string) (int64, bool) {
	if us, ok := parseTimestampFast(s); ok {
		return us, true
	}
	for _, layout := range timestampLayouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UnixMicro(), true
		}
	}
	return 0, false
}

// digits2 parses s[i:i+2] as two ASCII digits.
func digits2(s string, i int) (int, bool) {
	a, b := s[i]-'0', s[i+1]-'0'
	if a > 9 || b > 9 {
		return 0, false
	}
	return int(a)*10 + int(b), true
}

// parseTimestampFast handles "YYYY-MM-DD[T ]HH:MM:SS[.ffffff][Z]" without
// zone offsets.
func parseTimestampFast(s string) (int64, bool) {
	if len(s) < 19 || (s[10] != 'T' && s[10] != ' ') || s[13] != ':' || s[16] != ':' {
		return 0, false
	}
	days, ok := parseDateFast(s[:10])
	if !ok {
		return 0, false
	}
	hh, ok1 := digits2(s, 11)
	mm, ok2 := digits2(s, 14)
	ss, ok3 := digits2(s, 17)
	if !ok1 || !ok2 || !ok3 || hh > 23 || mm > 59 || ss > 60 {
		return 0, false
	}
	rest := s[19:]
	var frac int64
	if len(rest) > 0 && rest[0] == '.' {
		rest = rest[1:]
		n := 0
		for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
			n++
		}
		if n == 0 || n > 9 {
			return 0, false
		}
		for i := 0; i < n && i < 6; i++ {
			frac = frac*10 + int64(rest[i]-'0')
		}
		for i := n; i < 6; i++ {
			frac *= 10
		}
		rest = rest[n:]
	}
	if len(rest) > 0 {
		if rest != "Z" {
			return 0, false // zone offsets go through time.Parse
		}
	}
	sec := int64(days)*86400 + int64(hh)*3600 + int64(mm)*60 + int64(ss)
	return sec*1_000_000 + frac, true
}

// ParseDate parses YYYY-MM-DD into days since epoch.
func ParseDate(s string) (int32, bool) {
	if len(s) != 10 {
		return 0, false
	}
	return parseDateFast(s)
}

// parseDateFast converts a "YYYY-MM-DD" prefix to days since epoch using
// civil-date math (Howard Hinnant's algorithm), no time.Time involved.
func parseDateFast(s string) (int32, bool) {
	if s[4] != '-' || s[7] != '-' {
		return 0, false
	}
	y2a, oka := digits2(s, 0)
	y2b, okb := digits2(s, 2)
	m, okm := digits2(s, 5)
	d, okd := digits2(s, 8)
	if !oka || !okb || !okm || !okd || m < 1 || m > 12 || d < 1 || d > 31 {
		return 0, false
	}
	y := y2a*100 + y2b
	if m <= 2 {
		y--
	}
	era := y / 400
	yoe := y - era*400
	mp := (int(m) + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return int32(era*146097 + doe - 719468), true
}
