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
// carries no zone.
func ParseTimestamp(s string) (int64, bool) {
	for _, layout := range timestampLayouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UnixMicro(), true
		}
	}
	return 0, false
}

// ParseDate parses YYYY-MM-DD into days since epoch.
func ParseDate(s string) (int32, bool) {
	t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		return 0, false
	}
	return int32(t.Unix() / 86400), true
}
