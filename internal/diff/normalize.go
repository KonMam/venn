package diff

// Value normalization: the canonicalizations venn applies to every value
// before it is hashed *and* before it is compared.
//
// The rule that makes these work everywhere — including --summary, streaming
// and snapshots — is hash consistency: a normalization must be a
// deterministic function of a single value, so both sides map equal logical
// values onto identical bits. Rounding, case folding, whitespace trimming
// and timestamp truncation all qualify. An epsilon tolerance does not (it is
// not transitive), which is why tolerance lives outside the join entirely
// (see tolerance.go).

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"

	"github.com/KonMam/venn/internal/source"
)

// normalizer holds the whole hash-consistent canonicalization set for one
// run. A nil *normalizer means "compare exactly"; every method is nil-safe
// so the disabled path is one branch on a hot, always-predicted nil check.
type normalizer struct {
	quant  *floatQuantizer // --float-precision
	fold   bool            // --ignore-case
	trim   bool            // --trim
	tsUnit int64           // --timestamp-precision, in µs (0 or 1 = exact)
}

// newNormalizer builds the normalizer for these options, or nil when every
// knob is off.
func newNormalizer(opts *Options) (*normalizer, error) {
	n := &normalizer{fold: opts.IgnoreCase, trim: opts.Trim}
	if opts.FloatPrecision > 0 {
		n.quant = newFloatQuantizer(opts.FloatPrecision)
	}
	unit, err := parseTimestampPrecision(opts.TimestampPrecision)
	if err != nil {
		return nil, err
	}
	n.tsUnit = unit
	if n.quant == nil && !n.fold && !n.trim && n.tsUnit <= 1 {
		return nil, nil
	}
	return n, nil
}

// parseTimestampPrecision maps a precision name onto its size in µs.
func parseTimestampPrecision(s string) (int64, error) {
	switch s {
	case "", "us", "µs", "micro", "microsecond":
		return 1, nil
	case "ms", "milli", "millisecond":
		return 1_000, nil
	case "s", "sec", "second":
		return 1_000_000, nil
	default:
		return 0, fmt.Errorf("--timestamp-precision must be s, ms or us, got %q", s)
	}
}

// settings renders the normalization as a stable, comparable string. It goes
// into the snapshot header, so a baseline taken with different settings is
// refused instead of silently compared.
func (n *normalizer) settings() string {
	if n == nil {
		return ""
	}
	var parts []string
	if n.trim {
		parts = append(parts, "trim")
	}
	if n.fold {
		parts = append(parts, "ignore-case")
	}
	if n.tsUnit > 1 {
		switch n.tsUnit {
		case 1_000:
			parts = append(parts, "ts=ms")
		default:
			parts = append(parts, "ts=s")
		}
	}
	return strings.Join(parts, ",")
}

// describeComparison renders every setting that loosened this comparison, for
// the report. A result that was reached under a tolerance or a normalization
// has to say so — otherwise "identical" overstates what was checked.
func (p *plan) describeComparison() string {
	var parts []string
	if n := p.norm; n != nil {
		if n.quant != nil {
			parts = append(parts, fmt.Sprintf("float precision %d", n.quant.digits))
		}
		if n.trim {
			parts = append(parts, "trim")
		}
		if n.fold {
			parts = append(parts, "ignore-case")
		}
		switch n.tsUnit {
		case 1_000:
			parts = append(parts, "timestamp precision ms")
		case 1_000_000:
			parts = append(parts, "timestamp precision s")
		}
	}
	return strings.Join(append(parts, p.tolDesc...), ", ")
}

// --- floats ---------------------------------------------------------------

// canonF is the float canonicalization: quantize (when asked) then collapse
// -0/NaN. Nil-safe.
func (n *normalizer) canonF(f float64) uint64 {
	if n == nil {
		return canonFloat(f)
	}
	return n.quant.canon(f)
}

// --- integers and timestamps ----------------------------------------------

// canonI canonicalizes an integer-domain value. Only timestamps are touched,
// and only under --timestamp-precision: the value is floored onto the coarser
// grid, still in µs, so equal instants at that precision hash alike.
func (n *normalizer) canonI(typ source.Type, v int64) int64 {
	if n == nil || n.tsUnit <= 1 || typ != source.TypeTimestamp {
		return v
	}
	return floorDiv(v, n.tsUnit) * n.tsUnit
}

// floorDiv divides rounding toward negative infinity, so truncation is
// monotonic across the epoch (Go's / truncates toward zero, which would map
// two different pre-epoch seconds onto the same bucket edge as one
// post-epoch second).
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// --- strings --------------------------------------------------------------

// canonStr materializes the canonical form of a string. Used on the compare
// path (pass 3), which is cold — the hot hash path uses hashString instead
// and never allocates for ASCII.
func (n *normalizer) canonStr(s string) string {
	if n == nil {
		return s
	}
	if n.trim {
		s = strings.TrimSpace(s)
	}
	if n.fold {
		s = strings.ToLower(s)
	}
	return s
}

// hashString hashes a string in canonical form. It must agree with
// canonStr exactly: the hash decides the join, the compare decides the
// attribution, and a disagreement would show up as a changed row with no
// changed columns.
func (n *normalizer) hashString(s string) uint64 {
	if n == nil {
		return xxhash.Sum64String(s)
	}
	if n.trim {
		s = strings.TrimSpace(s)
	}
	if !n.fold {
		return xxhash.Sum64String(s)
	}
	return foldedHash(s)
}

// foldedHash hashes strings.ToLower(s) without materializing it when s is
// ASCII — folding happens in a stack buffer, streamed through xxhash, so the
// common case allocates nothing. (xxhash is a streaming hash: writing the
// bytes in chunks yields the same digest as hashing them at once.)
func foldedHash(s string) uint64 {
	ascii, upper := true, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			ascii = false
			break
		}
		if 'A' <= c && c <= 'Z' {
			upper = true
		}
	}
	if !ascii {
		return xxhash.Sum64String(strings.ToLower(s))
	}
	if !upper {
		return xxhash.Sum64String(s)
	}
	var d xxhash.Digest
	d.Reset()
	var buf [256]byte
	for off := 0; off < len(s); off += len(buf) {
		n := copy(buf[:], s[off:])
		for j := 0; j < n; j++ {
			if c := buf[j]; 'A' <= c && c <= 'Z' {
				buf[j] = c + 'a' - 'A'
			}
		}
		_, _ = d.Write(buf[:n])
	}
	return d.Sum64()
}

// describeSettings renders a settings string for an error message.
func describeSettings(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// describeFilter renders a --where setting for an error message.
func describeFilter(s string) string {
	if s == "" {
		return "no filter"
	}
	return "\"" + s + "\""
}
