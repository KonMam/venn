package diff

// Column masking (--mask).
//
// A diff report is a data export: examples and `--output` carry real cell
// values, which is exactly what you cannot paste into a PR when the column
// holds names, emails or card numbers. Masking replaces those values with a
// short stable token, so a reader can still see *that* a column changed and
// correlate rows across a report — without reading the values.
//
// Masking is display-only. The comparison itself always uses the real
// values, so the counts are unaffected and no flag combination can change
// what "differs" means.
//
// It is not anonymization. The token is a plain hash of the value, so a
// low-cardinality column (a country, a status, a boolean) can be recovered
// by hashing the candidate values. Use it to keep values out of a report,
// not to publish one safely.

import (
	"encoding/hex"
	"fmt"

	"github.com/cespare/xxhash/v2"

	"github.com/KonMam/tdiff/internal/source"
)

// maskTokenBytes is how many hash bytes a token shows. Four bytes is enough
// that two different values in one report practically never collide, and
// short enough to stay readable in a table.
const maskTokenBytes = 4

// maskToken renders a value as its stable token. NULL stays NULL: the
// comparison already reports nullability, and hiding it would make a
// masked column's report unreadable without protecting anything.
func maskToken(v *source.Value) string {
	if v.Null {
		return "NULL"
	}
	var buf [8]byte
	h := xxhash.Sum64String(v.Display())
	for i := range buf {
		buf[i] = byte(h >> (8 * (7 - i)))
	}
	return "xxh:" + hex.EncodeToString(buf[:maskTokenBytes])
}

// resolveMask marks which key and compared columns are masked. Naming a
// column that is not part of the comparison is an error, not a silently
// ignored flag — a typo in a masking flag must never leak values.
func (p *plan) resolveMask(names []string) error {
	if len(names) == 0 {
		return nil
	}
	p.maskKey = make([]bool, len(p.keyNames))
	p.maskVal = make([]bool, len(p.valNames))
	for _, name := range names {
		found := false
		for i, k := range p.keyNames {
			if k == name {
				p.maskKey[i], found = true, true
			}
		}
		for i, v := range p.valNames {
			if v == name {
				p.maskVal[i], found = true, true
			}
		}
		if !found {
			return fmt.Errorf("--mask names column %q, which is not compared (not in both inputs, or ignored)", name)
		}
	}
	p.hasMask = true
	return nil
}

// maskRow replaces the masked entries of a gathered row with their tokens, in
// place. vals must be a gathered copy, never a batch's own storage.
func maskRow(vals []source.Value, mask []bool) {
	if mask == nil {
		return
	}
	for i := range vals {
		if mask[i] {
			vals[i] = source.Value{Type: source.TypeString, Str: maskToken(&vals[i])}
		}
	}
}

// maskedNames returns the masked column names, for the report.
func (p *plan) maskedNames() []string {
	if !p.hasMask {
		return nil
	}
	var out []string
	for i, m := range p.maskKey {
		if m {
			out = append(out, p.keyNames[i])
		}
	}
	for i, m := range p.maskVal {
		if m {
			out = append(out, p.valNames[i])
		}
	}
	return out
}
