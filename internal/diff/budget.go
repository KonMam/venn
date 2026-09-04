package diff

// Early abort on --max-diff.
//
// A CI gate that only says "too many rows differ" does not need the exact
// count: once added+changed alone exceeds the budget, the verdict is fixed no
// matter what the rest of the scan would find (removed rows can only add to
// the total). So the run stops there.
//
// The catch is the percentage form. Its denominator is max(leftRows,
// rightRows), and rightRows is only known up front when the format carries
// it (parquet footers, lake manifests). Without it the budget can still grow
// as rows arrive, so no threshold is provable and the run goes to
// completion. The counts stay exact; only the shortcut is lost.

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/KonMam/venn/internal/source"
)

// ParseBudget turns a --max-diff spec ("1000" or "0.5%") into an absolute
// row budget against a row count.
func ParseBudget(spec string, rows int64) (int64, error) {
	if pct, ok := parsePercent(spec); ok {
		return int64(pct / 100 * float64(rows)), nil
	}
	if strings.HasSuffix(spec, "%") {
		return 0, fmt.Errorf("bad --max-diff percentage %q", spec)
	}
	n, err := strconv.ParseInt(spec, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad --max-diff %q (want a count or percentage)", spec)
	}
	return n, nil
}

// parsePercent parses "0.5%" into 0.5.
func parsePercent(spec string) (float64, bool) {
	rest, ok := strings.CutSuffix(spec, "%")
	if !ok {
		return 0, false
	}
	pct, err := strconv.ParseFloat(rest, 64)
	if err != nil || pct < 0 {
		return 0, false
	}
	return pct, true
}

// abortThreshold returns the added+changed count beyond which the run may
// stop early, and whether stopping early is possible at all. leftRows must
// already be final (pass 1 done).
func abortThreshold(opts *Options, leftRows int64, right source.Source) (int64, bool, error) {
	if opts.MaxDiff == "" {
		return 0, false, nil
	}
	if opts.Sink != nil {
		// --output promises the differing rows; half a file is worse than a
		// slower run
		return 0, false, nil
	}
	if opts.OnDup == "match" {
		// multiset matching defers a row's classification until both sides'
		// leftovers are known, so counts taken mid-scan are not a meaningful
		// lower bound. The budget is still enforced, just at the end.
		return 0, false, nil
	}
	if pct, ok := parsePercent(opts.MaxDiff); ok {
		nr, ok := right.(interface{ NumRows() int64 })
		if !ok || nr.NumRows() <= 0 {
			return 0, false, nil // denominator not yet final: no shortcut
		}
		return int64(pct / 100 * float64(max(leftRows, nr.NumRows()))), true, nil
	}
	n, err := ParseBudget(opts.MaxDiff, 0)
	if err != nil {
		return 0, false, err
	}
	return n, true, nil
}

// errBudgetExceeded cancels a scan once the budget is provably blown.
type errBudgetExceeded struct {
	seen   int64
	budget int64
}

func (e errBudgetExceeded) Error() string {
	return fmt.Sprintf("--max-diff budget of %d exceeded (%d differing rows found so far)", e.budget, e.seen)
}
