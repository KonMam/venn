package source

// Same-table snapshot pruning: when the two sides of a diff are the same
// Iceberg/Delta table at different snapshots, data files present in both
// snapshots contribute identical rows to both sides (copy-on-write tables
// hold each live row in exactly one file), so they cancel exactly and only
// the symmetric difference of the file sets needs scanning. Their row counts
// come from the table metadata and are reported so the caller can fold them
// back into the totals.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PairInfo reports what same-table pruning did (zero value = no pruning).
type PairInfo struct {
	Table       string // table root when pruning applied
	SharedFiles int    // data files present in both snapshots, skipped
	SharedRows  int64  // rows each skipped file set contributes per side
}

// splitSnapshot splits table#snapshot addressing. Locally the suffix is
// taken only when the prefix is an existing directory (same rule as
// OpenWith); for s3:// the '#' is unambiguous — object keys addressing
// tables don't contain it.
func splitSnapshot(path string) (base, snapshot string) {
	if i := strings.LastIndex(path, "#"); i > 0 {
		if isRemote(path) {
			return path[:i], path[i+1:]
		}
		if st, err := os.Stat(path[:i]); err == nil && st.IsDir() {
			return path[:i], path[i+1:]
		}
	}
	return path, ""
}

// OpenPair opens both sides of a diff, applying same-table snapshot pruning
// when possible and falling back to two independent opens otherwise.
func OpenPair(leftPath, rightPath string, o Options) (Source, Source, PairInfo, error) {
	var none PairInfo
	lb, ls := splitSnapshot(leftPath)
	rb, rs := splitSnapshot(rightPath)
	lb, rb = strings.TrimSuffix(lb, "/"), strings.TrimSuffix(rb, "/")
	tableCapable := func(p string) bool {
		return !isRemote(p) || strings.HasPrefix(p, "s3://")
	}
	samePair := lb == rb
	if !isRemote(lb) {
		samePair = filepath.Clean(lb) == filepath.Clean(rb)
	}
	if tableCapable(leftPath) && tableCapable(rightPath) &&
		!strings.ContainsAny(leftPath+rightPath, "*?[") && samePair {
		var list func(dir, snap string) ([]TableFile, string, error)
		switch {
		case isIcebergTable(lb):
			list = ListIcebergFiles
		case isDeltaTable(lb):
			list = ListDeltaFiles
		}
		if list != nil {
			return openPrunedPair(lb, ls, rs, list, o)
		}
	}
	left, err := OpenWith(leftPath, o)
	if err != nil {
		return nil, nil, none, err
	}
	right, err := OpenWith(rightPath, o)
	if err != nil {
		left.Close()
		return nil, nil, none, err
	}
	return left, right, none, nil
}

// openPrunedPair lists both snapshots of one table and opens only the files
// unique to each side.
func openPrunedPair(dir, leftSnap, rightSnap string, list func(string, string) ([]TableFile, string, error), o Options) (Source, Source, PairInfo, error) {
	var none PairInfo
	lf, llabel, err := list(dir, leftSnap)
	if err != nil {
		return nil, nil, none, err
	}
	rf, rlabel, err := list(dir, rightSnap)
	if err != nil {
		return nil, nil, none, err
	}

	// A file is prunable when it is live in both snapshots with a known,
	// matching row count AND an identical merge-on-read delete state —
	// otherwise the same physical file contributes different rows to each
	// side. Equality deletes make the live row count unknowable from
	// metadata, so files carrying them always stay. Non-prunable shared
	// files remain on both sides and cancel through the normal diff.
	rightBy := make(map[string]TableFile, len(rf))
	for _, f := range rf {
		rightBy[f.Path] = f
	}
	prunableRows := func(l, r TableFile) (int64, bool) {
		if l.Rows <= 0 || l.Rows != r.Rows {
			return 0, false
		}
		if l.deletes.empty() && r.deletes.empty() {
			return l.Rows, true
		}
		if l.deletes.empty() != r.deletes.empty() ||
			l.deletes.key != r.deletes.key ||
			len(l.deletes.eq) > 0 || len(r.deletes.eq) > 0 {
			return 0, false
		}
		return l.Rows - l.deletes.posCount(), true
	}
	shared := make(map[string]int64) // path → live rows per side
	for _, f := range lf {
		if r, ok := rightBy[f.Path]; ok {
			if rows, ok := prunableRows(f, r); ok {
				shared[f.Path] = rows
			}
		}
	}

	var lu, ru []TableFile
	for _, f := range lf {
		if _, ok := shared[f.Path]; !ok {
			lu = append(lu, f)
		}
	}
	for _, f := range rf {
		if _, ok := shared[f.Path]; !ok {
			ru = append(ru, f)
		}
	}
	// A side with no files can't be opened (and carries no schema); put the
	// smallest shared file back on both sides — it cancels in the diff.
	if (len(lu) == 0 || len(ru) == 0) && len(shared) > 0 {
		pick := ""
		var pickRows int64
		for _, f := range lf {
			if rows, ok := shared[f.Path]; ok && (pick == "" || rows < pickRows) {
				pick, pickRows = f.Path, rows
			}
		}
		delete(shared, pick)
		for _, f := range lf {
			if f.Path == pick {
				lu = append(lu, f)
			}
		}
		for _, f := range rf {
			if f.Path == pick {
				ru = append(ru, f)
			}
		}
	}

	info := PairInfo{Table: dir}
	for _, rows := range shared {
		info.SharedFiles++
		info.SharedRows += rows
	}

	left, err := openTableFiles(llabel, lu, o)
	if err != nil {
		return nil, nil, none, fmt.Errorf("%s: %w", llabel, err)
	}
	right, err := openTableFiles(rlabel, ru, o)
	if err != nil {
		left.Close()
		return nil, nil, none, fmt.Errorf("%s: %w", rlabel, err)
	}
	return left, right, info, nil
}
