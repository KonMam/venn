package diff

// Key auto-inference: when no --key is given, sample both inputs and pick a
// column that is unique in the sample on both sides, preferring id-ish names
// and integer/string types. The choice is verified for real during the build
// pass (a duplicate fails with a message naming the inferred key).

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"

	"venn/internal/schema"
	"venn/internal/source"
)

// inferSampleLimit rows per side are examined for uniqueness.
const inferSampleLimit = 100_000

var errStopSampling = errors.New("sampling complete")

// InferKey picks a key column for the pair, or errors with guidance.
func InferKey(left, right source.Source) (string, error) {
	sd := schema.Compare(left.Schema(), right.Schema())
	if len(sd.Common) == 0 {
		return "", fmt.Errorf("no comparable common columns to infer a key from")
	}
	uniqL, err := sampleUnique(left, sd.Common)
	if err != nil {
		return "", err
	}
	uniqR, err := sampleUnique(right, sd.Common)
	if err != nil {
		return "", err
	}
	var candidates []string
	for _, c := range sd.Common {
		if uniqL[c] && uniqR[c] {
			candidates = append(candidates, c)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no column is unique in both inputs (sampled %d rows) — pass --key explicitly", inferSampleLimit)
	}
	ls := left.Schema()
	pos := map[string]int{}
	for i, c := range ls.Columns {
		pos[c.Name] = i
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		si, sj := keyScore(candidates[i], &ls), keyScore(candidates[j], &ls)
		if si != sj {
			return si > sj
		}
		return pos[candidates[i]] < pos[candidates[j]]
	})
	return candidates[0], nil
}

// keyScore ranks key candidates: id-ish names first, then integer keys,
// then strings.
func keyScore(name string, s *source.Schema) int {
	score := 0
	ln := strings.ToLower(name)
	switch {
	case ln == "id" || ln == "key" || ln == "pk" || ln == "uuid" || ln == "guid":
		score += 100
	case strings.HasSuffix(ln, "_id") || strings.HasSuffix(ln, "id"):
		score += 50
	}
	switch s.Columns[s.ColumnIndex(name)].Type {
	case source.TypeInt64:
		score += 10
	case source.TypeString:
		score += 5
	}
	return score
}

// sampleUnique reports which of the named columns are duplicate-free within
// the first inferSampleLimit rows.
func sampleUnique(src source.Source, cols []string) (map[string]bool, error) {
	s := src.Schema()
	idx := make([]int, len(cols))
	for i, c := range cols {
		idx[i] = s.ColumnIndex(c)
	}
	seen := make([]map[uint64]struct{}, len(cols))
	dup := make([]bool, len(cols))
	for i := range seen {
		seen[i] = make(map[uint64]struct{}, 4096)
	}
	rows := 0
	// single-threaded scan: sampling stops after inferSampleLimit rows and
	// per-column dedup state stays lock-free
	err := source.Scan(src, 1, func() (source.BatchFunc, error) {
		return func(b *source.Batch) error {
			for r := 0; r < b.N; r++ {
				for i, ci := range idx {
					if dup[i] {
						continue
					}
					v := b.Cols[ci].Value(r)
					var h uint64
					switch {
					case v.Null:
						h = nullSentinel
					case v.Type == source.TypeString || v.Type == source.TypeBytes:
						h = xxhash.Sum64String(v.Str)
					case v.Type == source.TypeFloat64:
						h = canonFloat(v.Float)
					default:
						h = uint64(v.Int)
					}
					if _, ok := seen[i][h]; ok {
						dup[i] = true
						seen[i] = nil
						continue
					}
					seen[i][h] = struct{}{}
				}
				rows++
				if rows >= inferSampleLimit {
					return errStopSampling
				}
			}
			return nil
		}, nil
	})
	if err != nil && !errors.Is(err, errStopSampling) {
		return nil, err
	}
	out := make(map[string]bool, len(cols))
	for i, c := range cols {
		out[c] = !dup[i]
	}
	return out, nil
}
