// Package venn is the embeddable API over the venn engine: keyed,
// format-agnostic diffing of tabular data files (parquet, CSV/TSV, NDJSON,
// optionally gzip/zstd compressed), with in-memory and larger-than-RAM
// streaming join strategies.
//
//	res, err := venn.Diff("a.parquet", "b.csv", venn.Options{Keys: []string{"id"}})
//	if res.Same() { ... }
//
// Every type in this package is self-contained: nothing from the internal
// engine leaks into the API surface. Exporting the differing rows as data
// (the CLI's --output) is not exposed here.
package venn

import (
	"github.com/KonMam/venn/internal/diff"
	"github.com/KonMam/venn/internal/source"
)

// Options mirrors the CLI surface. Zero values mean: inferred key, auto join
// mode, exact float compare, duplicate keys are errors.
type Options struct {
	// Keys are the key column names; empty infers one (unique in both
	// inputs, id-ish names preferred).
	Keys []string
	// Keyless diffs without a key: whole rows are matched as a multiset, so
	// leftovers on either side are added/removed and Changed is always 0.
	// Mutually exclusive with Keys, OnDup and Tolerance.
	Keyless bool
	// IgnoreColumns are excluded from comparison.
	IgnoreColumns []string
	// Rename maps a right-side column name to the left-side name it is
	// compared under, so a renamed column is compared instead of being
	// reported as one added and one removed column.
	Rename map[string]string
	// Mask lists columns whose values are replaced by a stable short token
	// wherever they would be shown. Display-only: the comparison still uses
	// the real values. Not anonymization; see the CLI docs.
	Mask []string
	// Limit caps example rows per category (default 10).
	Limit int
	// Threads caps concurrency (default GOMAXPROCS).
	Threads int
	// Summary computes counts only (fastest; skips column attribution).
	Summary bool
	// Mode is "auto" (default), "memory" or "stream".
	Mode string
	// TempDir is the streaming spill directory (default system temp).
	TempDir string
	// OnDup is "error" (default), "warn" (keep the first occurrence per
	// side) or "match" (pair a duplicated key's rows as multisets:
	// identical rows cancel, leftovers are added/removed, and no change is
	// attributed inside a duplicate group). "match" requires Mode "memory".
	OnDup string
	// FloatPrecision rounds float comparisons to N decimal digits (0 =
	// exact bit-level comparison after -0/NaN canonicalization).
	FloatPrecision int
	// IgnoreCase compares strings case-insensitively.
	IgnoreCase bool
	// Trim ignores leading and trailing whitespace in string comparisons.
	Trim bool
	// TimestampPrecision compares timestamps at "s", "ms" or "us" precision
	// (empty or "us" = exact microseconds).
	TimestampPrecision string
	// Tolerance is the numeric epsilon applied to every numeric column;
	// ColumnTolerance overrides it per column. Row pairs whose every
	// difference falls inside tolerance are counted in
	// Result.WithinTolerance instead of Result.Changed. Incompatible with
	// Summary.
	Tolerance       *Tolerance
	ColumnTolerance map[string]Tolerance
	// MaxDiff is a CI budget ("1000" or "0.5%"). Setting it lets the diff
	// stop as soon as the budget is provably exceeded, marking the Result
	// Aborted; the caller still decides the verdict from the counts.
	MaxDiff string
	// Where filters both inputs to the rows matching every predicate, in
	// CLI syntax ("price > 10", "region = 'eu'", "note IS NULL"). Filtering
	// happens before the diff, so every count is of the matching rows.
	Where []string
	// InferRows is the CSV/NDJSON type-inference sample size (0 = 1000,
	// negative = whole file).
	InferRows int
	// Delimiter overrides the delimited-text field separator: one character,
	// or "\t". Empty means the extension decides.
	Delimiter string
}

// Tolerance is a numeric epsilon: two values are equal when they differ by
// at most Abs, or relatively by at most Rel of the larger magnitude. A zero
// bound is off; at least one must be set.
type Tolerance struct {
	Abs float64
	Rel float64
}

// ParseTolerance parses a CLI-style tolerance spec ("0.01",
// "0.01,rel=1e-6", "rel=1e-6", "price=0.01"), returning the column it
// applies to ("" for every numeric column) and the tolerance.
func ParseTolerance(spec string) (string, Tolerance, error) {
	col, t, err := diff.ParseTolerance(spec)
	return col, Tolerance{Abs: t.Abs, Rel: t.Rel}, err
}

// ColumnStat is one compared column's change profile.
type ColumnStat struct {
	// Changed counts row pairs differing in this column (differences inside
	// tolerance are not counted).
	Changed int64
	// MatchRate is the share of compared row pairs agreeing in this column.
	MatchRate float64
	// MaxAbsDiff/MeanAbsDiff size the differences of a numeric column
	// (int64 and float only, over pairs where neither side is NULL).
	MaxAbsDiff  float64
	MeanAbsDiff float64
	Numeric     bool
}

// TypeChange is one column whose logical type differs between the inputs.
type TypeChange struct {
	Column string
	Left   string // logical (physical) rendering, e.g. "int64 (INT32)"
	Right  string
	// Comparable is false when the types cannot be compared value-by-value
	// (e.g. string vs timestamp); such columns are excluded from the row diff.
	Comparable bool
}

// NullabilityChange is one column whose nullability differs.
type NullabilityChange struct {
	Column        string
	LeftNullable  bool
	RightNullable bool
}

// Rename records a right-side column compared under a left-side name.
type Rename struct {
	Left  string // the name the column is compared under
	Right string // the right input's own name for it
}

// SchemaDiff describes how the two inputs' schemas differ.
type SchemaDiff struct {
	AddedColumns       []string // in right only
	RemovedColumns     []string // in left only
	TypeChanges        []TypeChange
	NullabilityChanges []NullabilityChange
	// Renames lists the columns compared under a left-side name
	// (Options.Rename). They do not make the schemas differ.
	Renames []Rename
}

// Same reports whether the schemas match exactly (ignoring column order).
func (d *SchemaDiff) Same() bool {
	return len(d.AddedColumns) == 0 && len(d.RemovedColumns) == 0 &&
		len(d.TypeChanges) == 0 && len(d.NullabilityChanges) == 0
}

// ColumnChange is one changed cell in an example row.
type ColumnChange struct {
	Column string
	Left   string
	Right  string
}

// RowExample is one example changed row.
type RowExample struct {
	Key     string
	Columns []ColumnChange
}

// Result is the diff outcome.
type Result struct {
	Schema SchemaDiff

	LeftRows  int64
	RightRows int64
	Added     int64
	Removed   int64
	Changed   int64
	Unchanged int64

	// Comparison names the settings that loosened this comparison
	// (normalizations, tolerances); empty for an exact diff.
	Comparison string

	// Masked lists the columns whose values were replaced by a token.
	Masked []string

	// Keyless marks a result produced without a key: examples name whole
	// rows rather than keys, and Changed is always 0.
	Keyless bool

	// Aborted is set when the diff stopped early because Options.MaxDiff was
	// provably exceeded: the remaining work was skipped, so column
	// attribution and examples are absent.
	Aborted bool
	// PartialCounts distinguishes the two ways a diff aborts: true when the
	// probe scan itself was cancelled, so every count is a lower bound;
	// false when the counts are complete and only attribution was skipped.
	PartialCounts bool
	// AbortReason explains an aborted run.
	AbortReason string

	// Filter is the row filter both inputs were reduced by (Options.Where).
	Filter string
	// FilesPruned counts the data files partition pruning skipped.
	FilesPruned int

	// WithinTolerance counts row pairs whose every difference fell inside
	// Options.Tolerance; excluded from Changed.
	WithinTolerance int64

	// ColumnChanges counts, per column, how many changed rows changed in
	// that column (empty under Options.Summary).
	ColumnChanges map[string]int64

	// ColumnStats carries the same counts plus a match rate and, for
	// numeric columns, the size of the differences.
	ColumnStats map[string]ColumnStat

	// DupsLeft/DupsRight count rows set aside under OnDup "warn" (the first
	// occurrence of each key stays in the diff).
	DupsLeft  int64
	DupsRight int64
	// DupKeys counts the keys that appear more than once on the left under
	// OnDup "match", and DupRows the rows in them. (Keys duplicated only on
	// the right are matched as multisets too, but are not counted here.)
	DupKeys int64
	DupRows int64

	AddedExamples   []string
	RemovedExamples []string
	ChangedExamples []RowExample
	DupExamples     []string
}

// Same reports whether the inputs are identical (schema and rows). An
// aborted diff is never same: it stopped because too many rows differed.
func (r *Result) Same() bool {
	return r.Schema.Same() && r.RowsSame()
}

// RowsSame reports whether the compared rows are identical.
func (r *Result) RowsSame() bool {
	return !r.Aborted && r.Added == 0 && r.Removed == 0 && r.Changed == 0
}

// Diff compares two inputs by key. Paths accept everything the CLI does:
// files, directories/globs, Iceberg/Delta table roots (with #snapshot), and
// s3:// or http(s):// locations.
func Diff(leftPath, rightPath string, o Options) (res *Result, err error) {
	srcOpts := source.Options{InferRows: o.InferRows, Delimiter: o.Delimiter}
	left, err := source.OpenWith(leftPath, srcOpts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := left.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	right, err := source.OpenWith(rightPath, srcOpts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := right.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// filtering comes first: key inference should judge uniqueness over the
	// rows that will actually be compared
	var preds []source.Predicate
	for _, spec := range o.Where {
		p, perr := source.ParseWhere(spec)
		if perr != nil {
			return nil, perr
		}
		preds = append(preds, p...)
	}
	var pruned int
	if len(preds) > 0 {
		fl, ferr := source.Filter(left, preds)
		if ferr != nil {
			return nil, ferr
		}
		fr, ferr := source.Filter(right, preds)
		if ferr != nil {
			return nil, ferr
		}
		for _, f := range []source.Source{fl, fr} {
			if pf, ok := f.(interface{ PrunedFiles() int }); ok {
				pruned += pf.PrunedFiles()
			}
		}
		left, right = fl, fr
	}

	keys := o.Keys
	if len(keys) == 0 && !o.Keyless {
		k, err := diff.InferKey(left, right, o.Rename)
		if err != nil {
			return nil, err
		}
		keys = []string{k}
	}
	dopts := diff.Options{
		Keys:               keys,
		Keyless:            o.Keyless,
		Where:              source.PredicateString(preds),
		MaxDiff:            o.MaxDiff,
		IgnoreColumns:      o.IgnoreColumns,
		Rename:             o.Rename,
		Mask:               o.Mask,
		Limit:              o.Limit,
		Threads:            o.Threads,
		Summary:            o.Summary,
		Mode:               o.Mode,
		TempDir:            o.TempDir,
		OnDup:              o.OnDup,
		FloatPrecision:     o.FloatPrecision,
		IgnoreCase:         o.IgnoreCase,
		Trim:               o.Trim,
		TimestampPrecision: o.TimestampPrecision,
	}
	if o.Tolerance != nil {
		dopts.Tolerance = &diff.Tolerance{Abs: o.Tolerance.Abs, Rel: o.Tolerance.Rel}
	}
	if len(o.ColumnTolerance) > 0 {
		dopts.ColumnTolerance = make(map[string]diff.Tolerance, len(o.ColumnTolerance))
		for c, t := range o.ColumnTolerance {
			dopts.ColumnTolerance[c] = diff.Tolerance{Abs: t.Abs, Rel: t.Rel}
		}
	}
	r, err := diff.Run(left, right, dopts)
	if err != nil {
		return nil, err
	}
	out := convertResult(r)
	out.FilesPruned = pruned
	return out, nil
}

// convertResult copies the engine result into the public types.
func convertResult(r *diff.Result) *Result {
	out := &Result{
		LeftRows:        r.LeftRows,
		RightRows:       r.RightRows,
		Added:           r.Added,
		Removed:         r.Removed,
		Changed:         r.Changed,
		Unchanged:       r.Unchanged,
		Comparison:      r.Comparison,
		Masked:          r.Masked,
		Keyless:         r.Keyless,
		Filter:          r.Filter,
		Aborted:         r.Aborted,
		PartialCounts:   r.PartialCounts,
		AbortReason:     r.AbortReason,
		WithinTolerance: r.WithinTolerance,
		ColumnChanges:   r.ColumnChanges,
		DupsLeft:        r.DupsLeft,
		DupsRight:       r.DupsRight,
		DupKeys:         r.DupKeys,
		DupRows:         r.DupRows,
		AddedExamples:   r.AddedExamples,
		RemovedExamples: r.RemovedExamples,
		DupExamples:     r.DupExamples,
	}
	if len(r.ColumnStats) > 0 {
		out.ColumnStats = make(map[string]ColumnStat, len(r.ColumnStats))
		for name, st := range r.ColumnStats {
			out.ColumnStats[name] = ColumnStat{
				Changed: st.Changed, MatchRate: st.MatchRate,
				MaxAbsDiff: st.MaxAbsDiff, MeanAbsDiff: st.MeanAbsDiff,
				Numeric: st.Numeric,
			}
		}
	}
	out.Schema = SchemaDiff{
		AddedColumns:   r.Schema.AddedColumns,
		RemovedColumns: r.Schema.RemovedColumns,
	}
	for _, rn := range r.Schema.Renames {
		out.Schema.Renames = append(out.Schema.Renames, Rename{Left: rn.Left, Right: rn.Right})
	}
	for _, tc := range r.Schema.TypeChanges {
		out.Schema.TypeChanges = append(out.Schema.TypeChanges, TypeChange{
			Column: tc.Column, Left: tc.Left, Right: tc.Right, Comparable: tc.Comparable,
		})
	}
	for _, nc := range r.Schema.Nullability {
		out.Schema.NullabilityChanges = append(out.Schema.NullabilityChanges, NullabilityChange{
			Column: nc.Column, LeftNullable: nc.LeftNullable, RightNullable: nc.RightNullable,
		})
	}
	for _, ex := range r.ChangedExamples {
		pe := RowExample{Key: ex.Key}
		for _, c := range ex.Columns {
			pe.Columns = append(pe.Columns, ColumnChange{Column: c.Column, Left: c.Left, Right: c.Right})
		}
		out.ChangedExamples = append(out.ChangedExamples, pe)
	}
	return out
}
