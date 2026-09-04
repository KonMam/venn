package source

// The --where filter's binding and evaluation (grammar lives in where.go).

import (
	"fmt"
	"strconv"
	"strings"
)

// boundPred is a predicate resolved against a concrete schema: a column
// index plus the literal converted into the column's own domain, so
// evaluation is a comparison and never a parse.
type boundPred struct {
	col  int
	name string
	op   whereOp
	typ  Type

	i       int64   // int/bool/timestamp/date domain
	f       float64 // float domain, and the promoted form of an int comparison
	s       string  // string/bytes domain
	asFloat bool    // an int column compared against a fractional literal
}

// bindPredicates resolves predicates against a schema. A column that is not
// there, or a literal that does not fit the column's type, is an error: a
// typo in a filter must not silently pass every row.
func bindPredicates(s Schema, preds []Predicate) ([]boundPred, error) {
	out := make([]boundPred, 0, len(preds))
	for _, p := range preds {
		ci := s.ColumnIndex(p.Column)
		if ci < 0 {
			return nil, fmt.Errorf("--where names column %q, which this input does not have", p.Column)
		}
		b := boundPred{col: ci, name: p.Column, op: p.Op, typ: s.Columns[ci].Type}
		if p.Op == opIsNull || p.Op == opNotNull {
			out = append(out, b)
			continue
		}
		if err := b.bindLiteral(p.Literal); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (b *boundPred) bindLiteral(lit string) error {
	switch b.typ {
	case TypeString, TypeBytes:
		b.s = lit
	case TypeFloat64:
		f, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return fmt.Errorf("--where %s: %q is not a number", b.name, lit)
		}
		b.f = f
	case TypeBool:
		switch strings.ToLower(lit) {
		case "true", "t", "1", "yes":
			b.i = 1
		case "false", "f", "0", "no":
			b.i = 0
		default:
			return fmt.Errorf("--where %s: %q is not a boolean", b.name, lit)
		}
	case TypeTimestamp:
		if µs, ok := ParseTimestamp(lit); ok {
			b.i = µs
			return nil
		}
		n, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			return fmt.Errorf("--where %s: %q is not a timestamp (try 2026-01-31T12:00:00Z) or a µs count", b.name, lit)
		}
		b.i = n
	case TypeDate:
		if days, ok := ParseDate(lit); ok {
			b.i = int64(days)
			return nil
		}
		n, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			return fmt.Errorf("--where %s: %q is not a date (try 2026-01-31) or a day count", b.name, lit)
		}
		b.i = n
	default: // TypeInt64
		if n, err := strconv.ParseInt(lit, 10, 64); err == nil {
			b.i = n
			return nil
		}
		f, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return fmt.Errorf("--where %s: %q is not a number", b.name, lit)
		}
		// an integer column compared against 1.5 still has a well-defined
		// answer; promote the comparison instead of refusing it
		b.f, b.asFloat = f, true
	}
	return nil
}

// match evaluates the predicate against one value.
func (b *boundPred) match(v *Value) bool {
	if b.op == opIsNull {
		return v.Null
	}
	if b.op == opNotNull {
		return !v.Null
	}
	if v.Null {
		return false // a NULL satisfies no comparison; use IS NULL for that
	}
	switch b.typ {
	case TypeString, TypeBytes:
		return cmpOrder(strings.Compare(v.Str, b.s), b.op)
	case TypeFloat64:
		return cmpFloat(v.Float, b.f, b.op)
	default:
		if b.asFloat {
			return cmpFloat(float64(v.Int), b.f, b.op)
		}
		switch {
		case v.Int < b.i:
			return cmpOrder(-1, b.op)
		case v.Int > b.i:
			return cmpOrder(1, b.op)
		default:
			return cmpOrder(0, b.op)
		}
	}
}

func cmpFloat(a, want float64, op whereOp) bool {
	switch {
	case a < want:
		return cmpOrder(-1, op)
	case a > want:
		return cmpOrder(1, op)
	case a == want:
		return cmpOrder(0, op)
	default:
		return op == opNe // NaN: unordered, so only "not equal" holds
	}
}

// cmpOrder turns a three-way comparison into the operator's verdict.
func cmpOrder(c int, op whereOp) bool {
	switch op {
	case opEq:
		return c == 0
	case opNe:
		return c != 0
	case opLt:
		return c < 0
	case opLe:
		return c <= 0
	case opGt:
		return c > 0
	default: // opGe
		return c >= 0
	}
}

// Filter wraps src so only the rows satisfying every predicate reach the
// caller. Partition pruning happens here too, when the source supports it.
func Filter(src Source, preds []Predicate) (Source, error) {
	if len(preds) == 0 {
		return src, nil
	}
	bound, err := bindPredicates(src.Schema(), preds)
	if err != nil {
		return nil, err
	}
	f := &filteredSource{inner: src, preds: preds, bound: bound}
	f.prune()
	return f, nil
}

// PartitionPruner is an optional Source capability: dropping whole files
// whose constant partition values cannot satisfy a predicate.
type PartitionPruner interface {
	// PrunePartitions keeps only the files keep accepts, and reports how
	// many were dropped. partition maps partition column names to values.
	PrunePartitions(keep func(partition map[string]string) bool) int
	// PartitionColumns names the columns that are constant per file.
	PartitionColumns() []string
}

type filteredSource struct {
	inner  Source
	preds  []Predicate
	bound  []boundPred
	pruned int
}

// prune drops the files that a predicate on a partition column rules out.
// Partition values are text, and are compared as text, the same semantics
// the rest of tdiff gives them.
func (f *filteredSource) prune() {
	pp, ok := f.inner.(PartitionPruner)
	if !ok {
		return
	}
	partCols := map[string]bool{}
	for _, c := range pp.PartitionColumns() {
		partCols[c] = true
	}
	var usable []boundPred
	for _, b := range f.bound {
		if partCols[b.name] && (b.typ == TypeString || b.typ == TypeBytes) {
			usable = append(usable, b)
		}
	}
	if len(usable) == 0 {
		return
	}
	f.pruned = pp.PrunePartitions(func(partition map[string]string) bool {
		for i := range usable {
			b := &usable[i]
			val, present := partition[b.name]
			v := Value{Type: TypeString, Str: val, Null: !present}
			if !b.match(&v) {
				return false
			}
		}
		return true
	})
}

// PrunedFiles reports how many files partition pruning skipped.
func (f *filteredSource) PrunedFiles() int { return f.pruned }

func (f *filteredSource) Schema() Schema { return f.inner.Schema() }
func (f *filteredSource) Close() error   { return f.inner.Close() }

// NumRows forwards the unfiltered count, which is an upper bound on what the
// scan will deliver. RowsAreUpperBound says so, so the engine can size its
// table for the filtered reality instead of the file's.
func (f *filteredSource) NumRows() int64 {
	if n, ok := f.inner.(interface{ NumRows() int64 }); ok {
		return n.NumRows()
	}
	return -1
}

// RowsAreUpperBound marks NumRows as a ceiling rather than a count.
func (f *filteredSource) RowsAreUpperBound() bool { return true }

func (f *filteredSource) SizeBytes() int64 {
	if sz, ok := f.inner.(interface{ SizeBytes() int64 }); ok {
		return sz.SizeBytes()
	}
	return 0
}

func (f *filteredSource) PreferStreaming() bool {
	ps, ok := f.inner.(interface{ PreferStreaming() bool })
	return ok && ps.PreferStreaming()
}

func (f *filteredSource) Warnings() []string {
	if w, ok := f.inner.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

// ForceStringColumn passes the retype through and rebinds: the column's type
// changed, so its literal has to be re-converted.
func (f *filteredSource) ForceStringColumn(name string) bool {
	rt, ok := f.inner.(Retypeable)
	if !ok || !rt.ForceStringColumn(name) {
		return false
	}
	bound, err := bindPredicates(f.inner.Schema(), f.preds)
	if err == nil {
		f.bound = bound
	}
	return true
}

// evaluate fills keep and returns how many rows survived. Predicates are
// applied column at a time: each pass only revisits the rows still alive,
// and a predicate that rules everything out stops the rest.
func (f *filteredSource) evaluate(b *Batch, keep *[]bool) int {
	if cap(*keep) < b.N {
		*keep = make([]bool, b.N)
	}
	k := (*keep)[:b.N]
	for r := range k {
		k[r] = true
	}
	alive := b.N
	for i := range f.bound {
		p := &f.bound[i]
		c := &b.Cols[p.col]
		for r := 0; r < b.N && alive > 0; r++ {
			if !k[r] {
				continue
			}
			v := c.Value(r)
			if !p.match(&v) {
				k[r] = false
				alive--
			}
		}
		if alive == 0 {
			break
		}
	}
	return alive
}

func (f *filteredSource) ScanBatches(n int, makeWorker func() (BatchFunc, error)) error {
	return Scan(f.inner, n, func() (BatchFunc, error) {
		fn, err := makeWorker()
		if err != nil {
			return nil, err
		}
		var out Batch
		var keep []bool
		var sel []int32
		return func(b *Batch) error {
			alive := f.evaluate(b, &keep)
			if alive == b.N {
				return fn(b) // nothing filtered: hand the batch straight on
			}
			if alive == 0 {
				return nil
			}
			compactBatch(&out, b, keep, &sel)
			return fn(&out)
		}, nil
	})
}

// Rows is the serial path (used by key inference and the duplicate-key error
// lookup), filtering row by row.
func (f *filteredSource) Rows() (RowIter, error) {
	it, err := f.inner.Rows()
	if err != nil {
		return nil, err
	}
	return &filteredIter{inner: it, preds: f.bound}, nil
}

type filteredIter struct {
	inner RowIter
	preds []boundPred
}

func (i *filteredIter) Next(dst []Value) (bool, error) {
	for {
		ok, err := i.inner.Next(dst)
		if err != nil || !ok {
			return ok, err
		}
		keep := true
		for p := range i.preds {
			if !i.preds[p].match(&dst[i.preds[p].col]) {
				keep = false
				break
			}
		}
		if keep {
			return true, nil
		}
	}
}

func (i *filteredIter) Close() error { return i.inner.Close() }

// compactBatch copies the kept rows of src into out.
func compactBatch(out, src *Batch, keep []bool, sel *[]int32) {
	s := (*sel)[:0]
	for r := 0; r < src.N; r++ {
		if keep[r] {
			s = append(s, int32(r))
		}
	}
	*sel = s
	if cap(out.Cols) < len(src.Cols) {
		out.Cols = make([]Col, len(src.Cols))
	}
	out.Cols = out.Cols[:len(src.Cols)]
	out.N = len(s)
	for ci := range src.Cols {
		gatherCol(&out.Cols[ci], &src.Cols[ci], s)
	}
}

// gatherCol copies the selected rows of one column, keeping its physical
// representation: a dictionary column stays a dictionary column (its
// dictionary is shared, only the index vector is compacted), so the hashing
// memo downstream still pays for each entry once.
func gatherCol(dst, src *Col, sel []int32) {
	n := len(sel)
	dst.Type = src.Type
	dst.Dict, dst.Idx, dst.I64, dst.F64, dst.Str = nil, nil, nil, nil, nil
	if src.Nulls != nil {
		dst.Nulls = grow(dst.Nulls, n)
		for i, r := range sel {
			dst.Nulls[i] = src.Nulls[r]
		}
	} else {
		dst.Nulls = nil
	}
	switch {
	case src.Idx != nil:
		dst.Dict = src.Dict
		dst.Idx = grow(dst.Idx, n)
		for i, r := range sel {
			dst.Idx[i] = src.Idx[r]
		}
	case src.Type == TypeFloat64:
		dst.F64 = grow(dst.F64, n)
		for i, r := range sel {
			dst.F64[i] = src.F64[r]
		}
	case src.Type == TypeString || src.Type == TypeBytes:
		dst.Str = grow(dst.Str, n)
		for i, r := range sel {
			dst.Str[i] = src.Str[r]
		}
	default:
		dst.I64 = grow(dst.I64, n)
		for i, r := range sel {
			dst.I64[i] = src.I64[r]
		}
	}
}
