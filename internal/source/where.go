package source

// Row filtering (--where).
//
// A predicate is applied to both sides identically, before the diff sees a
// row, so the counts are of the filtered universe: "which rows differ among
// last month's orders" rather than "which of last month's orders differ".
// That distinction is worth stating in a report, and the CLI does.
//
// Two layers, in this order:
//
//  1. a wrapping Source that evaluates the predicates over decoded batches.
//     It works for every format and every layout, so correctness never
//     depends on the second layer existing.
//  2. partition pruning: a predicate on a hive/Iceberg/Delta partition
//     column is constant per file, so whole files can be skipped without
//     being opened. Pure optimization — layer 1 would have filtered the same
//     rows anyway.
//
// V1 grammar: `col OP literal`, or `col IS [NOT] NULL`, joined by AND
// (repeat the flag, or write " and " between clauses). No OR, no
// expressions: the point is to cut the input down, not to be a query engine.

import (
	"fmt"
	"strings"
)

// whereOp is a comparison operator.
type whereOp uint8

const (
	opEq whereOp = iota
	opNe
	opLt
	opLe
	opGt
	opGe
	opIsNull
	opNotNull
)

func (o whereOp) String() string {
	switch o {
	case opEq:
		return "="
	case opNe:
		return "!="
	case opLt:
		return "<"
	case opLe:
		return "<="
	case opGt:
		return ">"
	case opGe:
		return ">="
	case opIsNull:
		return "IS NULL"
	default:
		return "IS NOT NULL"
	}
}

// Predicate is one parsed clause, still untyped: the literal is bound to the
// column's logical type when the filter is attached to a source.
type Predicate struct {
	Column  string
	Op      whereOp
	Literal string
}

func (p Predicate) String() string {
	if p.Op == opIsNull || p.Op == opNotNull {
		return p.Column + " " + p.Op.String()
	}
	return p.Column + " " + p.Op.String() + " " + p.Literal
}

// PredicateString renders a predicate set the way it will be reported, in a
// stable order (the given order — the caller's).
func PredicateString(preds []Predicate) string {
	parts := make([]string, len(preds))
	for i, p := range preds {
		parts[i] = p.String()
	}
	return strings.Join(parts, " AND ")
}

// operators, longest first so "<=" is not read as "<".
var whereOps = []struct {
	tok string
	op  whereOp
}{
	{"!=", opNe}, {"<>", opNe}, {"<=", opLe}, {">=", opGe},
	{"==", opEq}, {"=", opEq}, {"<", opLt}, {">", opGt},
}

// ParseWhere parses one --where spec into its AND-ed clauses.
func ParseWhere(spec string) ([]Predicate, error) {
	var out []Predicate
	for _, clause := range splitAnd(spec) {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		p, err := parseClause(clause)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty --where")
	}
	return out, nil
}

// splitAnd splits on a case-insensitive " and " outside quotes.
func splitAnd(spec string) []string {
	var parts []string
	var quote byte
	start := 0
	for i := 0; i < len(spec); i++ {
		c := spec[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if (c == ' ' || c == '\t') && hasFoldPrefix(spec[i:], " and ") {
			parts = append(parts, spec[start:i])
			i += len(" and ") - 1
			start = i + 1
		}
	}
	return append(parts, spec[start:])
}

func hasFoldPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func parseClause(clause string) (Predicate, error) {
	// IS [NOT] NULL first: it has no literal, and "is" is not an operator
	// token, so the operator scan below would not find it
	for _, suffix := range []struct {
		text string
		op   whereOp
	}{{"is not null", opNotNull}, {"is null", opIsNull}} {
		if len(clause) > len(suffix.text) &&
			strings.EqualFold(clause[len(clause)-len(suffix.text):], suffix.text) {
			col := strings.TrimSpace(clause[:len(clause)-len(suffix.text)])
			if col == "" {
				return Predicate{}, fmt.Errorf("bad --where %q: no column before %s", clause, suffix.op)
			}
			return Predicate{Column: unquote(col), Op: suffix.op}, nil
		}
	}
	for _, cand := range whereOps {
		i := indexOutsideQuotes(clause, cand.tok)
		if i < 0 {
			continue
		}
		col := strings.TrimSpace(clause[:i])
		lit := strings.TrimSpace(clause[i+len(cand.tok):])
		if col == "" || lit == "" {
			return Predicate{}, fmt.Errorf("bad --where %q (want col %s value)", clause, cand.tok)
		}
		return Predicate{Column: unquote(col), Op: cand.op, Literal: unquote(lit)}, nil
	}
	return Predicate{}, fmt.Errorf("bad --where %q: no comparison found (want col = value, col > value, col IS NULL, …)", clause)
}

// indexOutsideQuotes finds tok outside single/double quotes.
func indexOutsideQuotes(s, tok string) int {
	var quote byte
	for i := 0; i+len(tok) <= len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if s[i:i+len(tok)] == tok {
			return i
		}
	}
	return -1
}

// unquote strips one layer of matching quotes and un-doubles quotes inside.
func unquote(s string) string {
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[len(s)-1] == s[0] {
		q := string(s[0])
		return strings.ReplaceAll(s[1:len(s)-1], q+q, q)
	}
	return s
}
