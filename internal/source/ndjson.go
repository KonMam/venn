package source

// NDJSON (newline-delimited JSON) source for flat objects. Raw newlines
// cannot appear inside JSON string literals, so files split safely on '\n'
// and blocks parse in parallel, like the CSV fast path. Lines are tokenized
// by a hand-rolled scanner (no per-row encoding/json): strings without
// escapes are zero-copy views, numbers parse straight from the bytes.
//
// Types are inferred from a sample: integral numbers → int64, other numbers
// → float64, strings that parse as timestamps/dates → those types, bool,
// null → nullable. Columns holding nested objects/arrays in the sample are
// skipped with a warning. Keys missing from a line are NULL; keys not seen
// during inference are ignored.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

type ndjsonSource struct {
	path      string
	compress  string
	inferRows int
	schema    Schema
	colIdx    map[string]int
	forced    map[string]bool
	warnings  []string
}

func (ns *ndjsonSource) Warnings() []string { return ns.warnings }

// OpenNDJSON opens a .ndjson/.jsonl file (optionally .gz/.zst compressed).
func OpenNDJSON(path string, inferRows int) (Source, error) {
	compress := ""
	switch {
	case strings.HasSuffix(path, ".gz"):
		compress = "gz"
	case strings.HasSuffix(path, ".zst"):
		compress = "zst"
	}
	ns := &ndjsonSource{path: path, compress: compress, inferRows: inferRows}
	if err := ns.inferSchema(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return ns, nil
}

func (ns *ndjsonSource) openRaw() (io.Reader, func() error, error) {
	return openDecompressed(ns.path, ns.compress)
}

func (ns *ndjsonSource) ForceStringColumn(name string) bool {
	i := ns.schema.ColumnIndex(name)
	if i < 0 || ns.schema.Columns[i].Type == TypeString {
		return false
	}
	if ns.forced == nil {
		ns.forced = map[string]bool{}
	}
	ns.forced[name] = true
	ns.schema.Columns[i].Type = TypeString
	ns.schema.Columns[i].PhysicalType = "ndjson:string(forced)"
	return true
}

// inferSchema samples lines with encoding/json (sampling only; the scan path
// never touches it).
func (ns *ndjsonSource) inferSchema() error {
	raw, closer, err := ns.openRaw()
	if err != nil {
		return err
	}
	defer closer() //nolint:errcheck // inference: read errors already surface
	limit := ns.inferRows
	if limit <= 0 {
		limit = 1 << 62
	}
	type info struct {
		sawInt, sawFloat, sawBool, sawStr, sawNull, sawNested bool
		sawTS, sawDate, sawNonTS                              bool
		count                                                 int
	}
	cols := map[string]*info{}
	sc := bufio.NewScanner(raw)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	rows := 0
	for rows < limit && sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var obj map[string]any
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&obj); err != nil {
			return fmt.Errorf("line %d: %w", rows+1, err)
		}
		rows++
		for k, v := range obj {
			ci := cols[k]
			if ci == nil {
				ci = &info{}
				cols[k] = ci
			}
			ci.count++
			switch t := v.(type) {
			case nil:
				ci.sawNull = true
			case bool:
				ci.sawBool = true
			case json.Number:
				if _, err := strconv.ParseInt(string(t), 10, 64); err == nil {
					ci.sawInt = true
				} else {
					ci.sawFloat = true
				}
			case string:
				ci.sawStr = true
				if _, ok := ParseTimestamp(t); ok {
					ci.sawTS = true
				} else if _, ok := ParseDate(t); ok {
					ci.sawDate = true
				} else {
					ci.sawNonTS = true
				}
			default: // objects, arrays
				ci.sawNested = true
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("empty file")
	}

	names := make([]string, 0, len(cols))
	for k := range cols {
		names = append(names, k)
	}
	sort.Strings(names)
	ns.colIdx = make(map[string]int)
	for _, k := range names {
		ci := cols[k]
		if ci.sawNested {
			ns.warnings = append(ns.warnings,
				fmt.Sprintf("column %q skipped: nested JSON values are not compared", k))
			continue
		}
		typ := TypeString
		switch {
		case ci.sawStr && (ci.sawInt || ci.sawFloat || ci.sawBool):
			typ = TypeString // mixed → string
		case ci.sawStr && ci.sawTS && !ci.sawNonTS && !ci.sawDate:
			typ = TypeTimestamp
		case ci.sawStr && ci.sawDate && !ci.sawNonTS && !ci.sawTS:
			typ = TypeDate
		case ci.sawStr:
			typ = TypeString
		case ci.sawFloat:
			typ = TypeFloat64
		case ci.sawInt:
			typ = TypeInt64
		case ci.sawBool:
			typ = TypeBool
		}
		nullable := ci.sawNull || ci.count < rows
		ns.colIdx[k] = len(ns.schema.Columns)
		ns.schema.Columns = append(ns.schema.Columns, Column{
			Name: k, Type: typ, Nullable: nullable, PhysicalType: "ndjson:" + typ.String(),
		})
	}
	if len(ns.schema.Columns) == 0 {
		return fmt.Errorf("no scalar columns found")
	}
	return nil
}

func (ns *ndjsonSource) Schema() Schema { return ns.schema }
func (ns *ndjsonSource) Close() error   { return nil }

// PreferStreaming reports whether this source is decompressor-bound.
func (ns *ndjsonSource) PreferStreaming() bool { return ns.compress != "" }

// SizeBytes reports the file size (auto mode-selection heuristic).
func (ns *ndjsonSource) SizeBytes() int64 {
	st, err := os.Stat(ns.path)
	if err != nil {
		return 0
	}
	return st.Size()
}

// ---- serial iterator (fallback / findKeyByHash) ----

type ndjsonRowIter struct {
	ns    *ndjsonSource
	sc    *bufio.Scanner
	close func() error
	p     ndjsonParser
}

func (ns *ndjsonSource) Rows() (RowIter, error) {
	raw, closer, err := ns.openRaw()
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(raw)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	return &ndjsonRowIter{ns: ns, sc: sc, close: closer, p: ndjsonParser{ns: ns}}, nil
}

func (it *ndjsonRowIter) Next(dst []Value) (bool, error) {
	for it.sc.Scan() {
		line := bytes.TrimSpace(it.sc.Bytes())
		if len(line) == 0 {
			continue
		}
		for i := range dst {
			dst[i] = Value{Type: it.ns.schema.Columns[i].Type, Null: true}
		}
		if err := it.p.parseLineInto(line, func(ci int, v Value) { dst[ci] = v }); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, it.sc.Err()
}

func (it *ndjsonRowIter) Close() error { return it.close() }

// ---- parallel scan (same shape as the CSV fast path) ----

const ndjsonBlockSize = 1 << 20

func (ns *ndjsonSource) ScanBatches(n int, makeWorker func() (BatchFunc, error)) (err error) {
	raw, closer, oerr := ns.openRaw()
	if oerr != nil {
		return oerr
	}
	defer func() {
		if cerr := closer(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	work := make(chan []byte, n)
	free := make(chan []byte, n+2)
	for i := 0; i < n+2; i++ {
		free <- make([]byte, 0, ndjsonBlockSize+4096)
	}
	errc := make(chan error, n+1)
	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn, err := makeWorker()
			if err != nil {
				errc <- err
				return
			}
			ncols := len(ns.schema.Columns)
			b := &Batch{Cols: make([]Col, ncols)}
			p := ndjsonParser{ns: ns}
			for blk := range work {
				if err := p.parseBlock(blk, b, fn); err != nil {
					errc <- err
					return
				}
				free <- blk[:0]
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	var firstErr error
	var leftover []byte
feed:
	for {
		var block []byte
		select {
		case block = <-free:
		case firstErr = <-errc:
			break feed
		}
		block, _, eof, rerr := refillBlock(raw, block, leftover, ndjsonBlockSize)
		if rerr != nil {
			firstErr = rerr
			break
		}
		leftover = leftover[:0]
		if eof {
			if len(block) > 0 {
				if block[len(block)-1] != '\n' {
					block = append(block, '\n')
				}
				select {
				case work <- block:
				case firstErr = <-errc:
				}
			}
			break
		}
		cut := bytes.LastIndexByte(block, '\n')
		if cut < 0 {
			firstErr = fmt.Errorf("%s: line longer than %d bytes", ns.path, ndjsonBlockSize)
			break
		}
		leftover = append(leftover, block[cut+1:]...)
		select {
		case work <- block[:cut+1]:
		case firstErr = <-errc:
			break feed
		}
	}
	close(work)
	select {
	case <-done:
	case err := <-errc:
		if firstErr == nil {
			firstErr = err
		}
		<-done
	}
	if firstErr == nil {
		select {
		case firstErr = <-errc:
		default:
		}
	}
	return firstErr
}

// ndjsonParser tokenizes flat JSON lines directly into typed columns.
type ndjsonParser struct {
	ns      *ndjsonSource
	scratch []byte // unescape arena
	// order memoizes which column appeared at each field position on the
	// previous line: NDJSON lines almost always share key order, so a byte
	// compare replaces the per-key map lookup.
	order []int32
}

func (p *ndjsonParser) parseBlock(raw []byte, b *Batch, fn BatchFunc) error {
	ns := p.ns
	rows := bytes.Count(raw, []byte{'\n'})
	for ci := range b.Cols {
		b.Cols[ci].reset(ns.schema.Columns[ci].Type, rows, true)
		for r := 0; r < rows; r++ {
			b.Cols[ci].Nulls[r] = true // absent keys are NULL
		}
	}
	p.scratch = p.scratch[:0]
	r := 0
	for start := 0; start < len(raw); {
		nl := bytes.IndexByte(raw[start:], '\n')
		line := bytes.TrimSpace(raw[start : start+nl])
		start += nl + 1
		if len(line) == 0 {
			continue
		}
		err := p.parseLineInto(line, func(ci int, v Value) {
			col := &b.Cols[ci]
			col.Nulls[r] = v.Null
			switch col.Type {
			case TypeFloat64:
				col.F64[r] = v.Float
			case TypeString, TypeBytes:
				col.Str[r] = v.Str
			default:
				col.I64[r] = v.Int
			}
		})
		if err != nil {
			return err
		}
		r++
	}
	for ci := range b.Cols {
		b.Cols[ci].truncate(r)
	}
	b.N = r
	return fn(b)
}

// parseLineInto tokenizes one flat JSON object, emitting typed values for
// known columns.
func (p *ndjsonParser) parseLineInto(line []byte, emit func(ci int, v Value)) error {
	ns := p.ns
	i := 0
	skipWS := func() {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t' || line[i] == '\r') {
			i++
		}
	}
	skipWS()
	if i >= len(line) || line[i] != '{' {
		return fmt.Errorf("%s: line does not start with '{'", ns.path)
	}
	i++
	first := true
	fieldPos := 0
	for {
		skipWS()
		if i < len(line) && line[i] == '}' {
			return nil
		}
		if !first {
			if i >= len(line) || line[i] != ',' {
				if i < len(line) && line[i] == '}' {
					return nil
				}
				return fmt.Errorf("%s: expected ',' in object", ns.path)
			}
			i++
			skipWS()
			if i < len(line) && line[i] == '}' { // trailing comma tolerance
				return nil
			}
		}
		first = false
		key, n, err := p.parseString(line[i:])
		if err != nil {
			return fmt.Errorf("%s: bad object key: %w", ns.path, err)
		}
		i += n
		skipWS()
		if i >= len(line) || line[i] != ':' {
			return fmt.Errorf("%s: expected ':'", ns.path)
		}
		i++
		skipWS()
		// key-order memo: expect the same column at this position as on the
		// previous line and verify with one string compare
		ci, known := -1, false
		if fieldPos < len(p.order) {
			if exp := p.order[fieldPos]; exp >= 0 && ns.schema.Columns[exp].Name == key {
				ci, known = int(exp), true
			}
		}
		if !known {
			if idx, ok := ns.colIdx[key]; ok {
				ci, known = idx, true
				if fieldPos < len(p.order) {
					p.order[fieldPos] = int32(idx)
				} else if fieldPos == len(p.order) {
					p.order = append(p.order, int32(idx))
				}
			}
		}
		fieldPos++
		v, n, err := p.parseValue(line[i:], known, ci)
		if err != nil {
			return err
		}
		i += n
		if known {
			emit(ci, v)
		}
	}
}

// parseString parses a JSON string starting at b[0]=='"'. Returns the value
// (zero-copy when escape-free) and bytes consumed. A plain byte loop wins
// here: typical fields are 5-20 bytes, below IndexByte's call overhead
// (measured — the two-sweep IndexByte variant was ~7% slower end to end).
func (p *ndjsonParser) parseString(b []byte) (string, int, error) {
	if len(b) == 0 || b[0] != '"' {
		return "", 0, fmt.Errorf("expected string")
	}
	for i := 1; i < len(b); i++ {
		switch b[i] {
		case '"':
			return unsafeStr(b[1:i]), i + 1, nil
		case '\\':
			return p.parseEscapedString(b)
		}
	}
	return "", 0, fmt.Errorf("unterminated string")
}

func unsafeStr(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// parseEscapedString handles the rare escape-bearing strings via the arena.
func (p *ndjsonParser) parseEscapedString(b []byte) (string, int, error) {
	start := len(p.scratch)
	i := 1
	for i < len(b) {
		c := b[i]
		switch c {
		case '"':
			out := p.scratch[start:]
			return unsafeStr(out), i + 1, nil
		case '\\':
			i++
			if i >= len(b) {
				return "", 0, fmt.Errorf("truncated escape")
			}
			switch b[i] {
			case '"', '\\', '/':
				p.scratch = append(p.scratch, b[i])
			case 'n':
				p.scratch = append(p.scratch, '\n')
			case 't':
				p.scratch = append(p.scratch, '\t')
			case 'r':
				p.scratch = append(p.scratch, '\r')
			case 'b':
				p.scratch = append(p.scratch, '\b')
			case 'f':
				p.scratch = append(p.scratch, '\f')
			case 'u':
				if i+4 >= len(b) {
					return "", 0, fmt.Errorf("truncated \\u escape")
				}
				cp, err := strconv.ParseUint(unsafeStr(b[i+1:i+5]), 16, 32)
				if err != nil {
					return "", 0, fmt.Errorf("bad \\u escape")
				}
				p.scratch = append(p.scratch, string(rune(cp))...)
				i += 4
			default:
				return "", 0, fmt.Errorf("bad escape \\%c", b[i])
			}
			i++
		default:
			p.scratch = append(p.scratch, c)
			i++
		}
	}
	return "", 0, fmt.Errorf("unterminated string")
}

// parseValue parses one JSON value. For known columns it converts to the
// column's type; unknown/nested values are skipped structurally.
func (p *ndjsonParser) parseValue(b []byte, known bool, ci int) (Value, int, error) {
	ns := p.ns
	if len(b) == 0 {
		return Value{}, 0, fmt.Errorf("empty value")
	}
	typ := TypeString
	colName := ""
	if known {
		typ = ns.schema.Columns[ci].Type
		colName = ns.schema.Columns[ci].Name
	}
	switch b[0] {
	case 'n':
		if bytes.HasPrefix(b, []byte("null")) {
			return Value{Type: typ, Null: true}, 4, nil
		}
	case 't':
		if bytes.HasPrefix(b, []byte("true")) {
			return p.coerce(Value{Type: TypeBool, Int: 1}, typ, colName, "true")
		}
	case 'f':
		if bytes.HasPrefix(b, []byte("false")) {
			return p.coerce(Value{Type: TypeBool, Int: 0}, typ, colName, "false")
		}
	case '"':
		s, n, err := p.parseString(b)
		if err != nil {
			return Value{}, 0, err
		}
		v, n2, err := p.stringValue(s, typ, colName)
		return v, n + n2, err
	case '{', '[':
		n, err := skipJSONValue(b)
		if err != nil {
			return Value{}, 0, err
		}
		if known {
			return Value{}, 0, &TypeCoercionError{Column: colName, Value: "(nested)", Want: typ.String()}
		}
		return Value{Null: true}, n, nil
	default: // number
		end := 0
		for end < len(b) && (b[end] == '-' || b[end] == '+' || b[end] == '.' ||
			b[end] == 'e' || b[end] == 'E' || (b[end] >= '0' && b[end] <= '9')) {
			end++
		}
		if end == 0 {
			return Value{}, 0, fmt.Errorf("bad JSON value %q", b[:min(8, len(b))])
		}
		s := unsafeStr(b[:end])
		switch typ {
		case TypeInt64:
			iv, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return Value{}, 0, &TypeCoercionError{Column: colName, Value: strings.Clone(s), Want: "int64"}
			}
			return Value{Type: TypeInt64, Int: iv}, end, nil
		case TypeFloat64:
			fv, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return Value{}, 0, &TypeCoercionError{Column: colName, Value: strings.Clone(s), Want: "float64"}
			}
			return Value{Type: TypeFloat64, Float: fv}, end, nil
		case TypeString:
			return Value{Type: TypeString, Str: s}, end, nil
		default:
			return Value{}, 0, &TypeCoercionError{Column: colName, Value: strings.Clone(s), Want: typ.String()}
		}
	}
	return Value{}, 0, fmt.Errorf("bad JSON value")
}

// coerce fits a scalar into the column type or reports a coercion error.
func (p *ndjsonParser) coerce(v Value, typ Type, col, raw string) (Value, int, error) {
	n := 4
	if v.Int == 0 && v.Type == TypeBool && raw == "false" {
		n = 5
	}
	switch typ {
	case TypeBool:
		return v, n, nil
	case TypeString:
		return Value{Type: TypeString, Str: raw}, n, nil
	default:
		return Value{}, 0, &TypeCoercionError{Column: col, Value: raw, Want: typ.String()}
	}
}

// stringValue converts a JSON string to the column's type.
func (p *ndjsonParser) stringValue(s string, typ Type, col string) (Value, int, error) {
	switch typ {
	case TypeTimestamp:
		us, ok := ParseTimestamp(s)
		if !ok {
			return Value{}, 0, &TypeCoercionError{Column: col, Value: strings.Clone(s), Want: "timestamp"}
		}
		return Value{Type: TypeTimestamp, Int: us}, 0, nil
	case TypeDate:
		d, ok := ParseDate(s)
		if !ok {
			return Value{}, 0, &TypeCoercionError{Column: col, Value: strings.Clone(s), Want: "date"}
		}
		return Value{Type: TypeDate, Int: int64(d)}, 0, nil
	default:
		return Value{Type: TypeString, Str: s}, 0, nil
	}
}

// skipJSONValue advances past one JSON value (string-aware nesting).
func skipJSONValue(b []byte) (int, error) {
	depth := 0
	inStr := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inStr {
			switch c {
			case '\\':
				i++
			case '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
	}
	return 0, fmt.Errorf("truncated nested JSON value")
}
