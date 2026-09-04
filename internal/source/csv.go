package source

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"unsafe"
)

// inferSampleRows is how many data rows the CSV reader scans to infer
// column types before committing to a schema.
const inferSampleRows = 1000

// csvSource reads a delimited text file with a header row. Column types are
// inferred from a sample: the narrowest of bool → int64 → float64 → date →
// timestamp → string that fits every sampled value. Empty fields are NULL.
type csvSource struct {
	path   string
	comma  rune
	schema Schema
}

// OpenCSV opens a CSV/TSV file as a Source, inferring column types.
func OpenCSV(path string, comma rune) (Source, error) {
	cs := &csvSource{path: path, comma: comma}
	if err := cs.inferSchema(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cs, nil
}

func (cs *csvSource) newReader() (*csv.Reader, *os.File, error) {
	f, err := os.Open(cs.path)
	if err != nil {
		return nil, nil, err
	}
	r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	r.Comma = cs.comma
	r.ReuseRecord = true
	return r, f, nil
}

// candidate type lattice, narrowest first
const (
	candBool = 1 << iota
	candInt
	candFloat
	candDate
	candTimestamp
)

func (cs *csvSource) inferSchema() error {
	r, f, err := cs.newReader()
	if err != nil {
		return err
	}
	defer f.Close()

	header, err := r.Read()
	if err == io.EOF {
		return fmt.Errorf("empty file")
	}
	if err != nil {
		return err
	}
	names := make([]string, len(header))
	copy(names, header)

	cand := make([]int, len(names))
	for i := range cand {
		cand[i] = candBool | candInt | candFloat | candDate | candTimestamp
	}
	nullable := make([]bool, len(names))
	nonEmpty := make([]bool, len(names))

	for n := 0; n < inferSampleRows; n++ {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for i, s := range rec {
			if s == "" {
				nullable[i] = true
				continue
			}
			nonEmpty[i] = true
			c := cand[i]
			if c == 0 {
				continue
			}
			if c&candBool != 0 && !isCSVBool(s) {
				c &^= candBool
			}
			if c&candInt != 0 {
				if _, err := strconv.ParseInt(s, 10, 64); err != nil {
					c &^= candInt
				}
			}
			if c&candFloat != 0 {
				if _, err := strconv.ParseFloat(s, 64); err != nil {
					c &^= candFloat
				}
			}
			if c&candDate != 0 {
				if _, ok := ParseDate(s); !ok {
					c &^= candDate
				}
			}
			if c&candTimestamp != 0 {
				if _, ok := ParseTimestamp(s); !ok {
					c &^= candTimestamp
				}
			}
			cand[i] = c
		}
	}

	for i, name := range names {
		typ := TypeString
		if nonEmpty[i] {
			switch c := cand[i]; {
			case c&candBool != 0:
				typ = TypeBool
			case c&candInt != 0:
				typ = TypeInt64
			case c&candFloat != 0:
				typ = TypeFloat64
			case c&candDate != 0:
				typ = TypeDate
			case c&candTimestamp != 0:
				typ = TypeTimestamp
			}
		}
		cs.schema.Columns = append(cs.schema.Columns, Column{
			Name:         name,
			Type:         typ,
			Nullable:     nullable[i],
			PhysicalType: "csv:" + typ.String(),
		})
	}
	return nil
}

func isCSVBool(s string) bool {
	switch s {
	case "true", "false", "True", "False", "TRUE", "FALSE":
		return true
	}
	return false
}

func (cs *csvSource) Schema() Schema { return cs.schema }
func (cs *csvSource) Close() error   { return nil }

// SizeBytes reports the file size (used by diff's auto mode selection).
func (cs *csvSource) SizeBytes() int64 {
	st, err := os.Stat(cs.path)
	if err != nil {
		return 0
	}
	return st.Size()
}

func (cs *csvSource) Rows() (RowIter, error) {
	r, f, err := cs.newReader()
	if err != nil {
		return nil, err
	}
	if _, err := r.Read(); err != nil { // skip header
		f.Close()
		return nil, err
	}
	return &csvRowIter{cs: cs, r: r, f: f, line: 1}, nil
}

type csvRowIter struct {
	cs   *csvSource
	r    *csv.Reader
	f    *os.File
	line int64
}

func (it *csvRowIter) Next(dst []Value) (bool, error) {
	rec, err := it.r.Read()
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	it.line++
	return true, it.cs.convertRecord(rec, dst)
}

// convertRecord parses one raw CSV record into canonical values. String
// values alias the record's strings (encoding/csv allocates a fresh backing
// string per record, so they remain valid indefinitely).
func (cs *csvSource) convertRecord(rec []string, dst []Value) error {
	if len(rec) != len(dst) {
		return fmt.Errorf("%s: record has %d fields, want %d", cs.path, len(rec), len(dst))
	}
	for i, s := range rec {
		col := &cs.schema.Columns[i]
		out := &dst[i]
		out.Type = col.Type
		out.Str = ""
		if s == "" && col.Type != TypeString {
			out.Null = true
			continue
		}
		out.Null = false
		switch col.Type {
		case TypeBool:
			out.Int = 0
			if s == "true" || s == "True" || s == "TRUE" {
				out.Int = 1
			} else if !isCSVBool(s) {
				return cs.parseErr(i, s, "bool")
			}
		case TypeInt64:
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return cs.parseErr(i, s, "int64")
			}
			out.Int = v
		case TypeFloat64:
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return cs.parseErr(i, s, "float64")
			}
			out.Float = v
		case TypeDate:
			d, ok := ParseDate(s)
			if !ok {
				return cs.parseErr(i, s, "date")
			}
			out.Int = int64(d)
		case TypeTimestamp:
			us, ok := ParseTimestamp(s)
			if !ok {
				return cs.parseErr(i, s, "timestamp")
			}
			out.Int = us
		default: // TypeString
			out.Str = s
		}
	}
	return nil
}

func (cs *csvSource) parseErr(col int, s, typ string) error {
	return fmt.Errorf("%s: column %q: %q is not a valid %s (type inferred from first %d rows; consider cleaning the column)",
		cs.path, cs.schema.Columns[col].Name, s, typ, inferSampleRows)
}

// csvBatchRows is the number of records handed from the reader goroutine to
// each parse worker at a time.
const csvBatchRows = 2048

// convertColumn parses one column's fields out of a flat record batch into
// col (rows entries). Fields for row r sit at flat[r*ncols+ci].
func (cs *csvSource) convertColumn(flat []string, ci, ncols, rows int, col *Col) error {
	spec := &cs.schema.Columns[ci]
	col.reset(spec.Type, rows, false)
	for r := 0; r < rows; r++ {
		if err := cs.parseFieldString(flat[r*ncols+ci], ci, col, r, rows); err != nil {
			return err
		}
	}
	return nil
}

// csvWork is one unit handed to a parse worker: either a raw quote-free
// byte block ending in \n (fast path) or a flat batch of pre-split records
// from encoding/csv (fallback for quoted files).
type csvWork struct {
	raw  []byte
	flat []string
}

// csvBlockSize is the raw block size for the fast path.
const csvBlockSize = 1 << 20

// ScanBatches scans the file with one reader goroutine and n parse workers.
//
// Fast path: as long as no double-quote byte has been seen, the reader only
// aligns raw blocks on newline boundaries and the workers do all splitting
// and parsing in parallel. The first block containing a quote switches the
// remainder of the file to encoding/csv in the reader (correct for quoted
// fields, including embedded newlines and separators); the switch point is a
// true record boundary because everything before it was quote-free.
func (cs *csvSource) ScanBatches(n int, makeWorker func() (BatchFunc, error)) error {
	f, err := os.Open(cs.path)
	if err != nil {
		return err
	}
	defer f.Close()
	ncols := len(cs.schema.Columns)

	work := make(chan csvWork, n)
	free := make(chan []byte, n+2)
	for i := 0; i < n+2; i++ {
		free <- make([]byte, 0, csvBlockSize+4096)
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
			b := &Batch{Cols: make([]Col, ncols)}
			for wu := range work {
				var err error
				if wu.raw != nil {
					err = cs.parseRawBlock(wu.raw, b, fn)
					free <- wu.raw[:0]
				} else {
					err = cs.parseFlat(wu.flat, b, fn)
				}
				if err != nil {
					errc <- err
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	firstErr := cs.feed(f, ncols, work, free, errc)
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

// feed reads the file and dispatches work units, switching to the
// encoding/csv fallback if a quote appears.
func (cs *csvSource) feed(f *os.File, ncols int, work chan csvWork, free chan []byte, errc chan error) error {
	send := func(wu csvWork) (bool, error) {
		select {
		case work <- wu:
			return true, nil
		case err := <-errc:
			return false, err
		}
	}

	var leftover []byte // partial last line of the previous block, copied out
	headerSkipped := false
	for {
		var block []byte
		select {
		case block = <-free:
		case err := <-errc:
			return err
		}
		block = append(block, leftover...)
		leftover = leftover[:0]
		base := len(block)
		block = block[:cap(block)]
		m, rerr := io.ReadFull(f, block[base:base+csvBlockSize])
		block = block[:base+m]
		if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
			return rerr
		}
		eof := rerr != nil

		if !headerSkipped {
			nl := bytes.IndexByte(block, '\n')
			if nl < 0 {
				if !eof {
					return fmt.Errorf("%s: header line longer than %d bytes", cs.path, csvBlockSize)
				}
				return nil // header only, no data
			}
			if bytes.IndexByte(block[:nl+1], '"') >= 0 {
				// quoted header: hand everything incl. header to fallback
				return cs.feedFallback(f, ncols, block, true, work, errc)
			}
			block = append(block[:0], block[nl+1:]...)
			headerSkipped = true
		}

		if bytes.IndexByte(block[base:], '"') >= 0 {
			return cs.feedFallback(f, ncols, block, false, work, errc)
		}

		if eof {
			if len(block) > 0 {
				if block[len(block)-1] != '\n' {
					block = append(block, '\n')
				}
				if ok, err := send(csvWork{raw: block}); !ok {
					return err
				}
			}
			return nil
		}

		cut := bytes.LastIndexByte(block, '\n')
		if cut < 0 {
			return fmt.Errorf("%s: line longer than %d bytes", cs.path, csvBlockSize)
		}
		leftover = append(leftover, block[cut+1:]...)
		if ok, err := send(csvWork{raw: block[:cut+1]}); !ok {
			return err
		}
	}
}

// feedFallback routes the rest of the file (prefixed by pending unparsed
// bytes) through encoding/csv, emitting flat record batches.
func (cs *csvSource) feedFallback(f *os.File, ncols int, pending []byte, withHeader bool, work chan csvWork, errc chan error) error {
	r := csv.NewReader(io.MultiReader(bytes.NewReader(pending), bufio.NewReaderSize(f, 1<<20)))
	r.Comma = cs.comma
	r.ReuseRecord = true
	if withHeader {
		if _, err := r.Read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	flat := make([]string, 0, csvBatchRows*ncols)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) != ncols {
			return fmt.Errorf("%s: record has %d fields, want %d", cs.path, len(rec), ncols)
		}
		flat = append(flat, rec...)
		if len(flat) == cap(flat) {
			select {
			case work <- csvWork{flat: flat}:
			case err := <-errc:
				return err
			}
			flat = make([]string, 0, csvBatchRows*ncols)
		}
	}
	if len(flat) > 0 {
		select {
		case work <- csvWork{flat: flat}:
		case err := <-errc:
			return err
		}
	}
	return nil
}

// parseFlat converts one flat record batch (fallback path).
func (cs *csvSource) parseFlat(flat []string, b *Batch, fn BatchFunc) error {
	ncols := len(cs.schema.Columns)
	rows := len(flat) / ncols
	for ci := 0; ci < ncols; ci++ {
		if err := cs.convertColumn(flat, ci, ncols, rows, &b.Cols[ci]); err != nil {
			return err
		}
	}
	b.N = rows
	return fn(b)
}

// parseRawBlock splits a quote-free raw block (ending in \n) into rows and
// parses fields straight into the batch columns. Strings alias the block,
// which stays alive until fn returns.
func (cs *csvSource) parseRawBlock(raw []byte, b *Batch, fn BatchFunc) error {
	rows := bytes.Count(raw, []byte{'\n'})
	cols := cs.schema.Columns
	ncols := len(cols)
	for ci := range b.Cols {
		b.Cols[ci].reset(cols[ci].Type, rows, false)
	}
	sep := byte(cs.comma)
	r := 0
	for start := 0; start < len(raw); {
		nl := bytes.IndexByte(raw[start:], '\n')
		line := raw[start : start+nl]
		start += nl + 1
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			continue
		}
		for ci := 0; ci < ncols; ci++ {
			var field []byte
			if ci == ncols-1 {
				if bytes.IndexByte(line, sep) >= 0 {
					return fmt.Errorf("%s: record has more than %d fields", cs.path, ncols)
				}
				field = line
			} else {
				c := bytes.IndexByte(line, sep)
				if c < 0 {
					return fmt.Errorf("%s: record has fewer than %d fields", cs.path, ncols)
				}
				field = line[:c]
				line = line[c+1:]
			}
			if err := cs.parseField(field, ci, &b.Cols[ci], r, rows); err != nil {
				return err
			}
		}
		r++
	}
	for ci := range b.Cols {
		b.Cols[ci].truncate(r)
	}
	b.N = r
	return fn(b)
}

// parseField parses one raw field into row r of col. The zero-copy string
// view is valid for the batch lifetime only.
func (cs *csvSource) parseField(field []byte, ci int, col *Col, r, rows int) error {
	if len(field) == 0 {
		if col.Type == TypeString {
			col.Str[r] = ""
		} else {
			col.setNull(r, rows)
		}
		return nil
	}
	s := unsafe.String(&field[0], len(field))
	return cs.parseFieldValue(s, ci, col, r, rows)
}

// parseFieldString is parseField for fields already held as strings
// (fallback path).
func (cs *csvSource) parseFieldString(s string, ci int, col *Col, r, rows int) error {
	if s == "" {
		if col.Type == TypeString {
			col.Str[r] = ""
		} else {
			col.setNull(r, rows)
		}
		return nil
	}
	return cs.parseFieldValue(s, ci, col, r, rows)
}

func (cs *csvSource) parseFieldValue(s string, ci int, col *Col, r, rows int) error {
	switch col.Type {
	case TypeBool:
		col.I64[r] = 0
		if s == "true" || s == "True" || s == "TRUE" {
			col.I64[r] = 1
		} else if !isCSVBool(s) {
			return cs.parseErr(ci, s, "bool")
		}
	case TypeInt64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return cs.parseErr(ci, s, "int64")
		}
		col.I64[r] = v
	case TypeFloat64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return cs.parseErr(ci, s, "float64")
		}
		col.F64[r] = v
	case TypeDate:
		d, ok := ParseDate(s)
		if !ok {
			return cs.parseErr(ci, s, "date")
		}
		col.I64[r] = int64(d)
	case TypeTimestamp:
		us, ok := ParseTimestamp(s)
		if !ok {
			return cs.parseErr(ci, s, "timestamp")
		}
		col.I64[r] = us
	default: // TypeString
		col.Str[r] = s
	}
	return nil
}

func (it *csvRowIter) Close() error { return it.f.Close() }
