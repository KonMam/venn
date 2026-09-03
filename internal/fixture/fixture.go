// Package fixture produces fixture pairs (left/right) with known planted
// differences, in every supported format, plus a manifest.json of the ground
// truth. It is simultaneously the test oracle, the benchmark dataset
// generator, and the cross-format test bed (see bench/gen for the CLI).
//
// Values are pure functions of (seed, key, column), so any subset of rows is
// reproducible. The right side is written in a different physical row order
// to prove keyed diffing is order-independent.
package fixture

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"tdiff/internal/source"
)

// Config parameterizes one generated fixture pair.
type Config struct {
	Rows       int64
	Cols       int // including the id key column
	Seed       uint64
	PctChanged float64
	PctAdded   float64
	PctRemoved float64
	Out        string   // output directory
	Formats    []string // "parquet", "csv"
	Variant    string   // standard | wide | stringy
}

type colSpec struct {
	name     string
	typ      source.Type
	nullable bool
}

// variants
func columns(variant string, ncols int) []colSpec {
	cols := []colSpec{{name: "id", typ: source.TypeInt64}}
	cycle := []source.Type{
		source.TypeInt64, source.TypeFloat64, source.TypeString,
		source.TypeTimestamp, source.TypeBool, source.TypeDate,
	}
	if variant == "stringy" {
		cycle = []source.Type{source.TypeString, source.TypeString, source.TypeString, source.TypeInt64}
	}
	counts := map[source.Type]int{}
	for i := 1; i < ncols; i++ {
		t := cycle[(i-1)%len(cycle)]
		counts[t]++
		prefix := map[source.Type]string{
			source.TypeInt64: "i", source.TypeFloat64: "f", source.TypeString: "s",
			source.TypeTimestamp: "t", source.TypeBool: "b", source.TypeDate: "d",
		}[t]
		cols = append(cols, colSpec{
			name: fmt.Sprintf("%s%d", prefix, counts[t]),
			typ:  t,
			// only numeric/timestamp columns are nullable: CSV cannot
			// distinguish NULL from "" for strings
			nullable: i%7 == 3 && t != source.TypeString && t != source.TypeBool,
		})
	}
	return cols
}

// splitmix64
func mix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func h(seed uint64, key int64, col int) uint64 {
	return mix(seed ^ mix(uint64(key)^mix(uint64(col)*0x9e3779b97f4a7c15)))
}

const b36 = "0123456789abcdefghijklmnopqrstuvwxyz"

func randString(v uint64, minLen, spread int) string {
	n := minLen + int(v%uint64(spread))
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		v = mix(v)
		sb.WriteByte(b36[v%36])
	}
	return sb.String()
}

type generator struct {
	seed    uint64
	cols    []colSpec
	stringy bool
}

// value computes the canonical value of (key, col ci); pass 2 for perturbed.
func (g *generator) value(key int64, ci int, perturbed bool) source.Value {
	c := g.cols[ci]
	v := h(g.seed, key, ci)
	out := source.Value{Type: c.typ}
	if c.nullable && v%50 == 0 {
		out.Null = true
		return out
	}
	switch c.typ {
	case source.TypeInt64:
		out.Int = int64(v % 1000000)
		if perturbed {
			out.Int++
		}
	case source.TypeFloat64:
		out.Float = float64(v%1000000000) / 1000.0
		if perturbed {
			out.Float += 1.5
		}
	case source.TypeString:
		if g.stringy {
			out.Str = randString(v, 24, 40)
		} else {
			out.Str = randString(v, 6, 10)
		}
		if perturbed {
			out.Str += "x"
		}
	case source.TypeTimestamp:
		// 2020-01-01 .. ~2026, µs precision
		out.Int = 1577836800_000000 + int64(v%200000000000000)
		if perturbed {
			out.Int += 1000000
		}
	case source.TypeBool:
		out.Int = int64(v & 1)
		if perturbed {
			out.Int ^= 1
		}
	case source.TypeDate:
		out.Int = 18000 + int64(v%2500)
		if perturbed {
			out.Int++
		}
	}
	return out
}

// classification of each base key in the right side
const (
	keep = iota
	removed
	changed
)

func (g *generator) classify(key int64, pctRemoved, pctChanged float64) int {
	u := float64(h(g.seed^0xABCD, key, -1)%1_000_000_000) / 1_000_000_000
	if u < pctRemoved {
		return removed
	}
	if u < pctRemoved+pctChanged {
		return changed
	}
	return keep
}

// perturbedCols picks which value columns change for a changed key.
func (g *generator) perturbedCols(key int64) []int {
	v := h(g.seed^0xF00D, key, -2)
	n := 1 + int(v%3)
	ncols := len(g.cols) - 1 // exclude id
	picked := map[int]bool{}
	out := []int{}
	for len(out) < n && len(out) < ncols {
		v = mix(v)
		ci := 1 + int(v%uint64(ncols))
		if !picked[ci] {
			picked[ci] = true
			out = append(out, ci)
		}
	}
	return out
}

// Manifest records the exact planted ground truth of a fixture pair.
type Manifest struct {
	Seed          uint64           `json:"seed"`
	Variant       string           `json:"variant"`
	Columns       int              `json:"columns"`
	RowsLeft      int64            `json:"rows_left"`
	RowsRight     int64            `json:"rows_right"`
	Added         int64            `json:"added"`
	Removed       int64            `json:"removed"`
	Changed       int64            `json:"changed"`
	ColumnChanges map[string]int64 `json:"column_changes"`
	KeysCapped    bool             `json:"keys_capped"`
	AddedKeys     []int64          `json:"added_keys,omitempty"`
	RemovedKeys   []int64          `json:"removed_keys,omitempty"`
	ChangedKeys   []int64          `json:"changed_keys,omitempty"`
}

const keyListCap = 1_000_000

type rowSink interface {
	write(vals []source.Value) error
	close() error
}

// ---- parquet sink ----

type parquetSink struct {
	f      *os.File
	w      *parquet.Writer
	rb     *parquet.RowBuilder
	colIdx []int // fixture column i → schema leaf column index
}

func parquetNode(t source.Type) parquet.Node {
	switch t {
	case source.TypeBool:
		return parquet.Leaf(parquet.BooleanType)
	case source.TypeInt64:
		return parquet.Int(64)
	case source.TypeFloat64:
		return parquet.Leaf(parquet.DoubleType)
	case source.TypeString:
		return parquet.String()
	case source.TypeTimestamp:
		return parquet.Timestamp(parquet.Microsecond)
	case source.TypeDate:
		return parquet.Date()
	}
	panic("bad type")
}

func newParquetSink(path string, cols []colSpec) (*parquetSink, error) {
	group := parquet.Group{}
	for _, c := range cols {
		n := parquetNode(c.typ)
		if c.nullable {
			n = parquet.Optional(n)
		}
		group[c.name] = n
	}
	schema := parquet.NewSchema("fixture", group)
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := parquet.NewWriter(f, schema, parquet.Compression(&parquet.Snappy))
	// parquet.Group sorts fields alphabetically; map fixture column order →
	// schema leaf column index for the RowBuilder
	colIdx := make([]int, len(cols))
	for i, c := range cols {
		leaf, ok := schema.Lookup(c.name)
		if !ok {
			f.Close()
			return nil, fmt.Errorf("column %q not found in schema", c.name)
		}
		colIdx[i] = leaf.ColumnIndex
	}
	return &parquetSink{f: f, w: w, rb: parquet.NewRowBuilder(schema), colIdx: colIdx}, nil
}

func (s *parquetSink) write(vals []source.Value) error {
	s.rb.Reset()
	for i := range vals {
		v := &vals[i]
		ci := s.colIdx[i]
		if v.Null {
			// leave column empty → null for optional columns
			continue
		}
		var pv parquet.Value
		switch v.Type {
		case source.TypeBool:
			pv = parquet.BooleanValue(v.Int != 0)
		case source.TypeInt64, source.TypeTimestamp:
			pv = parquet.Int64Value(v.Int)
		case source.TypeFloat64:
			pv = parquet.DoubleValue(v.Float)
		case source.TypeString:
			pv = parquet.ByteArrayValue([]byte(v.Str))
		case source.TypeDate:
			pv = parquet.Int32Value(int32(v.Int))
		}
		s.rb.Add(ci, pv)
	}
	_, err := s.w.WriteRows([]parquet.Row{s.rb.Row()})
	return err
}

func (s *parquetSink) close() error {
	if err := s.w.Close(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}

// ---- csv sink ----

type csvSink struct {
	f *os.File
	b *bufio.Writer
	w *csv.Writer
	r []string
}

func newCSVSink(path string, cols []colSpec) (*csvSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	b := bufio.NewWriterSize(f, 1<<20)
	w := csv.NewWriter(b)
	header := make([]string, len(cols))
	for i, c := range cols {
		header[i] = c.name
	}
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, err
	}
	return &csvSink{f: f, b: b, w: w, r: make([]string, len(cols))}, nil
}

func csvField(v *source.Value) string {
	if v.Null {
		return ""
	}
	switch v.Type {
	case source.TypeBool:
		if v.Int != 0 {
			return "true"
		}
		return "false"
	case source.TypeInt64:
		return strconv.FormatInt(v.Int, 10)
	case source.TypeFloat64:
		return strconv.FormatFloat(v.Float, 'g', -1, 64)
	case source.TypeString:
		return v.Str
	case source.TypeTimestamp:
		return source.TimestampMicrosToString(v.Int)
	case source.TypeDate:
		return source.DateDaysToString(int32(v.Int))
	}
	return ""
}

func (s *csvSink) write(vals []source.Value) error {
	for i := range vals {
		s.r[i] = csvField(&vals[i])
	}
	return s.w.Write(s.r)
}

func (s *csvSink) close() error {
	s.w.Flush()
	if err := s.w.Error(); err != nil {
		s.f.Close()
		return err
	}
	if err := s.b.Flush(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}

// Generate writes the fixture pair described by cfg plus manifest.json, and
// returns the manifest.
func Generate(cfg Config) (*Manifest, error) {
	rows, ncols, seed := cfg.Rows, cfg.Cols, cfg.Seed
	pctChanged, pctAdded, pctRemoved := cfg.PctChanged, cfg.PctAdded, cfg.PctRemoved
	out, formats, variant := cfg.Out, cfg.Formats, cfg.Variant
	if ncols == 0 {
		ncols = 15
	}
	if variant == "" {
		variant = "standard"
	}
	if variant == "wide" && ncols == 15 {
		ncols = 100
	}
	if len(formats) == 0 {
		formats = []string{"parquet", "csv"}
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return nil, err
	}
	cols := columns(variant, ncols)
	g := &generator{seed: seed, cols: cols, stringy: variant == "stringy"}

	man := Manifest{
		Seed: seed, Variant: variant, Columns: ncols,
		ColumnChanges: map[string]int64{},
	}
	addedRows := int64(float64(rows) * pctAdded)

	// deterministic permutation stride for right-side row order
	stride := int64(0)
	if rows > 1 {
		stride = 6364136223846793005 % rows
		for gcd(stride, rows) != 1 {
			stride = (stride + 1) % rows
			if stride == 0 {
				stride = 1
			}
		}
	}

	for _, format := range formats {
		format = strings.TrimSpace(format)
		lPath := filepath.Join(out, "left."+format)
		rPath := filepath.Join(out, "right."+format)
		first := man.RowsLeft == 0 // record manifest counts only once

		lSink, err := newSink(format, lPath, cols)
		if err != nil {
			return nil, err
		}
		rSink, err := newSink(format, rPath, cols)
		if err != nil {
			return nil, err
		}

		vals := make([]source.Value, len(cols))
		fill := func(key int64, perturbedCols []int) {
			vals[0] = source.Value{Type: source.TypeInt64, Int: key}
			for ci := 1; ci < len(cols); ci++ {
				vals[ci] = g.value(key, ci, false)
			}
			for _, ci := range perturbedCols {
				pv := g.value(key, ci, true)
				if !vals[ci].Null { // never perturb a NULL: keep classes clean
					vals[ci] = pv
				}
			}
		}

		// left: keys 0..rows-1 in order
		for key := int64(0); key < rows; key++ {
			fill(key, nil)
			if err := lSink.write(vals); err != nil {
				return nil, err
			}
		}
		if err := lSink.close(); err != nil {
			return nil, err
		}
		if first {
			man.RowsLeft = rows
		}

		// right: permuted order, apply classification
		for j := int64(0); j < rows; j++ {
			key := j
			if stride > 0 {
				key = (j * stride) % rows
			}
			switch g.classify(key, pctRemoved, pctChanged) {
			case removed:
				if first {
					man.Removed++
					if man.Removed <= keyListCap {
						man.RemovedKeys = append(man.RemovedKeys, key)
					}
				}
				continue
			case changed:
				pc := g.perturbedCols(key)
				// count only columns that actually change (NULLs skipped)
				var effective []int
				for _, ci := range pc {
					if !g.value(key, ci, false).Null {
						effective = append(effective, ci)
					}
				}
				if len(effective) == 0 { // all picked columns were NULL: emit unchanged
					fill(key, nil)
					if err := rSink.write(vals); err != nil {
						return nil, err
					}
					if first {
						man.RowsRight++
					}
					continue
				}
				fill(key, effective)
				if err := rSink.write(vals); err != nil {
					return nil, err
				}
				if first {
					man.Changed++
					man.RowsRight++
					for _, ci := range effective {
						man.ColumnChanges[cols[ci].name]++
					}
					if man.Changed <= keyListCap {
						man.ChangedKeys = append(man.ChangedKeys, key)
					}
				}
			default:
				fill(key, nil)
				if err := rSink.write(vals); err != nil {
					return nil, err
				}
				if first {
					man.RowsRight++
				}
			}
		}
		// added keys: rows..rows+addedRows-1
		for key := rows; key < rows+addedRows; key++ {
			fill(key, nil)
			if err := rSink.write(vals); err != nil {
				return nil, err
			}
			if first {
				man.Added++
				man.RowsRight++
				if man.Added <= keyListCap {
					man.AddedKeys = append(man.AddedKeys, key)
				}
			}
		}
		if err := rSink.close(); err != nil {
			return nil, err
		}
	}

	if man.Added > keyListCap || man.Removed > keyListCap || man.Changed > keyListCap {
		man.KeysCapped = true
		man.AddedKeys, man.RemovedKeys, man.ChangedKeys = nil, nil, nil
	}
	mf, err := os.Create(filepath.Join(out, "manifest.json"))
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(mf)
	enc.SetIndent("", " ")
	if err := enc.Encode(&man); err != nil {
		mf.Close()
		return nil, err
	}
	return &man, mf.Close()
}

func newSink(format, path string, cols []colSpec) (rowSink, error) {
	switch format {
	case "parquet":
		return newParquetSink(path, cols)
	case "csv":
		return newCSVSink(path, cols)
	default:
		return nil, fmt.Errorf("unknown format %q", format)
	}
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
