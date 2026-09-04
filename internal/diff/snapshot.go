package diff

// Snapshot mode: persist a table's (keyHash, rowHash) pairs, 16 bytes per
// row, as a baseline, then diff a live file against the baseline without
// keeping the original data. Made for CI: regression-test yesterday's 8 GB
// export with a 160 MB .snap file.
//
// Format: a JSON header line (magic, version, key/value column layout, and
// every hash-affecting comparison setting (float precision and the value
// normalizations; a diff against an incompatible snapshot is refused),
// then raw little-endian hash pairs, the same layout the streaming spill
// uses, so loading is one read + a zero-copy cast.
//
// Semantics vs a snapshot: counts are exact; changed/added keys (present in
// the live file) are displayable; removed keys exist only as hashes in the
// snapshot, so removed examples are unavailable and reported as counts only.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/KonMam/venn/internal/schema"
	"github.com/KonMam/venn/internal/source"
)

const snapMagic = "vennsnap1"

type snapHeader struct {
	Magic          string        `json:"magic"`
	KeyNames       []string      `json:"key_names"`
	KeyModes       []compareMode `json:"key_modes"`
	ValNames       []string      `json:"val_names"`
	ValModes       []compareMode `json:"val_modes"`
	FloatPrecision int           `json:"float_precision"`
	// normalization settings: hash-affecting, so a baseline taken under
	// different ones cannot be compared (see normalize.go)
	IgnoreCase         bool   `json:"ignore_case,omitempty"`
	Trim               bool   `json:"trim,omitempty"`
	TimestampPrecision string `json:"timestamp_precision,omitempty"`
	// Where is the row filter the baseline was taken under. It is not
	// hash-affecting, it is row-affecting: comparing a filtered baseline
	// against an unfiltered file would report the whole difference in
	// filters as removed rows.
	Where string `json:"where,omitempty"`
	Rows  int64  `json:"rows"`
}

// normOptions returns the option subset the header pins.
func (h *snapHeader) normOptions() Options {
	return Options{
		FloatPrecision: h.FloatPrecision, IgnoreCase: h.IgnoreCase,
		Trim: h.Trim, TimestampPrecision: h.TimestampPrecision,
	}
}

// snapPlan builds the hashing plan for a single source (both "sides" are the
// same schema, so modes resolve against itself). Value columns are put in
// canonical (sorted) order: snapshot hashes must not depend on the physical
// column order of whichever format wrote the file.
func snapPlan(s source.Schema, opts Options) (*plan, error) {
	sd := schema.Compare(s, s)
	p, err := buildPlan(&sd, s, s, &opts)
	if err != nil {
		return nil, err
	}
	order := make([]int, len(p.valNames))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return p.valNames[order[a]] < p.valNames[order[b]] })
	permute := func(dst []int) []int {
		out := make([]int, len(dst))
		for i, o := range order {
			out[i] = dst[o]
		}
		return out
	}
	names := make([]string, len(order))
	modes := make([]compareMode, len(order))
	for i, o := range order {
		names[i] = p.valNames[o]
		modes[i] = p.valModes[o]
	}
	p.valNames, p.valModes = names, modes
	p.leftVal = permute(p.leftVal)
	p.rightVal = permute(p.rightVal)
	return p, nil
}

// WriteSnapshot scans src and writes its hash manifest to path.
func WriteSnapshot(src source.Source, path string, opts Options) (int64, error) {
	if opts.Threads == 0 {
		opts.Threads = defaultThreads()
	}
	p, err := snapPlan(src.Schema(), opts)
	if err != nil {
		return 0, err
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	hdr := snapHeader{
		Magic: snapMagic, KeyNames: p.keyNames, KeyModes: p.keyModes,
		ValNames: p.valNames, ValModes: p.valModes, FloatPrecision: opts.FloatPrecision,
		IgnoreCase: opts.IgnoreCase, Trim: opts.Trim,
		TimestampPrecision: opts.TimestampPrecision, Where: opts.Where,
	}
	// header goes first with slack for the final row count, rewritten at the
	// end in place (the padding keeps the byte offset of the pairs stable)
	headerBytes, _ := json.Marshal(&hdr)
	headerBytes = append(headerBytes, bytes.Repeat([]byte{' '}, 24)...)
	if _, err := fmt.Fprintf(w, "%s\n", headerBytes); err != nil {
		f.Close()
		return 0, err
	}
	headerLen := int64(len(headerBytes) + 1)

	var mu sync.Mutex
	var total int64
	var werr error
	err = withMerge(src, opts.Threads, func() (source.BatchFunc, func()) {
		var l lanes
		var rows int64
		var local []hashPair
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, p.leftKey, &l)
			p.hashVals(b, p.leftVal, &l)
			for r := 0; r < b.N; r++ {
				local = append(local, hashPair{kh: l.khs[r], rh: l.rhs[r]})
				if len(local) == 4096 {
					mu.Lock()
					err := writePairsTo(w, local)
					mu.Unlock()
					if err != nil {
						return err
					}
					local = local[:0]
				}
			}
			return nil
		}
		return fn, func() {
			mu.Lock()
			if len(local) > 0 && werr == nil {
				werr = writePairsTo(w, local)
			}
			total += rows
			mu.Unlock()
		}
	})
	if err == nil {
		err = werr
	}
	if err != nil {
		f.Close()
		os.Remove(path)
		return 0, err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return 0, err
	}
	// rewrite the header in place with the true row count, preserving length
	hdr.Rows = total
	finalHeader, _ := json.Marshal(&hdr)
	if int64(len(finalHeader)+1) > headerLen {
		// cannot happen while the padding covers any int64 row count, but a
		// silently stale count would corrupt every later --against run
		f.Close()
		os.Remove(path)
		return 0, fmt.Errorf("snapshot header grew past its padding (%d > %d bytes)", len(finalHeader)+1, headerLen)
	}
	pad := headerLen - int64(len(finalHeader)) - 1
	padded := append(finalHeader, bytes.Repeat([]byte{' '}, int(pad))...)
	if _, err := f.WriteAt(append(padded, '\n'), 0); err != nil {
		f.Close()
		return 0, err
	}
	return total, f.Close()
}

func writePairsTo(w *bufio.Writer, pairs []hashPair) error {
	var rec [16]byte
	for _, pr := range pairs {
		binary.LittleEndian.PutUint64(rec[:8], pr.kh)
		binary.LittleEndian.PutUint64(rec[8:], pr.rh)
		if _, err := w.Write(rec[:]); err != nil {
			return err
		}
	}
	return nil
}

// DiffAgainstSnapshot compares a live source (right side) against a snapshot
// baseline (left side).
func DiffAgainstSnapshot(snapPath string, right source.Source, opts Options) (*Result, error) {
	if opts.Limit == 0 {
		opts.Limit = 10
	}
	if opts.Threads == 0 {
		opts.Threads = defaultThreads()
	}
	f, err := os.Open(snapPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	headerLine, err := br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("%s: not a venn snapshot: %w", snapPath, err)
	}
	var hdr snapHeader
	if err := json.Unmarshal(bytes.TrimSpace(headerLine), &hdr); err != nil || hdr.Magic != snapMagic {
		return nil, fmt.Errorf("%s: not a venn snapshot", snapPath)
	}

	live := hdr.normOptions()
	live.Keys, live.IgnoreColumns = hdr.KeyNames, opts.IgnoreColumns
	// --rename lets a live file whose columns were renamed still be compared
	// against the baseline. The snapshot has no left schema, only the names
	// it recorded, which is all ApplyRenames needs to validate a target.
	baseline := source.Schema{}
	for _, n := range append(append([]string{}, hdr.KeyNames...), hdr.ValNames...) {
		baseline.Columns = append(baseline.Columns, source.Column{Name: n})
	}
	liveSchema, renames, err := schema.ApplyRenames(baseline, right.Schema(), opts.Rename)
	if err != nil {
		return nil, err
	}
	p, err := snapPlan(liveSchema, live)
	if err != nil {
		return nil, fmt.Errorf("live file no longer matches the snapshot layout: %w", err)
	}
	if strings.Join(p.valNames, ",") != strings.Join(hdr.ValNames, ",") ||
		!slices.Equal(p.valModes, hdr.ValModes) ||
		!slices.Equal(p.keyModes, hdr.KeyModes) {
		return nil, fmt.Errorf("snapshot layout mismatch: snapshot compares %v, live file has %v; re-snapshot or align schemas",
			hdr.ValNames, p.valNames)
	}
	if opts.FloatPrecision != 0 && opts.FloatPrecision != hdr.FloatPrecision {
		return nil, fmt.Errorf("snapshot was taken with --float-precision %d", hdr.FloatPrecision)
	}
	// the normalizations are baked into the stored hashes: comparing under a
	// different set would silently compare different values
	if want, err := newNormalizer(&opts); err != nil {
		return nil, err
	} else if got := p.norm.settings(); want.settings() != "" && want.settings() != got {
		return nil, fmt.Errorf("snapshot was taken with normalization %q, this run asks for %q; re-snapshot or drop the flags",
			describeSettings(got), describeSettings(want.settings()))
	}
	if opts.Tolerance != nil || len(opts.ColumnTolerance) > 0 {
		return nil, fmt.Errorf("--tolerance cannot be used against a snapshot (a baseline stores hashes, not values); use --float-precision, which is hash-consistent")
	}
	if opts.Where != hdr.Where {
		return nil, fmt.Errorf("snapshot was taken with --where %s, this run filters by %s; the two cover different rows",
			describeFilter(hdr.Where), describeFilter(opts.Where))
	}

	res := &Result{ColumnChanges: map[string]int64{}}
	res.Filter = hdr.Where
	res.Schema.Renames = renames
	res.Comparison = p.describeComparison()
	res.Masked = p.maskedNames()
	res.LeftRows = hdr.Rows

	// build the table from the snapshot pairs: a reader goroutine hands
	// 1 MB chunks to workers that batch inserts per stripe (same pattern as
	// the build pass: one lock per 512 rows, not per row)
	table := newStripedTable(int(hdr.Rows), false)
	warnDup := opts.OnDup == "warn"
	type chunk struct{ buf []byte }
	work := make(chan chunk, opts.Threads)
	free := make(chan []byte, opts.Threads+2)
	for i := 0; i < opts.Threads+2; i++ {
		free <- make([]byte, 1<<20)
	}
	var dupN atomic.Int64
	var loadErr atomic.Pointer[error]
	// quit unblocks the reader when a worker fails: without it the reader
	// can block forever on free (workers exited holding buffers) or on work
	// (no consumers left); the error check at loop top is not enough.
	quit := make(chan struct{})
	var quitOnce sync.Once
	fail := func(err error) {
		loadErr.CompareAndSwap(nil, &err)
		quitOnce.Do(func() { close(quit) })
	}
	var wg sync.WaitGroup
	for w := 0; w < opts.Threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([][]hashPair, len(table.stripes))
			flush := func(si int) bool {
				st := &table.stripes[si]
				st.mu.Lock()
				for _, pr := range local[si] {
					if !st.t.insert(pr.kh, pr.rh) {
						if warnDup {
							dupN.Add(1)
							continue
						}
						st.mu.Unlock()
						fail(fmt.Errorf("duplicate key hash in snapshot; re-create it with --on-dup warn"))
						return false
					}
				}
				st.mu.Unlock()
				local[si] = local[si][:0]
				return true
			}
			for ch := range work {
				for _, pr := range pairsView(ch.buf) {
					si := table.stripe(pr.kh)
					local[si] = append(local[si], pr)
					if len(local[si]) >= insertBatch {
						if !flush(si) {
							return
						}
					}
				}
				free <- ch.buf[:cap(ch.buf)]
			}
			for si := range local {
				if len(local[si]) > 0 && !flush(si) {
					return
				}
			}
		}()
	}
readLoop:
	for {
		var buf []byte
		select {
		case buf = <-free:
		case <-quit:
			break readLoop
		}
		n, rerr := io.ReadFull(br, buf)
		if n%16 != 0 {
			// pooled buffers are 16-byte multiples, so a short tail can only
			// mean the file was truncated, so refuse rather than drop pairs
			fail(fmt.Errorf("snapshot truncated: %d trailing bytes are not a whole hash pair", n%16))
			break readLoop
		}
		if n > 0 {
			select {
			case work <- chunk{buf: buf[:n]}:
			case <-quit:
				break readLoop
			}
		}
		if rerr != nil {
			break
		}
	}
	close(work)
	wg.Wait()
	if ep := loadErr.Load(); ep != nil {
		return nil, *ep
	}
	res.DupsLeft += dupN.Load()
	for i := range table.stripes {
		table.stripes[i].t.seal()
	}

	// probe with the live file (same as in-memory pass 2, keys displayable)
	e := &engine{p: p, opts: &opts, table: table, res: res}
	if err := e.pass2(right); err != nil {
		return nil, fmt.Errorf("live: %w", err)
	}
	for i := range table.stripes {
		res.Removed += table.stripes[i].t.unmatchedCount()
	}
	// column attribution needs the baseline's values, which a snapshot does
	// not keep; changed keys come from pass 2's stored rows instead
	for _, sr := range e.changed {
		if len(res.ChangedExamples) < opts.Limit {
			res.ChangedExamples = append(res.ChangedExamples, RowExample{Key: sr.key})
		}
	}
	res.finishExamples(opts.Limit)
	res.FinishStats()
	return res, nil
}

func defaultThreads() int { return runtime.GOMAXPROCS(0) }
