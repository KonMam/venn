package diff

// Snapshot mode: persist a table's (keyHash, rowHash) pairs — 16 bytes per
// row — as a baseline, then diff a live file against the baseline without
// keeping the original data. Made for CI: regression-test yesterday's 8 GB
// export with a 160 MB .snap file.
//
// Format: a JSON header line (magic, version, key/value column layout,
// float precision — a diff against an incompatible snapshot is refused),
// then raw little-endian hash pairs, the same layout the streaming spill
// uses, so loading is one read + a zero-copy cast.
//
// Semantics vs a snapshot: counts are exact; changed/added keys (present in
// the live file) are displayable; removed keys exist only as hashes in the
// snapshot, so removed examples are unavailable — reported as counts only.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"tdiff/internal/schema"
	"tdiff/internal/source"
)

const snapMagic = "tdiffsnap1"

type snapHeader struct {
	Magic          string        `json:"magic"`
	KeyNames       []string      `json:"key_names"`
	KeyModes       []compareMode `json:"key_modes"`
	ValNames       []string      `json:"val_names"`
	ValModes       []compareMode `json:"val_modes"`
	FloatPrecision int           `json:"float_precision"`
	Rows           int64         `json:"rows"`
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
	e := &engine{p: p, opts: &opts}
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
	_ = e
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
	if int64(len(finalHeader)+1) <= headerLen {
		pad := headerLen - int64(len(finalHeader)) - 1
		padded := append(finalHeader, bytes.Repeat([]byte{' '}, int(pad))...)
		if _, err := f.WriteAt(append(padded, '\n'), 0); err != nil {
			f.Close()
			return 0, err
		}
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
		return nil, fmt.Errorf("%s: not a tdiff snapshot: %w", snapPath, err)
	}
	var hdr snapHeader
	if err := json.Unmarshal(bytes.TrimSpace(headerLine), &hdr); err != nil || hdr.Magic != snapMagic {
		return nil, fmt.Errorf("%s: not a tdiff snapshot", snapPath)
	}

	p, err := snapPlan(right.Schema(), Options{
		Keys: hdr.KeyNames, IgnoreColumns: opts.IgnoreColumns,
		FloatPrecision: hdr.FloatPrecision,
	})
	if err != nil {
		return nil, fmt.Errorf("live file no longer matches the snapshot layout: %w", err)
	}
	if strings.Join(p.valNames, ",") != strings.Join(hdr.ValNames, ",") ||
		fmt.Sprint(p.valModes) != fmt.Sprint(hdr.ValModes) ||
		fmt.Sprint(p.keyModes) != fmt.Sprint(hdr.KeyModes) {
		return nil, fmt.Errorf("snapshot layout mismatch: snapshot compares %v, live file has %v — re-snapshot or align schemas",
			hdr.ValNames, p.valNames)
	}
	if opts.FloatPrecision != 0 && opts.FloatPrecision != hdr.FloatPrecision {
		return nil, fmt.Errorf("snapshot was taken with --float-precision %d", hdr.FloatPrecision)
	}

	res := &Result{ColumnChanges: map[string]int64{}}
	res.LeftRows = hdr.Rows

	// build the table from the snapshot pairs: a reader goroutine hands
	// 1 MB chunks to workers that batch inserts per stripe (same pattern as
	// the build pass — one lock per 512 rows, not per row)
	table := newStripedTable(int(hdr.Rows))
	warnDup := opts.OnDup == "warn"
	type chunk struct{ buf []byte }
	work := make(chan chunk, opts.Threads)
	free := make(chan []byte, opts.Threads+2)
	for i := 0; i < opts.Threads+2; i++ {
		free <- make([]byte, 1<<20)
	}
	var dupN atomic.Int64
	var loadErr atomic.Pointer[error]
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
						err := fmt.Errorf("duplicate key hash in snapshot — re-create it with --on-dup warn")
						loadErr.CompareAndSwap(nil, &err)
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
	for loadErr.Load() == nil {
		buf := <-free
		n, rerr := io.ReadFull(br, buf)
		if n > 0 {
			work <- chunk{buf: buf[:n-n%16]}
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
	// not keep — changed keys come from pass 2's stored rows instead
	for _, sr := range e.changed {
		if len(res.ChangedExamples) < opts.Limit {
			res.ChangedExamples = append(res.ChangedExamples, RowExample{Key: sr.key})
		}
	}
	sort.Slice(res.ChangedExamples, func(i, j int) bool {
		return res.ChangedExamples[i].Key < res.ChangedExamples[j].Key
	})
	sort.Strings(res.AddedExamples)
	res.AddedExamples = trim(res.AddedExamples, opts.Limit)
	return res, nil
}

func defaultThreads() int { return runtime.GOMAXPROCS(0) }
