package diff

// Streaming (larger-than-RAM) mode: a grace hash join.
//
//	pass A  scan both sides concurrently (their I/O stalls interleave),
//	        spilling (keyHash, rowHash) pairs into 128 partition files per
//	        side (partitioned by key-hash bits)
//	pass B  join matching partitions in parallel: each loads only its
//	        1/128th of the left keys into a table, so peak memory is bounded
//	        by partition size, not input size
//	pass C  only when attribution is wanted and rows differ: rescan both
//	        sides, spilling compact row payloads for keys in the changed
//	        set; join those per partition for column attribution, and
//	        collect example keys for added/removed rows
//
// Only hashes and (rarely) changed-row payloads ever hit disk; spill I/O is
// sequential. --summary streaming skips pass C entirely.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/KonMam/tdiff/internal/source"
)

const (
	// spill buffer per worker per partition, in pairs (16 B each)
	spillBufPairs = 1024
	// targetPartRows sizes the partition count: each partition's join table
	// should hold roughly this many rows (~32-68 MB with power-of-two
	// rounding), so memory stays bounded and cache-friendly at any scale.
	targetPartRows = 2 << 20
	// joinBudget caps how much partition-table memory concurrent joins may
	// hold, and the whole point of streaming mode is bounded memory.
	joinBudget = 448 << 20
)

// choosePartitionBits picks the partition count (as a power of two between
// 64 and 1024) from the estimated left row count.
func choosePartitionBits(leftRows int64) uint {
	bits := uint(6)
	for bits < 10 && leftRows > int64(targetPartRows)<<bits {
		bits++
	}
	return bits
}

// streamPart selects the partition for a key hash using the top bits.
// Partition joins run on unstriped per-partition tables (joinPartition), so
// overlapping with stripedTable's bit choice (also top bits) is harmless.
func streamPart(kh uint64, bits uint) int { return int(kh >> (64 - bits)) }

// spillSide is one side's set of partition files being written.
type spillSide struct {
	files []*os.File
	mu    []sync.Mutex
	bw    []*bufio.Writer
	bits  uint
}

func newSpillSide(dir, name string, bits uint) (*spillSide, error) {
	n := 1 << bits
	s := &spillSide{
		files: make([]*os.File, n),
		mu:    make([]sync.Mutex, n),
		bw:    make([]*bufio.Writer, n),
		bits:  bits,
	}
	for i := range s.files {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s-%02d.spill", name, i)))
		if err != nil {
			s.close()
			return nil, err
		}
		s.files[i] = f
		bw := 128 << 10
		if n > 256 {
			bw = 64 << 10
		}
		s.bw[i] = bufio.NewWriterSize(f, bw)
	}
	return s, nil
}

// writePairs appends encoded pairs to partition pi (thread-safe).
func (s *spillSide) writePairs(pi int, buf []byte) error {
	s.mu[pi].Lock()
	_, err := s.bw[pi].Write(buf)
	s.mu[pi].Unlock()
	return err
}

// writePairSlice writes pairs as raw records (LE layout == memory layout).
func (s *spillSide) writePairSlice(pi int, pairs []hashPair) error {
	if len(pairs) == 0 {
		return nil
	}
	b := unsafe.Slice((*byte)(unsafe.Pointer(&pairs[0])), len(pairs)*16)
	return s.writePairs(pi, b)
}

// flush drains the write buffers. Deliberately no fsync: spill files are
// scratch, so durability buys nothing (a lost spill is a failed run either
// way), reads come straight from the page cache, and files deleted before
// writeback may never touch the disk at all. Write errors (incl. disk-full)
// still surface from Flush.
func (s *spillSide) flush() error {
	for _, w := range s.bw {
		if w == nil {
			continue
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// pairsView reinterprets 16-byte spill records as hashPairs. Records are
// written little-endian, which matches every platform tdiff builds for; the
// buffer is heap-allocated and 8-aligned.
func pairsView(buf []byte) []hashPair {
	if len(buf) == 0 {
		return nil
	}
	return unsafe.Slice((*hashPair)(unsafe.Pointer(&buf[0])), len(buf)/16)
}

// forEachPairs streams one partition's pairs through fn in batches without
// materializing the whole partition (the probe side needs no random access).
func (s *spillSide) forEachPairs(pi int, fn func([]hashPair)) error {
	f := s.files[pi]
	st, err := f.Stat()
	if err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	off := int64(0)
	for off < st.Size() {
		want := int64(len(buf))
		if st.Size()-off < want {
			want = st.Size() - off
		}
		if _, err := f.ReadAt(buf[:want], off); err != nil {
			return err
		}
		fn(pairsView(buf[:want]))
		off += want
	}
	return nil
}

// readPartitionInto loads one partition's pairs into a reusable buffer.
func (s *spillSide) readPartitionInto(pi int, buf *[]byte) ([]hashPair, error) {
	f := s.files[pi]
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	n := int(st.Size())
	if cap(*buf) < n {
		*buf = make([]byte, n)
	}
	data := (*buf)[:n]
	if n > 0 {
		if _, err := f.ReadAt(data, 0); err != nil {
			return nil, err
		}
	}
	return pairsView(data), nil
}

func (s *spillSide) close() {
	for _, f := range s.files {
		if f != nil {
			f.Close()
		}
	}
}

// streamDebugTiming prints phase wall times when TDIFF_DEBUG_TIMING is set.
var streamDebugTiming = os.Getenv("TDIFF_DEBUG_TIMING") != ""

func phaseDone(name string, start time.Time) {
	if streamDebugTiming {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		fmt.Fprintf(os.Stderr, "[timing] %-12s %6.2fs  heapInUse=%dMB heapSys=%dMB stacks=%dMB otherSys=%dMB totalSys=%dMB\n",
			name, time.Since(start).Seconds(),
			ms.HeapInuse>>20, ms.HeapSys>>20, ms.StackSys>>20,
			(ms.MSpanSys+ms.MCacheSys+ms.BuckHashSys+ms.GCSys+ms.OtherSys)>>20, ms.Sys>>20)
	}
}

// runStream executes the diff in streaming mode.
func runStream(left, right source.Source, opts Options, p *plan, res *Result) (*Result, error) {
	tmpDir, err := os.MkdirTemp(opts.TempDir, "tdiff-spill-*")
	if err != nil {
		return nil, err
	}
	// streaming's contract is bounded memory at any input size, so encode it:
	// the GC keeps the heap under the limit instead of scaling with input
	oldLimit := debug.SetMemoryLimit(1 << 30)
	defer debug.SetMemoryLimit(oldLimit)
	defer func() {
		t := time.Now()
		os.RemoveAll(tmpDir)
		phaseDone("cleanup", t)
	}()

	e := &engine{p: p, opts: &opts, res: res}

	leftRows := int64(64 << 20) // conservative default when unknown
	if n, ok := left.(interface{ NumRows() int64 }); ok && n.NumRows() > 0 {
		leftRows = n.NumRows()
	} else if sz, ok := left.(interface{ SizeBytes() int64 }); ok && sz.SizeBytes() > 0 {
		leftRows = sz.SizeBytes() / 64 // rough rows-per-byte guess for text
	}
	bits := choosePartitionBits(leftRows)
	nParts := 1 << bits

	tCreate := time.Now()
	lSpill, err := newSpillSide(tmpDir, "l", bits)
	if err != nil {
		return nil, err
	}
	defer lSpill.close()
	rSpill, err := newSpillSide(tmpDir, "r", bits)
	if err != nil {
		return nil, err
	}
	defer rSpill.close()
	phaseDone("spill-create", tCreate)

	opts.Progress.setPhase("scan inputs")
	// pass A: spill hash pairs for both sides. The sides are independent
	// until the join, so they scan concurrently: each side's compute fills
	// the other side's I/O stalls, and the disk sustains both streams.
	var scanWG sync.WaitGroup
	var lErr, rErr error
	tA := time.Now()
	scanWG.Add(2)
	// full worker count per side: the sides' I/O stalls interleave, so 2×
	// oversubscription buys real throughput here (measured ~20%)
	sideThreads := opts.Threads
	go func() {
		defer scanWG.Done()
		n, err := e.streamSpill(left, sideThreads, p.leftKey, p.leftVal, lSpill)
		res.LeftRows, lErr = n, err
		phaseDone("scan-left", tA)
	}()
	go func() {
		defer scanWG.Done()
		n, err := e.streamSpill(right, sideThreads, p.rightKey, p.rightVal, rSpill)
		res.RightRows, rErr = n, err
		phaseDone("scan-right", tA)
	}()
	scanWG.Wait()
	phaseDone("passA", tA)
	if lErr != nil {
		return nil, fmt.Errorf("left: %w", lErr)
	}
	if rErr != nil {
		return nil, fmt.Errorf("right: %w", rErr)
	}
	if err := lSpill.flush(); err != nil {
		return nil, err
	}
	if err := rSpill.flush(); err != nil {
		return nil, err
	}
	tGC := time.Now()
	runtime.GC() // release scan-phase buffers before the join allocates tables
	phaseDone("gc-barrier", tGC)
	tB := time.Now()

	// pass B: join partitions in parallel. Concurrency adapts to partition
	// size: each in-flight join holds a table of ~16 B/row at ≤0.85 load
	// (power-of-two slots), and the joins together must respect the memory
	// budget that makes streaming mode worth using.
	results := make([]partResult, nParts)
	joinP := opts.Threads
	if st, err := os.Stat(lSpill.files[0].Name()); err == nil && st.Size() > 0 {
		pairs := st.Size() / 16
		slots := uint64(16)
		for float64(pairs) > float64(slots)*tableMaxLoad {
			slots *= 2
		}
		// per in-flight join: table (keys+rows+matched) + the left pair read
		// buffer; the right side streams through a fixed 1 MB window
		perWorker := int64(slots)*17 + st.Size()
		if maxP := int(int64(joinBudget) / max(perWorker, 1)); maxP < joinP {
			joinP = max(1, maxP)
		}
	}
	// joinP workers each own one reusable table + read buffer and walk the
	// partition list, so large allocations happen once per worker, not once
	// per partition
	var wg sync.WaitGroup
	partCh := make(chan int)
	for w := 0; w < joinP; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := &keyTable{}
			var pairBuf []byte
			for pi := range partCh {
				joinPartition(pi, t, &pairBuf, lSpill, rSpill, &results[pi], &opts)
			}
		}()
	}
	for pi := 0; pi < nParts; pi++ {
		partCh <- pi
	}
	close(partCh)
	wg.Wait()

	phaseDone("passB", tB)

	var changedKh, removedKh, addedKh []uint64
	for pi := range results {
		pr := &results[pi]
		if pr.err != nil {
			if dup, ok := errAs[errDuplicateKey](pr.err); ok {
				return nil, fmt.Errorf("left: duplicate key %s; keyed diff requires unique keys",
					findKeyByHash(left, p, dup.kh))
			}
			return nil, pr.err
		}
		res.Added += pr.added
		res.Removed += pr.removed
		res.Changed += pr.changed
		res.Unchanged += pr.unchanged
		res.DupsLeft += pr.dupsLeft
		res.DupsRight += pr.dupsRight
		changedKh = append(changedKh, pr.changedKh...)
		removedKh = append(removedKh, pr.removedKh...)
		addedKh = append(addedKh, pr.addedKh...)
	}

	// both row counts are final here, so the budget resolves exactly: an
	// over-budget run can skip pass C's two extra scans outright
	if opts.MaxDiff != "" && opts.Sink == nil {
		budget, err := ParseBudget(opts.MaxDiff, max(res.LeftRows, res.RightRows))
		if err != nil {
			return nil, err
		}
		if total := res.Added + res.Removed + res.Changed; total > budget {
			res.abort(errBudgetExceeded{seen: total, budget: budget}, false)
			return res, nil
		}
	}
	if opts.Summary || res.RowsSame() {
		res.FinishStats()
		return res, nil
	}

	// pass C: attribution + examples
	runtime.GC() // pass B garbage (partition tables) goes before pass C allocates scan state
	opts.Progress.setPhase("attribute changes")
	tC := time.Now()
	if err := e.streamAttribute(left, right, tmpDir, changedKh, removedKh, addedKh); err != nil {
		return nil, err
	}
	res.finishExamples(opts.Limit)
	res.FinishStats()
	phaseDone("passC", tC)
	return res, nil
}

// streamSpill runs pass A for one side.
func (e *engine) streamSpill(src source.Source, threads int, keyIdx, valIdx []int, side *spillSide) (int64, error) {
	p := e.p
	var mu sync.Mutex
	var total int64
	var deferred error
	err := withMerge(src, threads, func() (source.BatchFunc, func()) {
		var l lanes
		var rows int64
		var werr error
		// per-partition accumulation shrinks as partitions grow so each
		// worker's buffer set stays ~2 MB regardless of partition count
		perPart := max(128, spillBufPairs*128/len(side.files))
		bufs := make([][]hashPair, len(side.files))
		for i := range bufs {
			bufs[i] = make([]hashPair, 0, perPart)
		}
		fn := func(b *source.Batch) error {
			rows += int64(b.N)
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, keyIdx, &l)
			p.hashVals(b, valIdx, &l)
			for r := 0; r < b.N; r++ {
				kh := l.khs[r]
				pi := streamPart(kh, side.bits)
				buf := append(bufs[pi], hashPair{kh: kh, rh: l.rhs[r]})
				if len(buf) == cap(buf) {
					if err := side.writePairSlice(pi, buf); err != nil {
						return err
					}
					buf = buf[:0]
				}
				bufs[pi] = buf
			}
			return nil
		}
		return fn, func() {
			for pi, buf := range bufs {
				if len(buf) > 0 && werr == nil {
					werr = side.writePairSlice(pi, buf)
				}
			}
			mu.Lock()
			total += rows
			if werr != nil && deferred == nil {
				deferred = werr
			}
			mu.Unlock()
		}
	})
	if err == nil {
		err = deferred
	}
	return total, err
}

// khSet is a sorted-slice set of key hashes (memory-lean membership tests).
type khSet []uint64

func newKhSet(khs []uint64) khSet {
	sort.Slice(khs, func(i, j int) bool { return khs[i] < khs[j] })
	return khSet(khs)
}

func (s khSet) has(kh uint64) bool {
	i := sort.Search(len(s), func(i int) bool { return s[i] >= kh })
	return i < len(s) && s[i] == kh
}

// streamAttribute runs pass C: rescan both sides, spill row payloads for
// changed keys, join per partition; collect example keys along the way.
func (e *engine) streamAttribute(left, right source.Source, tmpDir string, changedKh, removedKh, addedKh []uint64) error {
	p, res, opts := e.p, e.res, e.opts
	changed := newKhSet(changedKh)
	if opts.Sink == nil { // examples only: a handful of keys suffice
		removedKh = trim(removedKh, opts.Limit*4)
		addedKh = trim(addedKh, opts.Limit*4)
	}
	removed := newKhSet(removedKh)
	added := newKhSet(addedKh)

	bits := uint(6) // pass C spills only changed rows: 64 partitions suffice
	lRows, err := newSpillSide(tmpDir, "lc", bits)
	if err != nil {
		return err
	}
	defer lRows.close()
	rRows, err := newSpillSide(tmpDir, "rc", bits)
	if err != nil {
		return err
	}
	defer rRows.close()

	var rescanWG sync.WaitGroup
	var collectL, collectR []string
	var lErr, rErr error
	rescanWG.Add(2)
	// 3/4 workers per side here: pass C's scan buffers set full-mode peak
	// memory, but starving it below this costs real wall time
	sideThreads := max(1, opts.Threads*3/4)
	go func() {
		defer rescanWG.Done()
		collectL, lErr = e.spillChangedRows(left, sideThreads, p.leftKey, p.leftVal, changed, removed, lRows, 'r')
	}()
	go func() {
		defer rescanWG.Done()
		collectR, rErr = e.spillChangedRows(right, sideThreads, p.rightKey, p.rightVal, changed, added, rRows, 'a')
	}()
	rescanWG.Wait()
	if lErr != nil {
		return fmt.Errorf("left: %w", lErr)
	}
	if rErr != nil {
		return fmt.Errorf("right: %w", rErr)
	}
	res.RemovedExamples = append(res.RemovedExamples, collectL...)
	res.AddedExamples = append(res.AddedExamples, collectR...)
	if err := lRows.flush(); err != nil {
		return err
	}
	if err := rRows.flush(); err != nil {
		return err
	}

	// join changed-row payloads per partition
	cols := make([]colAcc, len(p.valNames))
	var withinTol int64
	var exMu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, opts.Threads)
	nParts := len(lRows.files)
	errs := make([]error, nParts)
	partCols := make([][]colAcc, nParts)
	partWithinTol := make([]int64, nParts)
	for pi := 0; pi < nParts; pi++ {
		wg.Add(1)
		go func(pi int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			cc := make([]colAcc, len(p.valNames))
			partCols[pi] = cc
			var diffs []colDiff
			lrows, err := readRowSpill(lRows.files[pi], len(p.keyNames), len(p.valNames))
			if err != nil {
				errs[pi] = err
				return
			}
			rrows, err := readRowSpill(rRows.files[pi], len(p.keyNames), len(p.valNames))
			if err != nil {
				errs[pi] = err
				return
			}
			byKh := make(map[uint64]spilledRow, len(lrows))
			for _, sr := range lrows {
				byKh[sr.kh] = sr
			}
			var localEx []RowExample
			for _, rr := range rrows {
				lr, ok := byKh[rr.kh]
				if !ok {
					continue
				}
				var beyond int
				diffs, beyond = p.rowDiffs(lr.vals, rr.vals, diffs)
				if p.hasTol && beyond == 0 {
					partWithinTol[pi]++
					continue
				}
				example := RowExample{Key: rr.key}
				wantExample := len(localEx) < opts.Limit
				for di := range diffs {
					d := &diffs[di]
					if !d.beyond {
						continue // inside tolerance: not a difference
					}
					cc[d.i].add(d)
					if wantExample {
						example.Columns = append(example.Columns, ColumnChange{
							Column: p.valNames[d.i],
							Left:   p.showVal(d.i, &lr.vals[d.i]),
							Right:  p.showVal(d.i, &rr.vals[d.i]),
						})
					}
				}
				if wantExample {
					localEx = append(localEx, example)
				}
				if opts.Sink != nil {
					// the spilled rows are consumed once, after comparison
					maskRow(rr.keyVals, p.maskKey)
					maskRow(lr.vals, p.maskVal)
					maskRow(rr.vals, p.maskVal)
					if err := opts.Sink.WriteDiffRow('c', rr.keyVals, lr.vals, rr.vals); err != nil {
						errs[pi] = err
						return
					}
				}
			}
			exMu.Lock()
			res.ChangedExamples = append(res.ChangedExamples, localEx...)
			exMu.Unlock()
		}(pi)
	}
	wg.Wait()
	for pi := range errs {
		if errs[pi] != nil {
			return errs[pi]
		}
		for i := range partCols[pi] {
			cols[i].merge(&partCols[pi][i])
		}
		withinTol += partWithinTol[pi]
	}
	res.mergeColStats(p.valNames, cols)
	res.Changed -= withinTol
	res.WithinTolerance += withinTol
	return nil
}

// spilledRow is one serialized changed row.
type spilledRow struct {
	kh      uint64
	key     string
	keyVals []source.Value
	vals    []source.Value
}

// spillChangedRows rescans src; rows whose key hash is in spillSet get their
// comparison values serialized to the partitioned row spill. Rows in
// exampleSet get their key display collected (returned) and, when a sink is
// exporting, the full row emitted with sinkStatus.
func (e *engine) spillChangedRows(src source.Source, threads int, keyIdx, valIdx []int, spillSet, exampleSet khSet, side *spillSide, sinkStatus byte) ([]string, error) {
	p := e.p
	var mu sync.Mutex
	var examples []string
	var deferred error
	sink := e.opts.Sink
	err := withMerge(src, threads, func() (source.BatchFunc, func()) {
		var l lanes
		var localEx []string
		var werr error
		var kBuf, vBuf []source.Value
		bufs := make([][]byte, len(side.files))
		fn := func(b *source.Batch) error {
			e.opts.Progress.add(int64(b.N))
			l.size(b.N)
			p.hashKeys(b, keyIdx, &l)
			for r := 0; r < b.N; r++ {
				kh := l.khs[r]
				if len(exampleSet) > 0 && (len(localEx) < e.opts.Limit || sink != nil) && exampleSet.has(kh) {
					if len(localEx) < e.opts.Limit {
						localEx = append(localEx, p.keyDisplayBatch(b, r, keyIdx))
					}
					if sink != nil {
						kBuf = gatherRow(b, r, keyIdx, kBuf)
						vBuf = gatherRow(b, r, valIdx, vBuf)
						maskRow(kBuf, p.maskKey)
						maskRow(vBuf, p.maskVal)
						var lv, rv []source.Value
						if sinkStatus == 'r' {
							lv = vBuf
						} else {
							rv = vBuf
						}
						if err := sink.WriteDiffRow(sinkStatus, kBuf, lv, rv); err != nil {
							return err
						}
					}
				}
				if len(spillSet) > 0 && spillSet.has(kh) {
					pi := streamPart(kh, side.bits)
					bufs[pi] = encodeRow(bufs[pi], kh, p.keyDisplayBatch(b, r, keyIdx), b, r, keyIdx, valIdx)
					if len(bufs[pi]) >= 64<<10 {
						if err := side.writePairs(pi, bufs[pi]); err != nil {
							return err
						}
						bufs[pi] = bufs[pi][:0]
					}
				}
			}
			return nil
		}
		return fn, func() {
			for pi, buf := range bufs {
				if len(buf) > 0 && werr == nil {
					werr = side.writePairs(pi, buf)
				}
			}
			mu.Lock()
			examples = append(examples, localEx...)
			if werr != nil && deferred == nil {
				deferred = werr
			}
			mu.Unlock()
		}
	})
	if err == nil {
		err = deferred
	}
	return examples, err
}

// row spill format, little-endian:
//
//	u64 kh, u16 keyLen, key, then per value: u8 tag (0 null, 1 scalar, 2 str),
//	scalar: u64 payload (int bits or float bits by column mode),
//	str: u32 len then bytes
func encodeRow(buf []byte, kh uint64, key string, b *source.Batch, r int, keyIdx, valIdx []int) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], kh)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint16(tmp[:2], uint16(len(key)))
	buf = append(buf, tmp[:2]...)
	buf = append(buf, key...)
	for _, ci := range append(append([]int{}, keyIdx...), valIdx...) {
		v := b.Cols[ci].Value(r)
		switch {
		case v.Null:
			buf = append(buf, 0)
		case v.Type == source.TypeString || v.Type == source.TypeBytes:
			buf = append(buf, 2)
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len(v.Str)))
			buf = append(buf, tmp[:4]...)
			buf = append(buf, v.Str...)
		case v.Type == source.TypeFloat64:
			buf = append(buf, 1, byte(source.TypeFloat64))
			binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v.Float))
			buf = append(buf, tmp[:]...)
		default:
			buf = append(buf, 1, byte(v.Type))
			binary.LittleEndian.PutUint64(tmp[:], uint64(v.Int))
			buf = append(buf, tmp[:]...)
		}
	}
	return buf
}

// readRowSpill parses one partition file of spilled rows.
func readRowSpill(f *os.File, nkeys, ncols int) ([]spilledRow, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	data := make([]byte, st.Size())
	if _, err := f.ReadAt(data, 0); err != nil && st.Size() > 0 {
		return nil, err
	}
	var rows []spilledRow
	pos := 0
	for pos < len(data) {
		if pos+10 > len(data) {
			return nil, fmt.Errorf("truncated row spill")
		}
		sr := spilledRow{kh: binary.LittleEndian.Uint64(data[pos:])}
		pos += 8
		kl := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+kl > len(data) {
			return nil, fmt.Errorf("truncated row spill")
		}
		sr.key = string(data[pos : pos+kl])
		pos += kl
		all := make([]source.Value, nkeys+ncols)
		sr.keyVals = all[:nkeys]
		sr.vals = all[nkeys:]
		for i := 0; i < nkeys+ncols; i++ {
			if pos >= len(data) {
				return nil, fmt.Errorf("truncated row spill")
			}
			tag := data[pos]
			pos++
			switch tag {
			case 0:
				all[i] = source.Value{Null: true}
			case 1:
				if pos+9 > len(data) {
					return nil, fmt.Errorf("truncated row spill")
				}
				typ := source.Type(data[pos])
				pos++
				bits := binary.LittleEndian.Uint64(data[pos:])
				pos += 8
				v := source.Value{Type: typ}
				if typ == source.TypeFloat64 {
					v.Float = math.Float64frombits(bits)
				} else {
					v.Int = int64(bits)
				}
				all[i] = v
			case 2:
				if pos+4 > len(data) {
					return nil, fmt.Errorf("truncated row spill")
				}
				ln := int(binary.LittleEndian.Uint32(data[pos:]))
				pos += 4
				if pos+ln > len(data) {
					return nil, fmt.Errorf("row spill string overrun")
				}
				all[i] = source.Value{Type: source.TypeString, Str: string(data[pos : pos+ln])}
				pos += ln
			default:
				return nil, fmt.Errorf("bad row spill tag %d", tag)
			}
		}
		rows = append(rows, sr)
	}
	return rows, nil
}

// joinPartition builds the left table for one partition (reusing the
// worker's table allocation) and probes it with the right side's pairs.
// partResult accumulates one partition join's outcome.
type partResult struct {
	added, removed, changed, unchanged int64
	dupsLeft, dupsRight                int64
	changedKh                          []uint64
	removedKh                          []uint64
	addedKh                            []uint64
	err                                error
}

func joinPartition(pi int, t *keyTable, pairBuf *[]byte, lSpill, rSpill *spillSide, pr *partResult, opts *Options) {
	lp, err := lSpill.readPartitionInto(pi, pairBuf)
	if err != nil {
		pr.err = err
		return
	}
	warnDup := opts.OnDup == "warn"
	t.reuse(len(lp))
	for _, pair := range lp {
		if !t.insert(pair.kh, pair.rh) {
			if warnDup {
				pr.dupsLeft++
				continue
			}
			pr.err = errDuplicateKey{kh: pair.kh}
			return
		}
	}
	t.seal()
	err = rSpill.forEachPairs(pi, func(pairs []hashPair) {
		for _, pair := range pairs {
			rh, slot, found := t.probe(pair.kh)
			switch {
			case !found:
				pr.added++
				if len(pr.addedKh) < opts.Limit || opts.Sink != nil {
					pr.addedKh = append(pr.addedKh, pair.kh)
				}
			case rh == pair.rh:
				if t.markMatched(slot) && warnDup {
					pr.dupsRight++
					continue
				}
				pr.unchanged++
			default:
				if t.markMatched(slot) && warnDup {
					pr.dupsRight++
					continue
				}
				pr.changed++
				pr.changedKh = append(pr.changedKh, pair.kh)
			}
		}
	})
	if err != nil {
		pr.err = err
		return
	}
	for i, k := range t.keys {
		if k != 0 && !t.isMatched(uint64(i)) {
			pr.removed++
			if len(pr.removedKh) < opts.Limit || opts.Sink != nil {
				pr.removedKh = append(pr.removedKh, k)
			}
		}
	}
}
