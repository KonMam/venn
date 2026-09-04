package source

import "io"

// refillBlock prepares the next raw block for a parallel line-oriented scan
// (the CSV and NDJSON fast paths): it seeds buf with leftover (the partial
// last line carried over from the previous block) and reads up to blockSize
// further bytes.
//
// Pooled buffers are allocated with a small fixed slack over blockSize, but
// the carried line can approach a full blockSize (its only hard limit is
// "must contain a newline within one further read"), so the buffer is grown
// here whenever the leftover exceeds the slack. The grown buffer flows back
// into the pool through the normal worker recycle path.
//
// Returns the filled block, the index where the newly read bytes begin, and
// whether the underlying reader is exhausted.
func refillBlock(r io.Reader, buf, leftover []byte, blockSize int) (block []byte, base int, eof bool, err error) {
	block = append(buf, leftover...)
	base = len(block)
	if cap(block)-base < blockSize {
		grown := make([]byte, base, base+blockSize+4096)
		copy(grown, block)
		block = grown
	}
	block = block[:base+blockSize]
	m, rerr := io.ReadFull(r, block[base:])
	block = block[:base+m]
	if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
		return nil, 0, false, rerr
	}
	return block, base, rerr != nil, nil
}
