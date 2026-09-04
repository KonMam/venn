package source

// Text decompression. The pure-Go gzip inflater tops out around 225 MB/s on
// this class of hardware while the system gzip does ~530 MB/s, so when a
// system decompressor exists it wins by 2.4× — the subprocess pipes into the
// same scan path. The Go readers remain as the portable fallback (and can be
// forced with TDIFF_NO_EXEC_DECOMPRESS=1 for tests). zstd stays in-process:
// klauspost's decoder is within ~20% of libzstd and avoids the subprocess.

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
)

var noExecDecompress = os.Getenv("TDIFF_NO_EXEC_DECOMPRESS") != ""

// eofTracking notes whether the consumer read the stream to completion —
// only then does subprocess exit status mean anything.
type eofTracking struct {
	r      io.Reader
	sawEOF bool
}

func (t *eofTracking) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err == io.EOF {
		t.sawEOF = true
	}
	return n, err
}

var gzipPathOnce sync.Once
var gzipPath string

func systemGzip() string {
	gzipPathOnce.Do(func() {
		if noExecDecompress {
			return
		}
		for _, name := range []string{"gzip", "zcat"} {
			if p, err := exec.LookPath(name); err == nil {
				gzipPath = p
				return
			}
		}
	})
	return gzipPath
}

// openDecompressed opens path wrapped in the decompressor for compress
// ("gz", "zst" or ""). The closer propagates subprocess exit status, so a
// corrupt archive fails the scan instead of truncating it silently.
func openDecompressed(path, compress string) (io.Reader, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	switch compress {
	case "gz":
		if gz := systemGzip(); gz != "" {
			cmd := exec.Command(gz, "-dc")
			cmd.Stdin = f
			cmd.Stderr = nil
			out, err := cmd.StdoutPipe()
			if err == nil {
				if err := cmd.Start(); err == nil {
					tr := &eofTracking{r: out}
					return tr, func() error {
						f.Close()
						if !tr.sawEOF {
							// consumer stopped early (schema sampling, an
							// error): kill rather than drain gigabytes
							cmd.Process.Kill()
							cmd.Wait()
							return nil
						}
						if werr := cmd.Wait(); werr != nil {
							return fmt.Errorf("%s: gzip: %w", path, werr)
						}
						return nil
					}, nil
				}
			}
			// fall through to the in-process reader
		}
		zr, err := pgzip.NewReaderN(f, 1<<20, 8)
		if err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		return zr, func() error {
			if err := zr.Close(); err != nil {
				f.Close()
				return fmt.Errorf("%s: gzip: %w", path, err)
			}
			return f.Close()
		}, nil
	case "zst":
		zr, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(0))
		if err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		return zr, func() error { zr.Close(); return f.Close() }, nil
	default:
		return f, f.Close, nil
	}
}
