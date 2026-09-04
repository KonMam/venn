//go:build unix

package torture

import (
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func peakRSSMB(ps *os.ProcessState) int64 {
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil {
		return 0
	}
	rss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024
	}
	return rss / (1 << 20)
}

func timeAfter(_ *testing.T) <-chan time.Time { return time.After(30 * time.Second) }
