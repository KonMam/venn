//go:build unix

package main

import (
	"os"
	"runtime"
	"syscall"
)

// peakRSS extracts max resident set size in bytes. Darwin reports Maxrss in
// bytes, Linux (and the other unixes Go supports) in kilobytes.
func peakRSS(ps *os.ProcessState) int64 {
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil {
		return 0
	}
	rss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024
	}
	return rss
}
