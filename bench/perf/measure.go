package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"
)

// runMetrics is one subprocess execution's measurement. CPU (user+sys) is the
// primary regression metric: it is far less sensitive to scheduler and disk
// noise than wall time, which matters on shared CI runners.
type runMetrics struct {
	Wall   time.Duration
	CPU    time.Duration
	RSS    int64 // peak resident set, bytes; 0 when the platform can't report it
	Exit   int
	Stdout string
	Stderr string
}

func runOnce(bin string, args []string, capture bool, timeout time.Duration) (runMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	var out, errb bytes.Buffer
	if capture {
		cmd.Stdout = &out
	} else {
		cmd.Stdout = io.Discard
	}
	cmd.Stderr = &errb
	start := time.Now()
	runErr := cmd.Run()
	m := runMetrics{Wall: time.Since(start)}
	if ctx.Err() != nil {
		return m, fmt.Errorf("timeout after %s", timeout)
	}
	ps := cmd.ProcessState
	if ps == nil {
		return m, runErr
	}
	m.CPU = ps.UserTime() + ps.SystemTime()
	m.RSS = peakRSS(ps)
	m.Exit = ps.ExitCode()
	m.Stdout = out.String()
	m.Stderr = errb.String()
	if m.Exit < 0 {
		return m, fmt.Errorf("process killed: %v", runErr)
	}
	return m, nil
}

// agg folds repeated runs: best (min) wall and CPU — the least-disturbed run —
// and worst (max) RSS, the conservative bound.
type agg struct {
	n    int
	wall time.Duration
	cpu  time.Duration
	rss  int64
}

func (a *agg) add(m runMetrics) {
	if a.n == 0 || m.Wall < a.wall {
		a.wall = m.Wall
	}
	if a.n == 0 || m.CPU < a.cpu {
		a.cpu = m.CPU
	}
	if m.RSS > a.rss {
		a.rss = m.RSS
	}
	a.n++
}
