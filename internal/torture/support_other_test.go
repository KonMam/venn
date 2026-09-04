//go:build !unix

package torture

import (
	"os"
	"testing"
	"time"
)

func peakRSSMB(*os.ProcessState) int64 { return 0 }

func timeAfter(_ *testing.T) <-chan time.Time { return time.After(30 * time.Second) }
