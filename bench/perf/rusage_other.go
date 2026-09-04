//go:build !unix

package main

import "os"

func peakRSS(*os.ProcessState) int64 { return 0 }
