package source

import (
	"testing"
)

// Lake-metadata decoders also parse untrusted bytes (inline DVs in the log,
// .bin sidecars): same rule as fuzz_test.go — errors are fine, panics are not.

func FuzzZ85Decode(f *testing.F) {
	f.Add("HelloWorld")
	f.Add("00000")
	f.Add("#####")
	f.Add("abcd")
	f.Add("abcd~")
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = z85Decode(s)
	})
}

func FuzzParseDeltaDVData(f *testing.F) {
	f.Add(buildDVData(nil))
	f.Add(buildDVData([]dvPart{{key: 0, bits: []uint32{1, 5, 100}}}))
	f.Add(buildDVData([]dvPart{{key: 0, bits: []uint32{0, 2}}, {key: 1, bits: []uint32{3}}}))
	valid := buildDVData([]dvPart{{key: 7, bits: []uint32{9, 10, 11}}})
	f.Add(valid[:len(valid)-3])
	f.Add([]byte{})
	// found by fuzzing: a malformed run container that deserializes without
	// error but panicked on iteration before the Validate guard
	f.Add([]byte("\xd1\xd39d000000000000;0\x00\x0010000\x00\x00"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = parseDeltaDVData(data, 1<<20)
	})
}
