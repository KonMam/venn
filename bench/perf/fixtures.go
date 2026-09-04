package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/KonMam/tdiff/internal/fixture"
)

// ensureDataset makes sure the fixture pair for k exists in cacheRoot with at
// least the given formats, generating it if missing or incomplete. Fixtures
// are pure functions of their key (seed is fixed), so a cache hit is always
// valid.
func ensureDataset(cacheRoot string, k datasetKey, formats []string) (string, *fixture.Manifest, error) {
	dir := filepath.Join(cacheRoot, k.dirName())
	manPath := filepath.Join(dir, "manifest.json")
	if complete(dir, manPath, formats) {
		var man fixture.Manifest
		b, err := os.ReadFile(manPath)
		if err == nil && json.Unmarshal(b, &man) == nil {
			return dir, &man, nil
		}
	}
	// Missing or partial: regenerate the whole pair (fixture.Generate writes
	// all requested formats in one pass over the data).
	if err := os.RemoveAll(dir); err != nil {
		return "", nil, err
	}
	d := densities[k.Density]
	fmt.Printf("[gen] %s (%s)\n", k.dirName(), joinStrings(formats))
	man, err := fixture.Generate(fixture.Config{
		Rows: k.Rows, Cols: k.Cols, Seed: 1,
		PctChanged: d.changed, PctAdded: d.added, PctRemoved: d.removed,
		Out: dir, Formats: formats, Variant: k.Shape,
	})
	if err != nil {
		return "", nil, fmt.Errorf("generate %s: %w", k.dirName(), err)
	}
	return dir, man, nil
}

func complete(dir, manPath string, formats []string) bool {
	if !fileExists(manPath) {
		return false
	}
	for _, f := range formats {
		if !fileExists(filepath.Join(dir, "left."+f)) || !fileExists(filepath.Join(dir, "right."+f)) {
			return false
		}
	}
	return true
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// datasetFormats computes, per dataset, the union of formats every selected
// case needs, so each dataset is generated exactly once per invocation.
func datasetFormats(cases []Case, tier tierSpec) map[datasetKey][]string {
	set := map[datasetKey]map[string]bool{}
	for _, c := range cases {
		k := c.dataset(tier)
		if set[k] == nil {
			set[k] = map[string]bool{}
		}
		for _, f := range c.formats() {
			set[k][f] = true
		}
	}
	out := map[datasetKey][]string{}
	for k, fs := range set {
		var list []string
		for f := range fs {
			list = append(list, f)
		}
		sort.Strings(list)
		out[k] = list
	}
	return out
}

func joinStrings(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}
