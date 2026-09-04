package source

// Delta Lake table support (copy-on-write): a table directory is detected by
// _delta_log/; the live file set at a version is reconstructed from the last
// checkpoint at or before it plus the JSON commits after it.
//
//	venn /lake/orders#412 /lake/orders#450 --key id
//
// Limits (stated, not silent): tables using deletion vectors are refused;
// partition values compare as strings; parquet data files only.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// isDeltaTable reports whether dir looks like a Delta table root.
func isDeltaTable(dir string) bool {
	st, err := os.Stat(filepath.Join(dir, "_delta_log"))
	return err == nil && st.IsDir()
}

type deltaAdd struct {
	Path             string            `json:"path" parquet:"path,optional"`
	PartitionValues  map[string]string `json:"partitionValues" parquet:"partitionValues,optional"`
	DeletionVector   *struct{}         `json:"deletionVector" parquet:"-"`
	DeletionVectorPQ *struct {
		StorageType string `parquet:"storageType,optional"`
	} `json:"-" parquet:"deletionVector,optional"`
}

type deltaRemove struct {
	Path string `json:"path" parquet:"path,optional"`
}

type deltaAction struct {
	Add    *deltaAdd    `json:"add"`
	Remove *deltaRemove `json:"remove"`
}

// checkpointRow mirrors the columns of a Delta checkpoint parquet we need.
type checkpointRow struct {
	Add    *deltaAdd    `parquet:"add,optional"`
	Remove *deltaRemove `parquet:"remove,optional"`
}

// deltaState replays actions into the live file set.
type deltaState struct {
	live map[string]map[string]string // data path → partition values
}

func (st *deltaState) apply(add *deltaAdd, remove *deltaRemove, table string) error {
	switch {
	case add != nil:
		if add.DeletionVector != nil || (add.DeletionVectorPQ != nil && add.DeletionVectorPQ.StorageType != "") {
			return fmt.Errorf("%s: table uses deletion vectors (merge-on-read); not supported yet", table)
		}
		part := add.PartitionValues
		if len(part) == 0 {
			part = nil
		}
		st.live[add.Path] = part
	case remove != nil:
		delete(st.live, remove.Path)
	}
	return nil
}

// deltaLogFiles inventories the log directory.
func deltaLogFiles(dir string) (commits map[int64]string, checkpoints map[int64][]string, last int64, err error) {
	logDir := filepath.Join(dir, "_delta_log")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return nil, nil, 0, err
	}
	commits = map[int64]string{}
	checkpoints = map[int64][]string{}
	last = -1
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".json") && len(name) == 25:
			v, err := strconv.ParseInt(strings.TrimSuffix(name, ".json"), 10, 64)
			if err != nil {
				continue
			}
			commits[v] = filepath.Join(logDir, name)
			if v > last {
				last = v
			}
		case strings.Contains(name, ".checkpoint") && strings.HasSuffix(name, ".parquet"):
			v, err := strconv.ParseInt(name[:20], 10, 64)
			if err != nil {
				continue
			}
			checkpoints[v] = append(checkpoints[v], filepath.Join(logDir, name))
		}
	}
	if last < 0 {
		return nil, nil, 0, fmt.Errorf("%s: no delta commits found", dir)
	}
	return commits, checkpoints, last, nil
}

// readCheckpoint loads add/remove actions from checkpoint parquet part(s).
func readCheckpoint(paths []string, st *deltaState, table string) error {
	sort.Strings(paths)
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		stat, _ := f.Stat()
		pf, err := parquet.OpenFile(f, stat.Size())
		if err != nil {
			f.Close()
			return fmt.Errorf("%s: %w", p, err)
		}
		reader := parquet.NewGenericReader[checkpointRow](pf)
		rows := make([]checkpointRow, 256)
		for {
			n, err := reader.Read(rows)
			for i := 0; i < n; i++ {
				if aerr := st.apply(rows[i].Add, rows[i].Remove, table); aerr != nil {
					reader.Close()
					f.Close()
					return aerr
				}
			}
			if err != nil {
				break
			}
		}
		reader.Close()
		f.Close()
	}
	return nil
}

// OpenDelta opens one version of a Delta table as a dataset. version "" or
// "latest" = newest commit.
func OpenDelta(dir, version string, opts Options) (Source, error) {
	commits, checkpoints, last, err := deltaLogFiles(dir)
	if err != nil {
		return nil, err
	}
	want := last
	if version != "" && version != "latest" {
		v, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad delta version %q", version)
		}
		if v > last {
			return nil, fmt.Errorf("%s: version %d not found (latest is %d)", dir, v, last)
		}
		want = v
	}

	// start from the newest checkpoint at or before the wanted version
	st := &deltaState{live: map[string]map[string]string{}}
	from := int64(0)
	var cpVersions []int64
	for v := range checkpoints {
		cpVersions = append(cpVersions, v)
	}
	sort.Slice(cpVersions, func(i, j int) bool { return cpVersions[i] < cpVersions[j] })
	for _, v := range cpVersions {
		if v <= want {
			from = v + 1
		}
	}
	if from > 0 {
		if err := readCheckpoint(checkpoints[from-1], st, dir); err != nil {
			return nil, err
		}
	}
	for v := from; v <= want; v++ {
		path, ok := commits[v]
		if !ok {
			return nil, fmt.Errorf("%s: commit %020d.json missing (log truncated?)", dir, v)
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<26)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var act deltaAction
			if err := json.Unmarshal([]byte(line), &act); err != nil {
				f.Close()
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			if err := st.apply(act.Add, act.Remove, dir); err != nil {
				f.Close()
				return nil, err
			}
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}

	var paths []string
	var partitions []map[string]string
	var keys []string
	for p := range st.live {
		keys = append(keys, p)
	}
	sort.Strings(keys)
	for _, p := range keys {
		full := p
		if !filepath.IsAbs(p) && !isRemote(p) {
			full = filepath.Join(dir, p)
		}
		if !strings.HasSuffix(strings.ToLower(full), ".parquet") {
			return nil, fmt.Errorf("%s: data file %q is not parquet (parquet-only for Delta)", dir, p)
		}
		paths = append(paths, full)
		part := st.live[p]
		// treat delta's explicit null partition marker as empty
		for k, v := range part {
			if v == "__HIVE_DEFAULT_PARTITION__" {
				part[k] = ""
			}
		}
		if len(part) == 0 {
			part = nil
		}
		partitions = append(partitions, part)
	}
	label := fmt.Sprintf("%s#%d", dir, want)
	return openMulti(label, paths, partitions, opts, nil)
}
