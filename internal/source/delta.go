package source

// Delta Lake table support (copy-on-write): a table directory is detected by
// _delta_log/; the live file set at a version is reconstructed from the last
// checkpoint at or before it plus the JSON commits after it.
//
//	tdiff /lake/orders#412 /lake/orders#450 --key id
//
// Limits (stated, not silent): tables using deletion vectors are refused;
// partition values compare as strings; parquet data files only.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// isDeltaTable reports whether dir (local or s3://) looks like a Delta
// table root.
func isDeltaTable(dir string) bool {
	if !isRemote(dir) {
		st, err := os.Stat(filepath.Join(dir, "_delta_log"))
		return err == nil && st.IsDir()
	}
	names, err := fsFor(dir).List(joinPath(dir, "_delta_log"))
	return err == nil && len(names) > 0
}

// deltaDV is a deletion vector descriptor (merge-on-read row removal).
type deltaDV struct {
	StorageType    string `json:"storageType" parquet:"storageType,optional"`
	PathOrInlineDv string `json:"pathOrInlineDv" parquet:"pathOrInlineDv,optional"`
	Offset         int64  `json:"offset" parquet:"offset,optional"`
	SizeInBytes    int64  `json:"sizeInBytes" parquet:"sizeInBytes,optional"`
	Cardinality    int64  `json:"cardinality" parquet:"cardinality,optional"`
}

type deltaAdd struct {
	Path            string            `json:"path" parquet:"path,optional"`
	PartitionValues map[string]string `json:"partitionValues" parquet:"partitionValues,optional"`
	Stats           string            `json:"stats" parquet:"stats,optional"`
	DeletionVector  *deltaDV          `json:"deletionVector" parquet:"deletionVector,optional"`
}

// numRecords pulls the row count out of an add action's stats JSON
// (0 when stats are absent or unparseable).
func (a *deltaAdd) numRecords() int64 {
	if a.Stats == "" {
		return 0
	}
	var s struct {
		NumRecords int64 `json:"numRecords"`
	}
	if json.Unmarshal([]byte(a.Stats), &s) != nil {
		return 0
	}
	return s.NumRecords
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

// deltaLive is one live data file with its partition values and row count.
type deltaLive struct {
	part map[string]string
	rows int64
	dv   *deltaDV // deletion vector; nil when none
}

// deltaState replays actions into the live file set.
type deltaState struct {
	live map[string]deltaLive // data path → file info
}

func (st *deltaState) apply(add *deltaAdd, remove *deltaRemove, table string) error {
	switch {
	case add != nil:
		part := add.PartitionValues
		if len(part) == 0 {
			part = nil
		}
		dv := add.DeletionVector
		if dv != nil && dv.StorageType == "" {
			dv = nil // checkpoint rows decode absent DVs as empty structs
		}
		st.live[add.Path] = deltaLive{part: part, rows: add.numRecords(), dv: dv}
	case remove != nil:
		delete(st.live, remove.Path)
	}
	return nil
}

// deltaLogFiles inventories the log directory.
func deltaLogFiles(fs tableFS, dir string) (commits map[int64]string, checkpoints map[int64][]string, last int64, err error) {
	logDir := joinPath(dir, "_delta_log")
	names, err := fs.List(logDir)
	if err != nil {
		return nil, nil, 0, err
	}
	commits = map[int64]string{}
	checkpoints = map[int64][]string{}
	last = -1
	for _, name := range names {
		switch {
		case strings.HasSuffix(name, ".json") && len(name) == 25:
			v, err := strconv.ParseInt(strings.TrimSuffix(name, ".json"), 10, 64)
			if err != nil {
				continue
			}
			commits[v] = joinPath(logDir, name)
			if v > last {
				last = v
			}
		case strings.Contains(name, ".checkpoint") && strings.HasSuffix(name, ".parquet"):
			v, err := strconv.ParseInt(name[:20], 10, 64)
			if err != nil {
				continue
			}
			checkpoints[v] = append(checkpoints[v], joinPath(logDir, name))
		}
	}
	if last < 0 {
		return nil, nil, 0, fmt.Errorf("%s: no delta commits found", dir)
	}
	return commits, checkpoints, last, nil
}

// readCheckpoint loads add/remove actions from checkpoint parquet part(s).
// Checkpoints are metadata-sized; reading them whole keeps one code path for
// local disk and object storage.
func readCheckpoint(fs tableFS, paths []string, st *deltaState, table string) error {
	sort.Strings(paths)
	for _, p := range paths {
		raw, err := fs.ReadFile(p)
		if err != nil {
			return err
		}
		pf, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		reader := parquet.NewGenericReader[checkpointRow](pf)
		rows := make([]checkpointRow, 256)
		for {
			n, err := reader.Read(rows)
			for i := 0; i < n; i++ {
				if aerr := st.apply(rows[i].Add, rows[i].Remove, table); aerr != nil {
					reader.Close()
					return aerr
				}
			}
			if err != nil {
				break
			}
		}
		reader.Close()
	}
	return nil
}

// OpenDelta opens one version of a Delta table as a dataset. version "" or
// "latest" = newest commit.
func OpenDelta(dir, version string, opts Options) (Source, error) {
	files, label, err := ListDeltaFiles(dir, version)
	if err != nil {
		return nil, err
	}
	return openTableFiles(label, files, opts)
}

// ListDeltaFiles resolves the live data files of one table version.
func ListDeltaFiles(dir, version string) ([]TableFile, string, error) {
	fs := fsFor(dir)
	commits, checkpoints, last, err := deltaLogFiles(fs, dir)
	if err != nil {
		return nil, "", err
	}
	want := last
	if version != "" && version != "latest" {
		v, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("bad delta version %q", version)
		}
		if v > last {
			return nil, "", fmt.Errorf("%s: version %d not found (latest is %d)", dir, v, last)
		}
		want = v
	}

	// start from the newest checkpoint at or before the wanted version
	st := &deltaState{live: map[string]deltaLive{}}
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
		if err := readCheckpoint(fs, checkpoints[from-1], st, dir); err != nil {
			return nil, "", err
		}
	}
	for v := from; v <= want; v++ {
		path, ok := commits[v]
		if !ok {
			return nil, "", fmt.Errorf("%s: commit %020d.json missing (log truncated?)", dir, v)
		}
		raw, err := fs.ReadFile(path)
		if err != nil {
			return nil, "", err
		}
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(make([]byte, 1<<20), 1<<26)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var act deltaAction
			if err := json.Unmarshal([]byte(line), &act); err != nil {
				return nil, "", fmt.Errorf("%s: %w", path, err)
			}
			if err := st.apply(act.Add, act.Remove, dir); err != nil {
				return nil, "", err
			}
		}
		if err := sc.Err(); err != nil {
			return nil, "", err
		}
	}

	var files []TableFile
	var keys []string
	for p := range st.live {
		keys = append(keys, p)
	}
	sort.Strings(keys)
	for _, p := range keys {
		full := p
		if !filepath.IsAbs(p) && !isRemote(p) {
			full = joinPath(dir, p)
		}
		if !strings.HasSuffix(strings.ToLower(full), ".parquet") {
			return nil, "", fmt.Errorf("%s: data file %q is not parquet (parquet-only for Delta)", dir, p)
		}
		lv := st.live[p]
		part := lv.part
		// treat delta's explicit null partition marker as empty
		for k, v := range part {
			if v == "__HIVE_DEFAULT_PARTITION__" {
				part[k] = ""
			}
		}
		if len(part) == 0 {
			part = nil
		}
		tf := TableFile{Path: full, Partition: part, Rows: lv.rows}
		if lv.dv != nil {
			bm, err := loadDeltaDV(fs, dir, lv.dv)
			if err != nil {
				return nil, "", fmt.Errorf("%s: deletion vector for %s: %w", dir, p, err)
			}
			tf.deletes = &fileDeletes{
				pos: bm,
				key: fmt.Sprintf("dv:%s@%d", lv.dv.PathOrInlineDv, lv.dv.Offset),
			}
		}
		files = append(files, tf)
	}
	return files, fmt.Sprintf("%s#%d", dir, want), nil
}
