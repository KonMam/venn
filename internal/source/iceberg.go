package source

// Apache Iceberg table support (v1/v2, copy-on-write): a table directory is
// detected by metadata/*.metadata.json; the current (or #<id>-selected)
// snapshot's manifest list and manifests resolve to the live data files,
// which open as a multi-file dataset with the manifest's partition values as
// virtual columns.
//
//	venn /warehouse/db/orders#8412 /warehouse/db/orders#8500 --key id
//
// Merge-on-read delete files are applied per the spec's sequence-number
// rules (position deletes at or before the data file's sequence, equality
// deletes strictly after; see deletes.go). Limits (stated, not silent):
// partition values compare as strings; parquet data files only.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/hamba/avro/v2/ocf"
)

// isIcebergTable reports whether dir (local or s3://) looks like an Iceberg
// table root.
func isIcebergTable(dir string) bool {
	names, err := fsFor(dir).List(joinPath(dir, "metadata"))
	if err != nil {
		return false
	}
	for _, n := range names {
		if strings.HasSuffix(n, ".metadata.json") {
			return true
		}
	}
	return false
}

type icebergSchemaField struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type icebergMetadata struct {
	FormatVersion     int    `json:"format-version"`
	Location          string `json:"location"`
	CurrentSnapshotID int64  `json:"current-snapshot-id"`
	CurrentSchemaID   int    `json:"current-schema-id"`
	Schemas           []struct {
		SchemaID int                  `json:"schema-id"`
		Fields   []icebergSchemaField `json:"fields"`
	} `json:"schemas"`
	Schema *struct {
		Fields []icebergSchemaField `json:"fields"`
	} `json:"schema"` // v1 layout
	Snapshots []struct {
		SnapshotID   int64  `json:"snapshot-id"`
		TimestampMS  int64  `json:"timestamp-ms"`
		ManifestList string `json:"manifest-list"`
		Manifests    []string
	} `json:"snapshots"`
	PartitionSpecs []struct {
		SpecID int `json:"spec-id"`
		Fields []struct {
			Name      string `json:"name"`
			Transform string `json:"transform"`
		} `json:"fields"`
	} `json:"partition-specs"`
}

// fieldNames maps schema field ids to column names (for equality_ids).
func (m *icebergMetadata) fieldNames() map[int]string {
	out := map[int]string{}
	for _, sch := range m.Schemas {
		if sch.SchemaID == m.CurrentSchemaID {
			for _, f := range sch.Fields {
				out[f.ID] = f.Name
			}
			return out
		}
	}
	if m.Schema != nil {
		for _, f := range m.Schema.Fields {
			out[f.ID] = f.Name
		}
	}
	return out
}

var metadataVersionRe = regexp.MustCompile(`^(?:v)?(\d+)`)

// latestMetadataFile picks the newest metadata JSON (version-hint first,
// then the highest numeric prefix: both vN.metadata.json and
// NNNNN-uuid.metadata.json layouts).
func latestMetadataFile(fs tableFS, dir string) (string, error) {
	metaDir := joinPath(dir, "metadata")
	names, _ := fs.List(metaDir)
	var matches []string
	for _, n := range names {
		if strings.HasSuffix(n, ".metadata.json") {
			matches = append(matches, n)
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("%s: no metadata json found", dir)
	}
	if hint, err := fs.ReadFile(joinPath(metaDir, "version-hint.text")); err == nil {
		want := "v" + strings.TrimSpace(string(hint)) + ".metadata.json"
		for _, n := range matches {
			if n == want {
				return joinPath(metaDir, n), nil
			}
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		ni := metadataVersionRe.FindStringSubmatch(matches[i])
		nj := metadataVersionRe.FindStringSubmatch(matches[j])
		if ni != nil && nj != nil {
			a, _ := strconv.Atoi(ni[1])
			b, _ := strconv.Atoi(nj[1])
			return a < b
		}
		return matches[i] < matches[j]
	})
	return joinPath(metaDir, matches[len(matches)-1]), nil
}

// resolveIcebergPath turns manifest-recorded paths (absolute, file://,
// s3://, or table-relative) into openable paths. Recorded locations are
// rebased onto the actual table directory when they share the table's
// recorded location prefix. Tables get moved or copied, and the manifests
// keep their original absolute paths.
func resolveIcebergPath(tableRoot, tableLocation, p string) string {
	orig := p
	p = strings.TrimPrefix(p, "file://")
	loc := strings.TrimPrefix(tableLocation, "file://")
	if loc != "" {
		if rel, ok := strings.CutPrefix(p, strings.TrimSuffix(loc, "/")+"/"); ok {
			return joinPath(tableRoot, rel)
		}
	}
	switch {
	case strings.HasPrefix(orig, "s3a://"):
		return "s3://" + strings.TrimPrefix(orig, "s3a://")
	case isRemote(orig):
		return orig
	case filepath.IsAbs(p):
		return p
	default:
		return joinPath(tableRoot, p)
	}
}

// avroRecords decodes every record of an Avro object container file.
func avroRecords(fs tableFS, path string) ([]map[string]any, error) {
	raw, err := fs.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec, err := ocf.NewDecoder(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var out []map[string]any
	for dec.HasNext() {
		var rec map[string]any
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, rec)
	}
	return out, dec.Error()
}

// avroField digs a field out of a possibly-nested avro record.
func avroField(rec map[string]any, names ...string) any {
	var cur any = rec
	for _, n := range names {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[n]
		// unions decode as {"type-name": value}
		if u, ok := cur.(map[string]any); ok && len(u) == 1 {
			for k, v := range u {
				if strings.Contains(k, ".") || k == "record" {
					cur = v
				}
			}
		}
	}
	return cur
}

// avroUnwrap strips a single-entry union wrapper ({"long": v} etc.).
func avroUnwrap(v any) any {
	if u, ok := v.(map[string]any); ok && len(u) == 1 {
		for k, inner := range u {
			switch k {
			case "long", "int", "string", "boolean", "double", "float", "bytes", "array":
				return inner
			}
		}
	}
	return v
}

// avroIntOK is avroInt with presence reporting (unions decode null as nil).
func avroIntOK(v any) (int64, bool) {
	v = avroUnwrap(v)
	if v == nil {
		return 0, false
	}
	switch v.(type) {
	case int64, int32, int, float64:
		return avroInt(v), true
	}
	return 0, false
}

// avroIntList decodes an avro int array (possibly union-wrapped).
func avroIntList(v any) []int {
	if u, ok := v.(map[string]any); ok && len(u) == 1 {
		for _, inner := range u {
			v = inner
		}
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]int, 0, len(arr))
	for _, e := range arr {
		out = append(out, int(avroInt(e)))
	}
	return out
}

func avroInt(v any) int64 {
	v = avroUnwrap(v)
	switch t := v.(type) {
	case int64:
		return t
	case int32:
		return int64(t)
	case int:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

// TableFile is one live data file of a table snapshot.
type TableFile struct {
	Path      string
	Partition map[string]string
	Rows      int64 // physical rows from the metadata (0 when unknown)

	// merge-on-read deletes applied to this file's rows at scan time
	deletes *fileDeletes
}

// OpenIceberg opens one snapshot of an Iceberg table as a dataset.
// snapshot "" or "latest" = current snapshot.
func OpenIceberg(dir, snapshot string, opts Options) (Source, error) {
	files, label, err := ListIcebergFiles(dir, snapshot)
	if err != nil {
		return nil, err
	}
	return openTableFiles(label, files, opts)
}

// openTableFiles opens a resolved file list as a dataset, attaching
// merge-on-read deletes to the per-file parquet sources.
func openTableFiles(label string, files []TableFile, opts Options) (Source, error) {
	paths := make([]string, len(files))
	partitions := make([]map[string]string, len(files))
	var deletes map[string]*fileDeletes
	for i, f := range files {
		paths[i] = f.Path
		partitions[i] = f.Partition
		if !f.deletes.empty() {
			if deletes == nil {
				deletes = map[string]*fileDeletes{}
			}
			deletes[f.Path] = f.deletes
		}
	}
	var open func(string) (Source, error)
	if deletes != nil {
		open = func(p string) (Source, error) {
			src, err := OpenWith(p, opts)
			if err != nil {
				return nil, err
			}
			d := deletes[p]
			if d == nil {
				return src, nil
			}
			pq, ok := src.(*parquetSource)
			if !ok {
				src.Close()
				return nil, fmt.Errorf("%s: merge-on-read deletes need direct parquet access (got %T)", p, src)
			}
			if err := pq.setDeletes(d); err != nil {
				src.Close()
				return nil, err
			}
			return src, nil
		}
	}
	return openMulti(label, paths, partitions, opts, open)
}

// ListIcebergFiles resolves the live data files of one snapshot.
func ListIcebergFiles(dir, snapshot string) ([]TableFile, string, error) {
	fs := fsFor(dir)
	metaPath, err := latestMetadataFile(fs, dir)
	if err != nil {
		return nil, "", err
	}
	raw, err := fs.ReadFile(metaPath)
	if err != nil {
		return nil, "", err
	}
	var meta icebergMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, "", fmt.Errorf("%s: %w", metaPath, err)
	}
	wantID := meta.CurrentSnapshotID
	if snapshot != "" && snapshot != "latest" {
		id, err := strconv.ParseInt(snapshot, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("bad snapshot id %q", snapshot)
		}
		wantID = id
	}
	var manifestList string
	found := false
	var known []string
	for _, s := range meta.Snapshots {
		known = append(known, strconv.FormatInt(s.SnapshotID, 10))
		if s.SnapshotID == wantID {
			manifestList = s.ManifestList
			found = true
		}
	}
	if !found {
		return nil, "", fmt.Errorf("%s: snapshot %d not found (known: %s)", dir, wantID, strings.Join(known, ", "))
	}
	if manifestList == "" {
		return nil, "", fmt.Errorf("%s: snapshot %d has no manifest list (unsupported v1 layout)", dir, wantID)
	}

	// manifest list → manifest files
	mlRecs, err := avroRecords(fs, resolveIcebergPath(dir, meta.Location, manifestList))
	if err != nil {
		return nil, "", err
	}
	type delEntry struct {
		path  string
		eq    bool // equality delete (else position delete)
		seq   int64
		eqIDs []int
	}
	type dataEntry struct {
		file TableFile
		seq  int64
	}
	var datas []dataEntry
	var dels []delEntry
	for _, ml := range mlRecs {
		mPath, _ := avroField(ml, "manifest_path").(string)
		if mPath == "" {
			continue
		}
		mSeq := avroInt(avroField(ml, "sequence_number"))
		entries, err := avroRecords(fs, resolveIcebergPath(dir, meta.Location, mPath))
		if err != nil {
			return nil, "", err
		}
		for _, e := range entries {
			if avroInt(e["status"]) == 2 { // DELETED
				continue
			}
			df, ok := avroField(e, "data_file").(map[string]any)
			if !ok {
				continue
			}
			// v2 sequence inheritance: ADDED entries with null sequence
			// number take the manifest's
			seq, ok2 := avroIntOK(e["sequence_number"])
			if !ok2 {
				seq = mSeq
			}
			fp, _ := df["file_path"].(string)
			format, _ := df["file_format"].(string)
			if !strings.EqualFold(format, "parquet") {
				return nil, "", fmt.Errorf("%s: data file format %q not supported (parquet only)", dir, format)
			}
			resolved := resolveIcebergPath(dir, meta.Location, fp)
			switch avroInt(df["content"]) {
			case 0: // data
				part := map[string]string{}
				if pv, ok := avroField(df, "partition").(map[string]any); ok {
					for k, v := range pv {
						if v == nil {
							part[k] = ""
							continue
						}
						if u, ok := v.(map[string]any); ok && len(u) == 1 {
							for _, inner := range u {
								v = inner
							}
						}
						part[k] = fmt.Sprint(v)
					}
				}
				if len(part) == 0 {
					part = nil
				}
				datas = append(datas, dataEntry{
					file: TableFile{Path: resolved, Partition: part, Rows: avroInt(df["record_count"])},
					seq:  seq,
				})
			case 1: // position deletes
				dels = append(dels, delEntry{path: resolved, seq: seq})
			case 2: // equality deletes
				dels = append(dels, delEntry{path: resolved, eq: true, seq: seq,
					eqIDs: avroIntList(avroField(df, "equality_ids"))})
			}
		}
	}
	label := fmt.Sprintf("%s#%d", dir, wantID)
	files := make([]TableFile, 0, len(datas))
	if len(dels) == 0 {
		for _, d := range datas {
			files = append(files, d.file)
		}
		return files, label, nil
	}

	// merge-on-read: load each delete file once, then attach to each data
	// file the deletes that apply to it (position: deleteSeq >= dataSeq;
	// equality: deleteSeq > dataSeq).
	sort.Slice(dels, func(i, j int) bool { return dels[i].path < dels[j].path })
	fieldNames := meta.fieldNames()
	resolve := func(p string) string { return resolveIcebergPath(dir, meta.Location, p) }
	posByDel := make([]map[string]*roaring64.Bitmap, len(dels))
	eqByDel := make([]*eqSet, len(dels))
	for i, d := range dels {
		if d.eq {
			cols := make([]string, len(d.eqIDs))
			for j, id := range d.eqIDs {
				name, ok := fieldNames[id]
				if !ok {
					return nil, "", fmt.Errorf("%s: equality delete references unknown field id %d", dir, id)
				}
				cols[j] = name
			}
			set, err := readEqualityDeletes(d.path, cols)
			if err != nil {
				return nil, "", err
			}
			eqByDel[i] = set
		} else {
			rows, err := readPositionDeletes(d.path, resolve)
			if err != nil {
				return nil, "", err
			}
			posByDel[i] = rows
		}
	}
	for _, d := range datas {
		var fd fileDeletes
		var keyParts []string
		for i, del := range dels {
			switch {
			case !del.eq && del.seq >= d.seq:
				bm := posByDel[i][d.file.Path]
				if bm == nil || bm.IsEmpty() {
					continue
				}
				if fd.pos == nil {
					fd.pos = roaring64.New()
				}
				fd.pos.Or(bm)
				keyParts = append(keyParts, fmt.Sprintf("p:%s@%d", del.path, del.seq))
			case del.eq && del.seq > d.seq:
				fd.eq = append(fd.eq, eqByDel[i])
				keyParts = append(keyParts, fmt.Sprintf("e:%s@%d", del.path, del.seq))
			}
		}
		if !fd.empty() {
			fd.key = strings.Join(keyParts, "|")
			d.file.deletes = &fd
		}
		files = append(files, d.file)
	}
	return files, label, nil
}
