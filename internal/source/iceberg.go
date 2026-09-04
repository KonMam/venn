package source

// Apache Iceberg table support (v1/v2, copy-on-write): a table directory is
// detected by metadata/*.metadata.json; the current (or #<id>-selected)
// snapshot's manifest list and manifests resolve to the live data files,
// which open as a multi-file dataset with the manifest's partition values as
// virtual columns.
//
//	venn /warehouse/db/orders#8412 /warehouse/db/orders#8500 --key id
//
// Limits (stated, not silent): merge-on-read tables carrying delete files
// are refused; partition values compare as strings; parquet data files only.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2/ocf"
)

// isIcebergTable reports whether dir looks like an Iceberg table root.
func isIcebergTable(dir string) bool {
	matches, _ := filepath.Glob(filepath.Join(dir, "metadata", "*.metadata.json"))
	return len(matches) > 0
}

type icebergMetadata struct {
	FormatVersion     int    `json:"format-version"`
	Location          string `json:"location"`
	CurrentSnapshotID int64  `json:"current-snapshot-id"`
	Snapshots         []struct {
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

var metadataVersionRe = regexp.MustCompile(`^(?:v)?(\d+)`)

// latestMetadataFile picks the newest metadata JSON (version-hint first,
// then the highest numeric prefix: both vN.metadata.json and
// NNNNN-uuid.metadata.json layouts).
func latestMetadataFile(dir string) (string, error) {
	metaDir := filepath.Join(dir, "metadata")
	if hint, err := os.ReadFile(filepath.Join(metaDir, "version-hint.text")); err == nil {
		v := strings.TrimSpace(string(hint))
		p := filepath.Join(metaDir, "v"+v+".metadata.json")
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	matches, _ := filepath.Glob(filepath.Join(metaDir, "*.metadata.json"))
	if len(matches) == 0 {
		return "", fmt.Errorf("%s: no metadata json found", dir)
	}
	sort.Slice(matches, func(i, j int) bool {
		ni := metadataVersionRe.FindStringSubmatch(filepath.Base(matches[i]))
		nj := metadataVersionRe.FindStringSubmatch(filepath.Base(matches[j]))
		if ni != nil && nj != nil {
			a, _ := strconv.Atoi(ni[1])
			b, _ := strconv.Atoi(nj[1])
			return a < b
		}
		return matches[i] < matches[j]
	})
	return matches[len(matches)-1], nil
}

// resolveIcebergPath turns manifest-recorded paths (absolute, file://,
// s3://, or table-relative) into openable paths. Recorded locations are
// rebased onto the actual table directory when they share the table's
// recorded location prefix — tables get moved/copied, and the manifests
// keep their original absolute paths.
func resolveIcebergPath(tableRoot, tableLocation, p string) string {
	orig := p
	p = strings.TrimPrefix(p, "file://")
	loc := strings.TrimPrefix(tableLocation, "file://")
	if loc != "" {
		if rel, ok := strings.CutPrefix(p, strings.TrimSuffix(loc, "/")+"/"); ok {
			return filepath.Join(tableRoot, rel)
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
		return filepath.Join(tableRoot, p)
	}
}

// avroRecords decodes every record of an Avro object container file.
func avroRecords(path string) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec, err := ocf.NewDecoder(f)
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

func avroInt(v any) int64 {
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

// OpenIceberg opens one snapshot of an Iceberg table as a dataset.
// snapshot "" or "latest" = current snapshot.
func OpenIceberg(dir, snapshot string, opts Options) (Source, error) {
	metaPath, err := latestMetadataFile(dir)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}
	var meta icebergMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("%s: %w", metaPath, err)
	}
	wantID := meta.CurrentSnapshotID
	if snapshot != "" && snapshot != "latest" {
		id, err := strconv.ParseInt(snapshot, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad snapshot id %q", snapshot)
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
		return nil, fmt.Errorf("%s: snapshot %d not found (known: %s)", dir, wantID, strings.Join(known, ", "))
	}
	if manifestList == "" {
		return nil, fmt.Errorf("%s: snapshot %d has no manifest list (unsupported v1 layout)", dir, wantID)
	}

	// manifest list → manifest files
	mlRecs, err := avroRecords(resolveIcebergPath(dir, meta.Location, manifestList))
	if err != nil {
		return nil, err
	}
	var paths []string
	var partitions []map[string]string
	for _, ml := range mlRecs {
		mPath, _ := avroField(ml, "manifest_path").(string)
		if mPath == "" {
			continue
		}
		if c := avroInt(avroField(ml, "content")); c == 1 {
			return nil, fmt.Errorf("%s: snapshot %d has delete manifests (merge-on-read tables are not supported yet)", dir, wantID)
		}
		entries, err := avroRecords(resolveIcebergPath(dir, meta.Location, mPath))
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if avroInt(e["status"]) == 2 { // DELETED
				continue
			}
			df, ok := avroField(e, "data_file").(map[string]any)
			if !ok {
				continue
			}
			if c := avroInt(df["content"]); c != 0 {
				return nil, fmt.Errorf("%s: delete files present (merge-on-read tables are not supported yet)", dir)
			}
			fp, _ := df["file_path"].(string)
			format, _ := df["file_format"].(string)
			if !strings.EqualFold(format, "parquet") {
				return nil, fmt.Errorf("%s: data file format %q not supported (parquet only)", dir, format)
			}
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
			paths = append(paths, resolveIcebergPath(dir, meta.Location, fp))
			partitions = append(partitions, part)
		}
	}
	label := fmt.Sprintf("%s#%d", dir, wantID)
	return openMulti(label, paths, partitions, opts, nil)
}
