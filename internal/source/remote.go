package source

// Remote inputs: http(s):// via range requests, s3:// via the AWS SDK.
// Parquet reads through an io.ReaderAt (range requests, one per window
// refill — the remote window is 8 MB so latency amortizes); CSV/NDJSON
// stream the body once.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// isRemote reports whether path names a remote object.
func isRemote(p string) bool {
	return strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") ||
		strings.HasPrefix(p, "s3://")
}

// remoteExt is the extension of the object's path component.
func remoteExt(p string) string {
	if u, err := url.Parse(p); err == nil {
		return strings.ToLower(path.Base(u.Path))
	}
	return strings.ToLower(path.Base(p))
}

// rangeReaderAt is an io.ReaderAt over a remote object addressed by ranges.
type rangeReaderAt interface {
	io.ReaderAt
	Size() int64
	Open() (io.ReadCloser, error) // full-body stream (text formats)
	SupportsRanges() bool
	Close() error
}

func openRangeReader(p string) (rangeReaderAt, error) {
	if strings.HasPrefix(p, "s3://") {
		return newS3Reader(p)
	}
	return newHTTPReader(p)
}

// listS3Prefix lists data objects under an s3:// prefix, with hive
// partition extraction relative to the prefix.
func listS3Prefix(p string) (paths []string, partitions []map[string]string, err error) {
	u, err := url.Parse(p)
	if err != nil {
		return nil, nil, err
	}
	if _, err := newS3Client(); err != nil {
		return nil, nil, err
	}
	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var token *string
	for {
		out, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", p, err)
		}
		for _, obj := range out.Contents {
			key := aws.ToString(obj.Key)
			base := path.Base(key)
			if !supportedDataExt(base) || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_") {
				continue
			}
			rel := strings.TrimPrefix(key, prefix)
			var part map[string]string
			for _, seg := range strings.Split(path.Dir(rel), "/") {
				if k, v, ok := strings.Cut(seg, "="); ok && k != "" {
					if part == nil {
						part = map[string]string{}
					}
					part[k] = v
				}
			}
			paths = append(paths, "s3://"+bucket+"/"+key)
			partitions = append(partitions, part)
		}
		if out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return paths, partitions, nil
}

// openRemote opens a remote object as a Source based on its extension.
func openRemote(p string, infer int) (Source, error) {
	if strings.HasPrefix(p, "s3://") && (strings.HasSuffix(p, "/") || !supportedDataExt(remoteExt(p))) {
		paths, partitions, err := listS3Prefix(p)
		if err != nil {
			return nil, err
		}
		return openMulti(p, paths, partitions, Options{InferRows: infer}, func(fp string) (Source, error) {
			return openRemote(fp, infer)
		})
	}
	if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
		if strings.HasSuffix(p, "/") {
			return nil, fmt.Errorf("%s: HTTP directories cannot be listed — pass explicit object URLs or use s3://", p)
		}
	}
	return openRemoteFile(p, infer)
}

func openRemoteFile(p string, infer int) (Source, error) {
	rr, err := openRangeReader(p)
	if err != nil {
		return nil, err
	}
	name := remoteExt(p)
	base := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".zst")
	switch ext := path.Ext(base); ext {
	case ".parquet":
		if !rr.SupportsRanges() {
			// no byte-range support: fetch once and read locally
			return openRemoteDownload(p, rr, name, infer)
		}
		return OpenParquetReaderAt(p, rr, rr.Size(), rr.Close)
	case ".csv", ".tsv", ".ndjson", ".jsonl":
		return openRemoteText(p, rr, name, infer)
	default:
		rr.Close()
		return nil, fmt.Errorf("%s: unsupported remote file extension %q", p, ext)
	}
}

// ---- HTTP ----

type httpReader struct {
	url     string
	size    int64
	client  *http.Client
	rangeOK bool
}

func newHTTPReader(u string) (*httpReader, error) {
	h := &httpReader{url: u, client: http.DefaultClient}
	resp, err := h.client.Head(u)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HEAD returned %s", u, resp.Status)
	}
	if resp.ContentLength < 0 {
		return nil, fmt.Errorf("%s: server does not report Content-Length", u)
	}
	h.rangeOK = strings.Contains(resp.Header.Get("Accept-Ranges"), "bytes")
	h.size = resp.ContentLength
	return h, nil
}

// SupportsRanges is advertised support; servers omitting the header but
// honoring ranges still work through ReadAt's 206 check (which errors and
// surfaces a clear message rather than mis-reading).
func (h *httpReader) SupportsRanges() bool { return h.rangeOK }

func (h *httpReader) Size() int64 { return h.size }
func (h *httpReader) Close() error {
	return nil
}

func (h *httpReader) ReadAt(p []byte, off int64) (int, error) {
	req, err := http.NewRequest(http.MethodGet, h.url, nil)
	if err != nil {
		return 0, err
	}
	end := off + int64(len(p)) - 1
	if end >= h.size {
		end = h.size - 1
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("%s: range request returned %s (server must support byte ranges)", h.url, resp.Status)
	}
	n, err := io.ReadFull(resp.Body, p[:end-off+1])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if n == len(p) {
		err = nil
	}
	return n, err
}

func (h *httpReader) Open() (io.ReadCloser, error) {
	resp, err := h.client.Get(h.url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("%s: GET returned %s", h.url, resp.Status)
	}
	return resp.Body, nil
}

// ---- S3 ----

type s3Reader struct {
	bucket, key string
	size        int64
	client      *s3.Client
}

var (
	s3ClientOnce sync.Once
	s3Client     *s3.Client
	s3ClientErr  error
)

func newS3Client() (*s3.Client, error) {
	s3ClientOnce.Do(func() {
		cfg, err := awsconfig.LoadDefaultConfig(context.Background())
		if err != nil {
			s3ClientErr = err
			return
		}
		s3Client = s3.NewFromConfig(cfg)
	})
	if s3ClientErr != nil {
		return nil, fmt.Errorf("aws config: %w", s3ClientErr)
	}
	return s3Client, nil
}

func newS3Reader(p string) (*s3Reader, error) {
	u, err := url.Parse(p)
	if err != nil {
		return nil, err
	}
	if _, err := newS3Client(); err != nil {
		return nil, err
	}
	r := &s3Reader{bucket: u.Host, key: strings.TrimPrefix(u.Path, "/"), client: s3Client}
	head, err := r.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.key),
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	r.size = aws.ToInt64(head.ContentLength)
	return r, nil
}

func (r *s3Reader) Size() int64          { return r.size }
func (r *s3Reader) SupportsRanges() bool { return true }
func (r *s3Reader) Close() error         { return nil }

func (r *s3Reader) ReadAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}
	out, err := r.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.key),
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
	})
	if err != nil {
		return 0, err
	}
	defer out.Body.Close()
	n, err := io.ReadFull(out.Body, p[:end-off+1])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	if n == len(p) {
		err = nil
	}
	return n, err
}

func (r *s3Reader) Open() (io.ReadCloser, error) {
	out, err := r.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(r.key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// ---- remote text (CSV/NDJSON, streamed once per scan) ----

// openRemoteText downloads to a temp file on open: text scans need two or
// three passes (inference + up to three engine passes), and re-downloading
// per pass would multiply transfer cost. The temp copy is deleted on Close.
func openRemoteText(p string, rr rangeReaderAt, name string, infer int) (Source, error) {
	return openRemoteDownload(p, rr, name, infer)
}

// openRemoteDownload fetches the whole object once and opens it locally.
func openRemoteDownload(p string, rr rangeReaderAt, name string, infer int) (Source, error) {
	body, err := rr.Open()
	if err != nil {
		return nil, err
	}
	defer body.Close()
	tmp, err := os.CreateTemp("", "tdiff-remote-*-"+sanitizeName(name))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(tmp, body); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("%s: download: %w", p, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return nil, err
	}
	inner, err := OpenWith(tmp.Name(), Options{InferRows: infer})
	if err != nil {
		os.Remove(tmp.Name())
		return nil, err
	}
	return &tempFileSource{Source: inner, tmpPath: tmp.Name(), label: p}, nil
}

func sanitizeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(s))
}

type tempFileSource struct {
	Source
	tmpPath string
	label   string
}

func (t *tempFileSource) Close() error {
	err := t.Source.Close()
	os.Remove(t.tmpPath)
	return err
}

// Optional-interface forwarding: without these the engine would silently
// fall back to the slow serial path for remote text sources.

func (t *tempFileSource) ScanBatches(n int, mk func() (BatchFunc, error)) error {
	if bs, ok := t.Source.(BatchScanner); ok {
		return bs.ScanBatches(n, mk)
	}
	return Scan(t.Source, n, mk)
}

func (t *tempFileSource) SizeBytes() int64 {
	if sz, ok := t.Source.(interface{ SizeBytes() int64 }); ok {
		return sz.SizeBytes()
	}
	return 0
}

func (t *tempFileSource) Warnings() []string {
	if w, ok := t.Source.(interface{ Warnings() []string }); ok {
		return w.Warnings()
	}
	return nil
}

func (t *tempFileSource) ForceStringColumn(name string) bool {
	if rt, ok := t.Source.(Retypeable); ok {
		return rt.ForceStringColumn(name)
	}
	return false
}
