package source

// tableFS abstracts the handful of file operations lake-table metadata
// reading needs, so Iceberg/Delta logs and manifests read the same way from
// local disk and object storage. Data files themselves open through the
// normal source machinery (which already handles s3://).

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type tableFS interface {
	ReadFile(p string) ([]byte, error)
	// List returns the base names of the entries directly under dir.
	List(dir string) ([]string, error)
}

// fsFor picks the filesystem implementation for a table root.
func fsFor(dir string) tableFS {
	if strings.HasPrefix(dir, "s3://") {
		return s3FS{}
	}
	return localFS{}
}

// joinPath joins elements under base, keeping URL bases intact (filepath.Join
// would collapse "s3://" to "s3:/").
func joinPath(base string, elem ...string) string {
	if isRemote(base) {
		return strings.TrimSuffix(base, "/") + "/" + path.Join(elem...)
	}
	return filepath.Join(append([]string{base}, elem...)...)
}

type localFS struct{}

func (localFS) ReadFile(p string) ([]byte, error) { return os.ReadFile(p) }

func (localFS) List(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names, nil
}

type s3FS struct{}

func s3Split(p string) (bucket, key string, err error) {
	u, err := url.Parse(p)
	if err != nil {
		return "", "", err
	}
	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

func (s3FS) ReadFile(p string) ([]byte, error) {
	if _, err := newS3Client(); err != nil {
		return nil, err
	}
	bucket, key, err := s3Split(p)
	if err != nil {
		return nil, err
	}
	out, err := s3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s3FS) List(dir string) ([]string, error) {
	if _, err := newS3Client(); err != nil {
		return nil, err
	}
	bucket, prefix, err := s3Split(dir)
	if err != nil {
		return nil, err
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var names []string
	var token *string
	for {
		out, err := s3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix),
			Delimiter: aws.String("/"), ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", dir, err)
		}
		for _, obj := range out.Contents {
			if name := path.Base(aws.ToString(obj.Key)); name != "" {
				names = append(names, name)
			}
		}
		if out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return names, nil
}
