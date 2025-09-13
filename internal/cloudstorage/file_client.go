// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cloudstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// FileClientProvider creates clients that operate on the local filesystem.
// It is intended for tests that want to bypass real cloud providers.
type FileClientProvider struct {
	base string
}

// NewFileClientProvider returns a new provider rooted at base.
func NewFileClientProvider(base string) ClientProvider {
	return &FileClientProvider{base: base}
}

// NewClient returns a client that reads and writes files under the base path.
func (p *FileClientProvider) NewClient(ctx context.Context, profile storageprofile.StorageProfile) (Client, error) {
	// Bucket names become subdirectories under the base path.
	return &fileClient{base: p.base}, nil
}

type fileClient struct {
	base string
}

func (c *fileClient) path(bucket, key string) string {
	return filepath.Join(c.base, bucket, filepath.FromSlash(key))
}

// DownloadObject copies the requested object to a temp file and returns the filename.
func (c *fileClient) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	src := c.path(bucket, key)
	fi, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, true, nil
		}
		return "", 0, false, err
	}
	// Preserve the original filename to maintain file extensions for downstream type detection
	filename := filepath.Base(key)
	dst, err := os.CreateTemp(tmpdir, "*-"+filename)
	if err != nil {
		return "", 0, false, err
	}
	defer func() { _ = dst.Close() }()

	f, err := os.Open(src)
	if err != nil {
		return "", 0, false, err
	}
	defer func() { _ = f.Close() }()

	if _, err := io.Copy(dst, f); err != nil {
		return "", 0, false, err
	}
	return dst.Name(), fi.Size(), false, nil
}

// UploadObject copies a local file into the bucket/key location.
func (c *fileClient) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	dst := c.path(bucket, key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	src, err := os.Open(sourceFilename)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	_, err = io.Copy(out, src)
	return err
}

// DeleteObject removes the file at bucket/key if it exists.
func (c *fileClient) DeleteObject(ctx context.Context, bucket, key string) error {
	path := c.path(bucket, key)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DeleteObjects removes multiple files at bucket/key locations
// File system doesn't have native batch delete, so we mimic it with individual calls
func (c *fileClient) DeleteObjects(ctx context.Context, bucket string, keys []string) ([]string, error) {
	var failed []string

	for _, key := range keys {
		if err := c.DeleteObject(ctx, bucket, key); err != nil {
			// Continue trying to delete other files even if one fails
			failed = append(failed, key)
		}
	}

	return failed, nil
}
