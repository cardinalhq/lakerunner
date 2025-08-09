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

package cmd

import (
	"context"
	"os"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/parquet-go/parquet-go"
)

type objectFetcherAdapter struct {
	s3Client *awsclient.S3Client
}

var _ ObjectFetcher = (*objectFetcherAdapter)(nil)

func (a objectFetcherAdapter) Download(ctx context.Context, bucket, key, tmpdir string) (string, int64, error) {
	return s3helper.DownloadS3Object(ctx, tmpdir, a.s3Client, bucket, key)
}

type fileOpenerAdapter struct{}

var _ FileOpener = (*fileOpenerAdapter)(nil)

func (fileOpenerAdapter) LoadSchemaForFile(path string) (*filecrunch.FileHandle, error) {
	return filecrunch.LoadSchemaForFile(path)
}

func (fileOpenerAdapter) NewParquetReader(f *os.File, schema *parquet.Schema) *parquet.Reader {
	return parquet.NewReader(f, schema)
}

type writerFactoryAdapter struct{}

var _ WriterFactory = (*writerFactoryAdapter)(nil)

func (writerFactoryAdapter) NewWriter(kind, tmpdir string, nodes map[string]parquet.Node, targetRowGroup int64) (Writer, error) {
	bw, err := buffet.NewWriter(kind, tmpdir, nodes, targetRowGroup)
	if err != nil {
		return nil, err
	}
	return writerAdapter{bw}, nil
}

type writerAdapter struct {
	inner *buffet.Writer
}

var _ Writer = (*writerAdapter)(nil)

func (w writerAdapter) Write(rec map[string]any) error {
	return w.inner.Write(rec)
}

func (w writerAdapter) Close() ([]buffet.Result, error) {
	return w.inner.Close()
}
