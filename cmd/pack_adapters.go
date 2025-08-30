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
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/objstore"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
)

type objectFetcherAdapter struct {
	s3Client *s3.Client
}

var _ ObjectFetcher = (*objectFetcherAdapter)(nil)

func (a objectFetcherAdapter) Download(ctx context.Context, bucket, key, tmpdir string) (string, int64, bool, error) {
	// Create a temporary S3Manager to use the DownloadToTempFile function
	manager := objstore.NewAWSS3Manager(a.s3Client)
	return manager.DownloadToTempFile(ctx, tmpdir, bucket, key)
}

// In pack_adapters.go

type fileOpenerAdapter struct{}

var _ FileOpener = (*fileOpenerAdapter)(nil)

func (fileOpenerAdapter) LoadSchemaForFile(path string) (*filecrunch.FileHandle, error) {
	return filecrunch.LoadSchemaForFile(path)
}

type genericMapReaderAdapter struct {
	r *parquet.GenericReader[map[string]any]
}

var mapPool = sync.Pool{New: func() any { return make(map[string]any) }}

func (g genericMapReaderAdapter) Read(batch []map[string]any) (int, error) {
	if len(batch) > 0 && batch[0] == nil {
		for i := range batch {
			batch[i] = mapPool.Get().(map[string]any)
		}
	}
	return g.r.Read(batch)
}

func (g genericMapReaderAdapter) Close() error { return g.r.Close() }

func (fileOpenerAdapter) NewGenericMapReader(f *os.File, schema *parquet.Schema) (GenericMapReader, error) {
	// If your parquet-go requires options/schema here, thread them in as needed.
	gr := parquet.NewGenericReader[map[string]any](f, schema)
	return genericMapReaderAdapter{r: gr}, nil
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
