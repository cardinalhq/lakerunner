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

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
)

type ObjectFetcher interface {
	Download(ctx context.Context, bucket, key, tmpdir string) (tmpfile string, size int64, notFound bool, err error)
}

type GenericMapReader interface {
	Read(batch []map[string]any) (int, error)
	Close() error
}

type FileOpener interface {
	LoadSchemaForFile(path string) (*filecrunch.FileHandle, error)
	NewGenericMapReader(f *os.File, schema *parquet.Schema) (GenericMapReader, error)
}

type Writer interface {
	Write(map[string]any) error
	Close() ([]buffet.Result, error)
}

type WriterFactory interface {
	NewWriter(kind, tmpdir string, nodes map[string]parquet.Node, targetRowGroup int64) (Writer, error)
}
