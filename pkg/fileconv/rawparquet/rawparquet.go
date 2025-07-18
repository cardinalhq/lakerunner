// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rawparquet

import (
	"errors"
	"fmt"
	"io"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/pkg/fileconv"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
	"github.com/parquet-go/parquet-go"
)

type RawParquetReader struct {
	fname  string
	fh     *filecrunch.FileHandle
	pf     *parquet.File
	pfr    *parquet.GenericReader[map[string]any]
	mapper *translate.Mapper
	tags   map[string]string
}

var _ fileconv.Reader = (*RawParquetReader)(nil)

func NewRawParquetReader(fname string, mapper *translate.Mapper, tags map[string]string) (*RawParquetReader, error) {
	fh, err := filecrunch.LoadSchemaForFile(fname)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema for file %s: %w", fname, err)
	}
	pf := fh.ParquetFile
	pfr := parquet.NewGenericReader[map[string]any](fh.File, fh.Schema)

	return &RawParquetReader{
		fname:  fname,
		fh:     fh,
		pf:     pf,
		pfr:    pfr,
		mapper: mapper,
		tags:   tags,
	}, nil
}

func (r *RawParquetReader) Close() error {
	var err error
	if r.pfr != nil {
		if err := r.pfr.Close(); err != nil {
			err = errors.Join(err, fmt.Errorf("failed to close parquet reader: %w", err))
		}
	}
	if r.fh != nil {
		if err := r.fh.Close(); err != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file handle: %w", err))
		}
	}
	r.pf = nil
	r.fh = nil
	return err
}

func (r *RawParquetReader) GetRow() (row map[string]any, done bool, err error) {
	if r.pfr == nil {
		return nil, true, fmt.Errorf("parquet file reader is not initialized")
	}

	rows := make([]map[string]any, 1)
	rows[0] = make(map[string]any)

	n, err := r.pfr.Read(rows)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("failed to read row: %w", err)
	}
	if n != 1 {
		return nil, true, fmt.Errorf("expected to read %d rows, got %d", len(rows), n)
	}

	parsedRow := translate.ParseLogRow(r.mapper, rows[0])

	ret := make(map[string]any)
	for k, v := range parsedRow.ResourceAttributes {
		ret["resource."+k] = v
	}
	for k, v := range parsedRow.ScopeAttributes {
		ret["scope."+k] = v
	}
	for k, v := range parsedRow.RecordAttributes {
		ret["log."+k] = v
	}
	ret["_cardinalhq.timestamp"] = parsedRow.Timestamp / 1000 // Convert to milliseconds
	ret["_cardinalhq.message"] = parsedRow.Body
	ret["_cardinalhq.name"] = "log.events"
	ret["_cardinalhq.telemetry_type"] = "logs"
	ret["_cardinalhq.value"] = int64(1)

	for k, v := range r.tags {
		ret[k] = v
	}

	return ret, false, nil
}
