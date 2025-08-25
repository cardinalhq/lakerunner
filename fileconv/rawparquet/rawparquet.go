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

package rawparquet

import (
	"errors"
	"fmt"
	"io"
	"maps"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/fileconv"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
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
		if err = r.pfr.Close(); err != nil {
			err = errors.Join(err, fmt.Errorf("failed to close parquet reader: %w", err))
		}
	}
	if r.fh != nil {
		if err = r.fh.Close(); err != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file handle: %w", err))
		}
	}
	r.pf = nil
	r.fh = nil
	return err
}

func (r *RawParquetReader) NumRows() int64 {
	if r.pfr == nil {
		return 0
	}
	return r.pfr.NumRows()
}

func (r *RawParquetReader) GetRow() (row map[string]any, done bool, err error) {
	if r.pfr == nil {
		return nil, true, fmt.Errorf("parquet file reader is not initialized")
	}

	rows := make([]map[string]any, 1)
	rows[0] = make(map[string]any)

	n, err := r.pfr.Read(rows)
	if n == 0 && err != nil {
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
		ret[k] = v
	}
	for k, v := range parsedRow.ScopeAttributes {
		ret[k] = v
	}
	for k, v := range parsedRow.RecordAttributes {
		ret[k] = v
	}
	maps.Copy(ret, parsedRow.RawAttributes)
	if _, ok := ret["_cardinalhq.timestamp"]; !ok && parsedRow.Timestamp > 0 {
		ret["_cardinalhq.timestamp"] = parsedRow.Timestamp
	}
	if _, ok := ret["_cardinalhq.message"]; !ok && parsedRow.Body != "" {
		ret["_cardinalhq.message"] = parsedRow.Body
	}
	ret["_cardinalhq.name"] = "log.events"
	ret["_cardinalhq.telemetry_type"] = "logs"
	ret["_cardinalhq.value"] = float64(1)

	for k, v := range r.tags {
		ret[k] = v
	}

	return ret, false, nil
}
