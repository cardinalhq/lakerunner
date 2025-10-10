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

package filereader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// ParquetRawReader reads rows from a generic Parquet stream.
// This reader provides raw parquet data without any opinionated transformations.
// Use wrapper readers like CookedMetricTranslatingReader for domain-specific logic.
type ParquetRawReader struct {
	pf        *parquet.File
	pfr       *parquet.GenericReader[map[string]any]
	closed    bool
	exhausted bool
	rowCount  int64
	batchSize int
	readBuf   []map[string]any // reusable buffer for reading parquet rows
}

// NewParquetRawReader creates a new ParquetRawReader for the given io.ReaderAt.
func NewParquetRawReader(reader io.ReaderAt, size int64, batchSize int) (*ParquetRawReader, error) {
	pf, err := parquet.OpenFile(reader, size)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	pfr := parquet.NewGenericReader[map[string]any](pf, pf.Schema())

	if pf.NumRows() == 0 {
		return nil, fmt.Errorf("parquet file has no rows")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Pre-allocate reusable buffer for reading parquet rows
	readBuf := make([]map[string]any, batchSize)
	for i := range readBuf {
		readBuf[i] = make(map[string]any)
	}

	return &ParquetRawReader{
		pf:        pf,
		pfr:       pfr,
		batchSize: batchSize,
		readBuf:   readBuf,
	}, nil
}

// Next returns the next batch of rows from the parquet file.
func (r *ParquetRawReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed || r.pfr == nil {
		return nil, errors.New("reader is closed or not initialized")
	}

	if r.exhausted {
		return nil, io.EOF
	}

	// Clear the reusable buffer maps from previous use
	for i := range r.readBuf {
		for k := range r.readBuf[i] {
			delete(r.readBuf[i], k)
		}
	}

	n, err := r.pfr.Read(r.readBuf)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("parquet reader error: %w", err)
	}
	if n == 0 {
		r.exhausted = true
		return nil, io.EOF
	}

	rowsInCounter.Add(ctx, int64(n), otelmetric.WithAttributes(
		attribute.String("reader", "ParquetRawReader"),
	))

	batch := pipeline.GetBatch()

	for i := range n {
		row := r.readBuf[i]
		batchRow := batch.AddRow()
		for k, v := range row {
			fieldName := strings.ReplaceAll(k, ".", "_")
			batchRow[wkk.NewRowKeyFromBytes([]byte(fieldName))] = v
		}
	}

	r.rowCount += int64(n)

	rowsOutCounter.Add(ctx, int64(n), otelmetric.WithAttributes(
		attribute.String("reader", "ParquetRawReader"),
	))

	if err == io.EOF {
		r.exhausted = true
	}

	return batch, nil
}

// Close closes the reader and releases resources.
func (r *ParquetRawReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.pfr != nil {
		if err := r.pfr.Close(); err != nil {
			return fmt.Errorf("failed to close parquet reader: %w", err)
		}
		r.pfr = nil
	}
	r.pf = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *ParquetRawReader) TotalRowsReturned() int64 {
	return r.rowCount
}
