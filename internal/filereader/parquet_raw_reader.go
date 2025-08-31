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
	"math"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// ParquetRawReader reads rows from a generic Parquet stream.
type ParquetRawReader struct {
	pf        *parquet.File
	pfr       *parquet.GenericReader[map[string]any]
	closed    bool
	exhausted bool
	rowCount  int64
	batchSize int
}

// shouldDropRowForInvalidRollupFields checks if a row has NaN or invalid rollup fields that should be dropped.
// This filters corrupted data at the parquet source level to isolate data quality issues.
func shouldDropRowForInvalidRollupFields(row map[string]any) bool {
	// List of rollup fields to validate
	rollupFields := []string{
		"rollup_sum", "rollup_count", "rollup_avg", "rollup_min", "rollup_max",
		"rollup_p25", "rollup_p50", "rollup_p75", "rollup_p90", "rollup_p95", "rollup_p99",
	}

	for _, fieldName := range rollupFields {
		if value, exists := row[fieldName]; exists {
			if floatVal, ok := value.(float64); ok {
				if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
					rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
						attribute.String("reader", "ParquetRawReader"),
						attribute.String("reason", "NaN"),
					))
					return true
				}
			}
		}
	}

	return false
}

// NewParquetRawReader creates a new ParquetRawReader for the given io.ReaderAt.
// The caller is responsible for closing the underlying reader.
func NewParquetRawReader(reader io.ReaderAt, size int64, batchSize int) (*ParquetRawReader, error) {
	pf, err := parquet.OpenFile(reader, size)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Use the file's schema to create a GenericReader
	pfr := parquet.NewGenericReader[map[string]any](pf, pf.Schema())

	// Check if file has data
	if pf.NumRows() == 0 {
		return nil, fmt.Errorf("parquet file has no rows")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &ParquetRawReader{
		pf:        pf,
		pfr:       pfr,
		batchSize: batchSize,
	}, nil
}

// Next returns the next batch of rows from the parquet file.
func (r *ParquetRawReader) Next() (*Batch, error) {
	if r.closed || r.pfr == nil {
		return nil, errors.New("reader is closed or not initialized")
	}

	if r.exhausted {
		return nil, io.EOF
	}

	// Create fresh maps for parquet reader to populate
	parquetRows := make([]map[string]any, r.batchSize)
	for i := range parquetRows {
		parquetRows[i] = make(map[string]any)
	}

	n, err := r.pfr.Read(parquetRows)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("parquet reader error: %w", err)
	}
	if n == 0 {
		r.exhausted = true
		return nil, io.EOF
	}

	// Track rows read from parquet
	rowsInCounter.Add(context.Background(), int64(n), otelmetric.WithAttributes(
		attribute.String("reader", "ParquetRawReader"),
	))

	batch := pipeline.GetBatch()

	// Transfer ownership instead of copying to reduce memory pressure
	// Also perform defensive conversion of _cardinalhq.tid field
	validRows := 0
	for i := 0; i < n; i++ {
		row := parquetRows[i]

		// Check and convert _cardinalhq.tid field if present
		if tidValue, exists := row["_cardinalhq.tid"]; exists {
			switch v := tidValue.(type) {
			case string:
				// Convert string to int64
				if tidInt64, err := strconv.ParseInt(v, 10, 64); err == nil {
					row["_cardinalhq.tid"] = tidInt64
				} else {
					// Drop row if conversion fails
					rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
						attribute.String("reader", "ParquetRawReader"),
						attribute.String("reason", "invalid_tid_conversion"),
					))
					continue
				}
			case int64:
				// Already correct type, no conversion needed
			default:
				// Drop row if _cardinalhq.tid is neither string nor int64
				rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
					attribute.String("reader", "ParquetRawReader"),
					attribute.String("reason", "invalid_tid_type"),
				))
				continue
			}
		}

		// Convert sketch field from string to []byte if needed (parquet sometimes returns byte fields as strings)
		if sketchValue, exists := row["sketch"]; exists {
			if sketchStr, ok := sketchValue.(string); ok {
				row["sketch"] = []byte(sketchStr)
			}
			// If it's already []byte or nil, leave it as-is
		}

		// Filter out rows with NaN or invalid rollup fields - this indicates corrupted data from parquet source
		if shouldDropRowForInvalidRollupFields(row) {
			continue
		}

		// Use AddRow and copy the data
		batchRow := batch.AddRow()
		for k, v := range row {
			batchRow[wkk.NewRowKeyFromBytes([]byte(k))] = v
		}
		validRows++
	}

	// Only increment rowCount for successfully processed rows
	r.rowCount += int64(validRows)

	// Track rows output to downstream
	rowsOutCounter.Add(context.Background(), int64(validRows), otelmetric.WithAttributes(
		attribute.String("reader", "ParquetRawReader"),
	))

	// If underlying reader hit EOF, mark as exhausted for next call
	if err == io.EOF {
		r.exhausted = true
	}

	// Return EOF if no valid rows remain
	if validRows == 0 {
		pipeline.ReturnBatch(batch)
		if r.exhausted {
			return nil, io.EOF
		}
		// If we dropped all rows but haven't reached EOF, try reading more
		return r.Next()
	}

	// Return valid batch without EOF (EOF will be returned on next call if exhausted)
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
