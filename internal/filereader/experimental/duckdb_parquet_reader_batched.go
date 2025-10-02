//go:build experimental

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

package experimental

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	// "go.opentelemetry.io/otel/attribute" // TODO: needed for prod use
	// otelmetric "go.opentelemetry.io/otel/metric" // TODO: needed for prod use

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// DuckDBParquetBatchedReader reads rows from a Parquet file using DuckDB with SQL-level batching.
// Uses LIMIT/OFFSET to avoid loading the entire file into memory at once.
type DuckDBParquetBatchedReader struct {
	s3db      *duckdbx.S3DB
	batchSize int

	// Query template and args
	baseQuery string
	queryArgs []any

	// Schema info (discovered on first query)
	rowKeys   []wkk.RowKey
	idxTID    int
	idxSketch int
	rollupIdx []int

	// State
	currentOffset int64
	rowCount      int64
	exhausted     bool
	closed        bool
	schemaInit    bool
}

// NewDuckDBParquetBatchedReader creates a new batched DuckDB reader for the given
// Parquet file paths. Uses LIMIT/OFFSET for memory-efficient streaming.
func NewDuckDBParquetBatchedReader(ctx context.Context, s3db *duckdbx.S3DB, paths []string, batchSize int) (*DuckDBParquetBatchedReader, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if len(paths) == 0 {
		return nil, errors.New("no parquet files provided")
	}

	// Build base query template
	var baseQuery string
	var queryArgs []any

	if len(paths) == 1 {
		baseQuery = "SELECT * FROM read_parquet(?, union_by_name=true) LIMIT ? OFFSET ?"
		queryArgs = []any{paths[0]}
	} else {
		quoted := make([]string, len(paths))
		for i, p := range paths {
			quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(p, "'", "''"))
		}
		baseQuery = fmt.Sprintf("SELECT * FROM read_parquet([%s], union_by_name=true) LIMIT ? OFFSET ?", strings.Join(quoted, ","))
		queryArgs = []any{} // No args needed for multi-file case since paths are inlined
	}

	return &DuckDBParquetBatchedReader{
		s3db:      s3db,
		batchSize: batchSize,
		baseQuery: baseQuery,
		queryArgs: queryArgs,
	}, nil
}

// initSchema discovers the schema on first query
func (r *DuckDBParquetBatchedReader) initSchema(ctx context.Context) error {
	if r.schemaInit {
		return nil
	}

	// Query just the first row to get schema
	conn, release, err := r.s3db.GetConnection(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer release()

	args := append(r.queryArgs, 1, 0) // LIMIT 1 OFFSET 0
	rows, err := conn.QueryContext(ctx, r.baseQuery, args...)
	if err != nil {
		return fmt.Errorf("schema discovery query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Build schema info
	r.rowKeys = make([]wkk.RowKey, len(cols))
	for i, c := range cols {
		r.rowKeys[i] = wkk.NewRowKeyFromBytes([]byte(c))
	}

	r.idxTID = -1
	r.idxSketch = -1
	r.rollupIdx = make([]int, len(rollupFieldNames))
	for i := range r.rollupIdx {
		r.rollupIdx[i] = -1
	}

	for i, c := range cols {
		switch c {
		case "_cardinalhq.tid":
			r.idxTID = i
		case "chq_sketch":
			r.idxSketch = i
		default:
			for j, name := range rollupFieldNames {
				if c == name {
					r.rollupIdx[j] = i
					break
				}
			}
		}
	}

	r.schemaInit = true
	return nil
}

// shouldDropRow checks if a row has NaN or invalid rollup fields that should be dropped
func (r *DuckDBParquetBatchedReader) shouldDropRow(ctx context.Context, values []any) bool {
	for _, idx := range r.rollupIdx {
		if idx >= 0 && idx < len(values) {
			if v, ok := values[idx].(float64); ok {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					// TODO: rowsDroppedCounter needed for prod use
					// rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					//	attribute.String("reader", "DuckDBParquetBatchedReader"),
					//	attribute.String("reason", "NaN"),
					// ))
					return true
				}
			}
		}
	}
	return false
}

// Next returns the next batch of rows from the parquet file using SQL-level batching
func (r *DuckDBParquetBatchedReader) Next(ctx context.Context) (*filereader.Batch, error) {
	if r.closed {
		return nil, errors.New("reader is closed")
	}
	if r.exhausted {
		return nil, io.EOF
	}

	// Initialize schema on first call
	if err := r.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("schema initialization failed: %w", err)
	}

	// Execute batched query with LIMIT/OFFSET
	conn, release, err := r.s3db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	args := append(r.queryArgs, r.batchSize, r.currentOffset)
	rows, err := conn.QueryContext(ctx, r.baseQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("batched query failed: %w", err)
	}
	defer rows.Close()

	batch := pipeline.GetBatch()
	validRows := 0

	// Prepare scanning
	values := make([]any, len(r.rowKeys))
	scanArgs := make([]any, len(r.rowKeys))
	for i := range scanArgs {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			pipeline.ReturnBatch(batch)
			return nil, fmt.Errorf("scan row: %w", err)
		}

		// Handle _cardinalhq.tid field conversion
		if r.idxTID >= 0 {
			val := values[r.idxTID]
			switch v := val.(type) {
			case string:
				tidInt, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					// TODO: rowsDroppedCounter needed for prod use
					// rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					//	attribute.String("reader", "DuckDBParquetBatchedReader"),
					//	attribute.String("reason", "invalid_tid_conversion"),
					// ))
					continue
				}
				values[r.idxTID] = tidInt
			case int64:
				// Already correct type
			case nil:
				// Null value is fine
			default:
				// TODO: rowsDroppedCounter needed for prod use
				// rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				//	attribute.String("reader", "DuckDBParquetBatchedReader"),
				//	attribute.String("reason", "invalid_tid_type"),
				// ))
				continue
			}
		}

		// Handle sketch field conversion
		if r.idxSketch >= 0 {
			if s, ok := values[r.idxSketch].(string); ok {
				values[r.idxSketch] = []byte(s)
			}
		}

		// Check for invalid rollup values
		if r.shouldDropRow(ctx, values) {
			continue
		}

		// Add row to batch
		batchRow := batch.AddRow()
		for i, key := range r.rowKeys {
			if b, ok := values[i].([]byte); ok {
				// DuckDB reuses backing arrays, so copy before storing
				batchRow[key] = append([]byte(nil), b...)
			} else {
				batchRow[key] = values[i]
			}
		}
		validRows++
	}

	if err := rows.Err(); err != nil {
		pipeline.ReturnBatch(batch)
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Update state
	r.currentOffset += int64(validRows)
	r.rowCount += int64(validRows)

	// Check if we've exhausted the data
	if validRows == 0 {
		r.exhausted = true
		pipeline.ReturnBatch(batch)
		return nil, io.EOF
	}

	// If we got fewer rows than requested, we've likely reached the end
	if validRows < r.batchSize {
		r.exhausted = true
	}

	return batch, nil
}

// Close closes the reader and releases resources
func (r *DuckDBParquetBatchedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return nil
}

// TotalRowsReturned returns the total number of rows successfully returned
func (r *DuckDBParquetBatchedReader) TotalRowsReturned() int64 {
	return r.rowCount
}
