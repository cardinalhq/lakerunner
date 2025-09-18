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
	"database/sql"
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

// DuckDBParquetRawReader reads rows from a Parquet file using DuckDB.
// It streams rows in batches without loading the entire file into memory.
type DuckDBParquetRawReader struct {
	s3db      *duckdbx.S3DB
	conn      *sql.Conn
	release   func()
	rows      *sql.Rows
	batchSize int

	rowKeys  []wkk.RowKey
	values   []any
	scanArgs []any

	idxTID    int
	idxSketch int
	rollupIdx []int

	rowCount  int64
	exhausted bool
	closed    bool
}

var rollupFieldNames = []string{
	"rollup_sum", "rollup_count", "rollup_avg", "rollup_min", "rollup_max",
	"rollup_p25", "rollup_p50", "rollup_p75", "rollup_p90", "rollup_p95", "rollup_p99",
}

// NewDuckDBParquetRawReader creates a new DuckDBParquetRawReader for the given
// Parquet file paths. Multiple files will be read using DuckDB's
// union_by_name option to unify schemas.
// The s3db parameter provides the DuckDB connection pool to use.
func NewDuckDBParquetRawReader(ctx context.Context, s3db *duckdbx.S3DB, paths []string, batchSize int) (*DuckDBParquetRawReader, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if len(paths) == 0 {
		return nil, errors.New("no parquet files provided")
	}

	// Get a connection from the pool
	conn, release, err := s3db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection from pool: %w", err)
	}

	var (
		query string
		args  []any
	)
	if len(paths) == 1 {
		query = "SELECT * FROM read_parquet(?, union_by_name=true)"
		args = []any{paths[0]}
	} else {
		quoted := make([]string, len(paths))
		for i, p := range paths {
			quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(p, "'", "''"))
		}
		query = fmt.Sprintf("SELECT * FROM read_parquet([%s], union_by_name=true)", strings.Join(quoted, ","))
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		release()
		return nil, fmt.Errorf("duckdb query: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		release()
		return nil, fmt.Errorf("duckdb columns: %w", err)
	}

	rowKeys := make([]wkk.RowKey, len(cols))
	for i, c := range cols {
		rowKeys[i] = wkk.NewRowKeyFromBytes([]byte(c))
	}

	values := make([]any, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range scanArgs {
		scanArgs[i] = &values[i]
	}

	idxTID := -1
	idxSketch := -1
	rollupIdx := make([]int, len(rollupFieldNames))
	for i := range rollupIdx {
		rollupIdx[i] = -1
	}
	for i, c := range cols {
		switch c {
		case "_cardinalhq.tid":
			idxTID = i
		case "sketch":
			idxSketch = i
		default:
			for j, name := range rollupFieldNames {
				if c == name {
					rollupIdx[j] = i
					break
				}
			}
		}
	}

	return &DuckDBParquetRawReader{
		s3db:      s3db,
		conn:      conn,
		release:   release,
		rows:      rows,
		batchSize: batchSize,
		rowKeys:   rowKeys,
		values:    values,
		scanArgs:  scanArgs,
		idxTID:    idxTID,
		idxSketch: idxSketch,
		rollupIdx: rollupIdx,
	}, nil
}

func (r *DuckDBParquetRawReader) shouldDropRow(ctx context.Context) bool {
	for _, idx := range r.rollupIdx {
		if idx >= 0 {
			if v, ok := r.values[idx].(float64); ok {
				if math.IsNaN(v) || math.IsInf(v, 0) {
					// TODO: rowsDroppedCounter needed for prod use
					// rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					//	attribute.String("reader", "DuckDBParquetRawReader"),
					//	attribute.String("reason", "NaN"),
					// ))
					return true
				}
			}
		}
	}
	return false
}

// Next returns the next batch of rows from the parquet file.
func (r *DuckDBParquetRawReader) Next(ctx context.Context) (*filereader.Batch, error) {
	if r.closed || r.rows == nil {
		return nil, errors.New("reader is closed or not initialized")
	}
	if r.exhausted {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()
	validRows := 0
	for validRows < r.batchSize {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				pipeline.ReturnBatch(batch)
				return nil, fmt.Errorf("duckdb rows error: %w", err)
			}
			r.exhausted = true
			break
		}

		if err := r.rows.Scan(r.scanArgs...); err != nil {
			pipeline.ReturnBatch(batch)
			return nil, fmt.Errorf("scan row: %w", err)
		}

		if r.idxTID >= 0 {
			val := r.values[r.idxTID]
			switch v := val.(type) {
			case string:
				tidInt, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					// TODO: rowsDroppedCounter needed for prod use
					// rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					//	attribute.String("reader", "DuckDBParquetRawReader"),
					//	attribute.String("reason", "invalid_tid_conversion"),
					// ))
					continue
				}
				r.values[r.idxTID] = tidInt
			case int64:
			case nil:
			default:
				// TODO: rowsDroppedCounter needed for prod use
				// rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				//	attribute.String("reader", "DuckDBParquetRawReader"),
				//	attribute.String("reason", "invalid_tid_type"),
				// ))
				continue
			}
		}

		if r.idxSketch >= 0 {
			if s, ok := r.values[r.idxSketch].(string); ok {
				r.values[r.idxSketch] = []byte(s)
			}
		}

		if r.shouldDropRow(ctx) {
			continue
		}

		batchRow := batch.AddRow()
		for i, key := range r.rowKeys {
			if b, ok := r.values[i].([]byte); ok {
				// DuckDB reuses the backing array for BLOB values, so copy before storing
				batchRow[key] = append([]byte(nil), b...)
				continue
			}
			batchRow[key] = r.values[i]
		}
		validRows++
	}

	r.rowCount += int64(validRows)

	if validRows == 0 {
		pipeline.ReturnBatch(batch)
		if r.exhausted {
			return nil, io.EOF
		}
		return r.Next(ctx)
	}

	return batch, nil
}

// Close closes the reader and releases resources.
func (r *DuckDBParquetRawReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.rows != nil {
		r.rows.Close()
		r.rows = nil
	}
	if r.release != nil {
		r.release()
		r.release = nil
	}
	return nil
}

// TotalRowsReturned returns the total number of rows successfully returned.
func (r *DuckDBParquetRawReader) TotalRowsReturned() int64 {
	return r.rowCount
}
