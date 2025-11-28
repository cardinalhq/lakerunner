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
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// CSVReader reads rows from a CSV stream using pipeline semantics.
type CSVReader struct {
	reader    *csv.Reader
	headers   []string
	rowKeys   []wkk.RowKey
	closed    bool
	totalRows int64
	closer    io.Closer
	batchSize int
	rowIndex  int
}

// NewCSVReader creates a new CSVReader for the given io.ReadCloser.
// The reader takes ownership of the closer and will close it when Close is called.
func NewCSVReader(reader io.ReadCloser, batchSize int) (*CSVReader, error) {
	csvReader := csv.NewReader(reader)
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields

	// Read headers
	headers, err := csvReader.Read()
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	if len(headers) == 0 {
		_ = reader.Close()
		return nil, fmt.Errorf("CSV file has no headers")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Pre-create RowKeys for headers for better performance
	rowKeys := make([]wkk.RowKey, len(headers))
	for i, header := range headers {
		rowKeys[i] = wkk.NewRowKey(header)
	}

	return &CSVReader{
		reader:    csvReader,
		headers:   headers,
		rowKeys:   rowKeys,
		closer:    reader,
		batchSize: batchSize,
		rowIndex:  0,
	}, nil
}

func (r *CSVReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		record, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			pipeline.ReturnBatch(batch)
			return nil, fmt.Errorf("CSV read error at line %d: %w", r.rowIndex+2, err)
		}

		r.rowIndex++

		// Skip rows with wrong number of columns
		if len(record) != len(r.headers) {
			// Track dropped rows
			rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("reader", "CSVReader"),
				attribute.String("reason", "column_count_mismatch"),
			))
			continue
		}

		// Track rows read from input
		rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "CSVReader"),
		))

		// Convert to Row
		batchRow := batch.AddRow()
		for i, value := range record {
			// Try to parse as number if possible
			parsedValue := r.parseValue(value)
			batchRow[r.rowKeys[i]] = parsedValue
		}
	}

	if batch.Len() == 0 {
		r.closed = true
		pipeline.ReturnBatch(batch)
		return nil, io.EOF
	}

	r.totalRows += int64(batch.Len())
	// Track rows output to downstream
	rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
		attribute.String("reader", "CSVReader"),
	))
	return batch, nil
}

// parseValue attempts to parse a string value as a number if possible
func (r *CSVReader) parseValue(value string) any {
	trimmed := strings.TrimSpace(value)

	// Empty strings remain as empty strings
	if trimmed == "" {
		return ""
	}

	// Try to parse as integer
	if i, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return i
	}

	// Try to parse as float
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return f
	}

	// Keep as string
	return value
}

// Close closes the reader and the underlying io.ReadCloser.
func (r *CSVReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	var err error
	if r.closer != nil {
		err = r.closer.Close()
		r.closer = nil
	}
	r.reader = nil
	return err
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *CSVReader) TotalRowsReturned() int64 {
	return r.totalRows
}

// GetSchema returns schema information extracted from CSV headers.
// All columns are marked as nullable since CSV doesn't enforce types.
func (r *CSVReader) GetSchema() *ReaderSchema {
	if r.headers == nil {
		return nil
	}

	schema := NewReaderSchema()
	for _, header := range r.headers {
		// CSV data is dynamically typed - can be string, int64, or float64
		// Mark all as string type with nullable=true since we don't know upfront
		schema.AddColumn(wkk.NewRowKey(header), DataTypeString, true)
	}
	return schema
}
