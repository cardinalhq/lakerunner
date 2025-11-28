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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// JSONLinesReader reads rows from a JSON lines stream using pipeline semantics.
type JSONLinesReader struct {
	scanner   *bufio.Scanner
	rowIndex  int
	closed    bool
	totalRows int64
	closer    io.Closer
	batchSize int
}

var _ Reader = (*JSONLinesReader)(nil)

// NewJSONLinesReader creates a new JSONLinesReader for the given io.ReadCloser.
// The reader takes ownership of the closer and will close it when Close is called.
func NewJSONLinesReader(reader io.ReadCloser, batchSize int) (*JSONLinesReader, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), constants.MaxLineSizeBytes)

	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	return &JSONLinesReader{
		scanner:   scanner,
		rowIndex:  0,
		closer:    reader,
		batchSize: batchSize,
	}, nil
}

func (r *JSONLinesReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		if !r.scanner.Scan() {
			// Check for scanner error
			if err := r.scanner.Err(); err != nil {
				pipeline.ReturnBatch(batch)
				return nil, fmt.Errorf("scanner error reading at line %d: %w", r.rowIndex+1, err)
			}
			// End of file - return what we have
			break
		}

		line := strings.TrimSpace(r.scanner.Text())
		r.rowIndex++

		// Skip empty lines
		if line == "" {
			continue
		}

		// Track lines read from input
		rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "JSONLinesReader"),
		))

		// Parse JSON into string-keyed map first
		var jsonRow map[string]any
		if err := json.Unmarshal([]byte(line), &jsonRow); err != nil {
			pipeline.ReturnBatch(batch)
			return nil, fmt.Errorf("JSON parse error at line %d: %w", r.rowIndex, err)
		}

		// Convert to Row with RowKey keys
		batchRow := batch.AddRow()
		for k, v := range jsonRow {
			batchRow[wkk.NewRowKey(k)] = v
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
		attribute.String("reader", "JSONLinesReader"),
	))
	return batch, nil
}

// Close closes the reader and the underlying io.ReadCloser.
func (r *JSONLinesReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	var err error
	if r.closer != nil {
		err = r.closer.Close()
		r.closer = nil
	}
	r.scanner = nil
	return err
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *JSONLinesReader) TotalRowsReturned() int64 {
	return r.totalRows
}

// GetSchema returns nil as JSON Lines does not have upfront schema metadata.
// Schema must be inferred from the data itself.
func (r *JSONLinesReader) GetSchema() *ReaderSchema {
	return NewReaderSchema()
}
