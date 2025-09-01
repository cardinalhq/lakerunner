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

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// TranslatingReader wraps another Reader and applies row transformations.
// This enables composition where any Reader can be enhanced with signal-specific
// translation logic without coupling file parsing to data transformation.
type TranslatingReader struct {
	reader     Reader
	translator RowTranslator
	closed     bool
	rowCount   int64 // Track total rows successfully read and translated
	batchSize  int
}

// NewTranslatingReader creates a new TranslatingReader that applies the given
// translator to each row returned by the underlying reader.
//
// The TranslatingReader takes ownership of the underlying reader and will
// close it when Close() is called.
func NewTranslatingReader(reader Reader, translator RowTranslator, batchSize int) (*TranslatingReader, error) {
	if reader == nil {
		return nil, errors.New("reader cannot be nil")
	}
	if translator == nil {
		return nil, errors.New("translator cannot be nil")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &TranslatingReader{
		reader:     reader,
		translator: translator,
		batchSize:  batchSize,
	}, nil
}

// Next returns the next batch of translated rows from the underlying reader.
func (tr *TranslatingReader) Next() (*Batch, error) {
	if tr.closed {
		return nil, errors.New("reader is closed")
	}

	// Get raw batch from underlying reader
	batch, err := tr.reader.Next()
	if batch == nil {
		return nil, err
	}

	// Track rows read from underlying reader
	rowsInCounter.Add(context.Background(), int64(batch.Len()), otelmetric.WithAttributes(
		attribute.String("reader", "TranslatingReader"),
	))

	// Create a new batch for translated rows
	translatedBatch := pipeline.GetBatch()

	// Translate each row in the batch
	for i := 0; i < batch.Len(); i++ {
		sourceRow := batch.Get(i)
		// Copy row to make it mutable
		row := make(Row)
		for k, v := range sourceRow {
			row[k] = v
		}

		if translateErr := tr.translator.TranslateRow(&row); translateErr != nil {
			// TODO: Add logging here when we have access to a logger

			// Return partial batch if we've successfully translated some rows
			if translatedBatch.Len() > 0 {
				tr.rowCount += int64(translatedBatch.Len())
				return translatedBatch, fmt.Errorf("translation failed for row %d: %w", i, translateErr)
			}

			// No rows successfully translated
			pipeline.ReturnBatch(translatedBatch)
			return nil, fmt.Errorf("translation failed for row %d: %w", i, translateErr)
		}

		// Add translated row to new batch
		translatedRow := translatedBatch.AddRow()
		for k, v := range row {
			translatedRow[k] = v
		}
	}

	// Return original batch to pool since we created a new one
	pipeline.ReturnBatch(batch)

	// Count each successfully translated row
	tr.rowCount += int64(translatedBatch.Len())

	// Track rows output to downstream
	rowsOutCounter.Add(context.Background(), int64(translatedBatch.Len()), otelmetric.WithAttributes(
		attribute.String("reader", "TranslatingReader"),
	))

	return translatedBatch, nil
}

// Close closes the underlying reader and releases resources.
func (tr *TranslatingReader) Close() error {
	if tr.closed {
		return nil
	}
	tr.closed = true

	if tr.reader != nil {
		if err := tr.reader.Close(); err != nil {
			return fmt.Errorf("failed to close underlying reader: %w", err)
		}
		tr.reader = nil
	}
	tr.translator = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully
// returned via Next() after translation by this reader.
func (tr *TranslatingReader) TotalRowsReturned() int64 {
	return tr.rowCount
}
