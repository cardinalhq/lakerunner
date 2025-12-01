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

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
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

var _ Reader = (*TranslatingReader)(nil)

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
func (tr *TranslatingReader) Next(ctx context.Context) (*Batch, error) {
	if tr.closed {
		return nil, errors.New("reader is closed")
	}

	// Get raw batch from underlying reader
	batch, err := tr.reader.Next(ctx)
	if batch == nil {
		return nil, err
	}

	// Track rows read from underlying reader
	rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
		attribute.String("reader", "TranslatingReader"),
	))

	// Create output batch for successful translations
	outputBatch := pipeline.GetBatch()

	// Translate each row, dropping failures and transferring successes with zero-copy
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)

		if translateErr := tr.translator.TranslateRow(ctx, &row); translateErr != nil {
			// Drop this row and increment counter
			rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("reader", "TranslatingReader"),
				attribute.String("reason", "translation_failed"),
			))
			continue // Skip this row, move to next
		}

		// Translation succeeded - transfer row to output batch (zero-copy)
		outputRow := outputBatch.AddRow()
		// Copy the translated data to the output row
		for k, v := range row {
			outputRow[k] = v
		}
	}

	// Return input batch to pool
	pipeline.ReturnBatch(batch)

	// If all rows were dropped, try to get another batch
	if outputBatch.Len() == 0 {
		pipeline.ReturnBatch(outputBatch)
		// Recursively try the next batch
		return tr.Next(ctx)
	}

	// Count each successfully translated row
	tr.rowCount += int64(outputBatch.Len())

	// Track rows output to downstream
	rowsOutCounter.Add(ctx, int64(outputBatch.Len()), otelmetric.WithAttributes(
		attribute.String("reader", "TranslatingReader"),
	))

	return outputBatch, nil
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

// GetSchema delegates to the wrapped reader.
func (tr *TranslatingReader) GetSchema() *ReaderSchema {
	var schema *ReaderSchema
	if tr.reader != nil {
		schema = tr.reader.GetSchema()
	} else {
		schema = NewReaderSchema()
	}

	// TranslatingReader is a generic wrapper that doesn't know what fields the translator adds.
	// However, we know that translators commonly add CHQ internal fields that need to be in the schema
	// before normalization happens (especially in MergesortReader).
	// Mark as hasNonNull=true since translators add these fields (even if conditionally).
	schema.AddColumn(wkk.RowKeyCTelemetryType, wkk.RowKeyCTelemetryType, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCCustomerID, wkk.RowKeyCCustomerID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCFingerprint, wkk.RowKeyCFingerprint, DataTypeInt64, true)

	return schema
}
