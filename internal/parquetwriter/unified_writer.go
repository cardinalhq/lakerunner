// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package parquetwriter

import (
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/pipeline"
)

// UnifiedWriter is the main implementation of ParquetWriter that coordinates
// all the components (sizing, splitting) to create optimized Parquet files.
type UnifiedWriter struct {
	config   WriterConfig
	splitter *FileSplitter
	closed   bool
}

var _ ParquetWriter = (*UnifiedWriter)(nil)

// NewUnifiedWriter creates a new unified Parquet writer with the given configuration.
func NewUnifiedWriter(config WriterConfig) (*UnifiedWriter, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Create the file splitter
	splitter := NewFileSplitter(config)

	return &UnifiedWriter{
		config:   config,
		splitter: splitter,
	}, nil
}

// Config returns the configuration used by this writer.
func (w *UnifiedWriter) Config() WriterConfig {
	return w.config
}

// Close finalizes all processing and returns metadata about the created files.
func (w *UnifiedWriter) Close(ctx context.Context) ([]Result, error) {
	if w.closed {
		return nil, ErrWriterClosed
	}
	w.closed = true

	// Ensure cleanup happens even if we encounter errors
	defer func() {
		if w.splitter != nil {
			w.splitter.Abort()
		}
	}()

	// Get final results from splitter
	results, err := w.splitter.Close(ctx)
	if err != nil {
		return results, fmt.Errorf("close splitter: %w", err)
	}
	w.splitter = nil

	return results, nil
}

// Abort stops processing and cleans up all resources and temporary files.
func (w *UnifiedWriter) Abort() {
	if w.closed {
		return
	}
	w.closed = true

	if w.splitter != nil {
		w.splitter.Abort()
		w.splitter = nil
	}
}

// WriteBatch efficiently writes multiple rows from a pipeline batch.
// This preserves string interning and avoids per-row map conversions.
func (w *UnifiedWriter) WriteBatch(batch *pipeline.Batch) error {
	if w.closed {
		return ErrWriterClosed
	}

	if batch == nil {
		return fmt.Errorf("%w: batch cannot be nil", ErrInvalidRow)
	}

	// Write batch directly to the splitter using a background context
	// The actual context handling happens in Close()
	return w.splitter.WriteBatchRows(context.TODO(), batch)
}

// GetCurrentStats returns current statistics about the writer's state.
// This is useful for monitoring and debugging.
func (w *UnifiedWriter) GetCurrentStats() WriterStats {
	stats := WriterStats{
		Closed: w.closed,
	}

	stats.RecordsPerFile = w.config.RecordsPerFile

	return stats
}
