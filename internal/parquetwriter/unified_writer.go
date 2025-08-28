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

package parquetwriter

import (
	"context"
	"fmt"
)

// UnifiedWriter is the main implementation of ParquetWriter that coordinates
// all the components (sizing, splitting) to create optimized Parquet files.
type UnifiedWriter struct {
	config   WriterConfig
	splitter *FileSplitter
	closed   bool
}

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

// Write adds a row to the writer and writes it directly to the output files.
func (w *UnifiedWriter) Write(row map[string]any) error {
	if w.closed {
		return ErrWriterClosed
	}

	if row == nil {
		return fmt.Errorf("%w: row cannot be nil", ErrInvalidRow)
	}

	// Write row directly to the splitter using a background context
	// The actual context handling happens in Close()
	return w.splitter.WriteRow(context.TODO(), row)
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

// WriteBatch is a convenience method to write multiple rows at once.
// This can be more efficient than calling Write multiple times for large batches.
func (w *UnifiedWriter) WriteBatch(rows []map[string]any) error {
	if w.closed {
		return ErrWriterClosed
	}

	for i, row := range rows {
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write row %d: %w", i, err)
		}
	}

	return nil
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

// WriterStats contains current statistics about a writer's state.
type WriterStats struct {
	Closed         bool
	RecordsPerFile int64
}
