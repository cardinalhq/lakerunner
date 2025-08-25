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

	"github.com/cardinalhq/lakerunner/internal/parquetwriter/spillers"
)

// UnifiedWriter is the main implementation of ParquetWriter that coordinates
// all the components (ordering, sizing, splitting) to create optimized Parquet files.
type UnifiedWriter struct {
	config   WriterConfig
	orderer  OrderingEngine
	splitter *FileSplitter
	closed   bool
}

// NewUnifiedWriter creates a new unified Parquet writer with the given configuration.
func NewUnifiedWriter(config WriterConfig) (*UnifiedWriter, error) {
	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Select and create the appropriate ordering engine
	orderer, err := createOrderingEngine(config)
	if err != nil {
		return nil, fmt.Errorf("create ordering engine: %w", err)
	}

	// Create the file splitter
	splitter := NewFileSplitter(config)

	return &UnifiedWriter{
		config:   config,
		orderer:  orderer,
		splitter: splitter,
	}, nil
}

// createOrderingEngine creates the appropriate ordering engine based on config.
func createOrderingEngine(config WriterConfig) (OrderingEngine, error) {
	switch config.OrderBy {
	case OrderNone, OrderPresumed:
		return NewPassthroughOrderer(), nil

	case OrderInMemory:
		if config.OrderKeyFunc == nil {
			return nil, fmt.Errorf("OrderKeyFunc required for OrderInMemory")
		}
		// Use reasonable default buffer size
		return NewInMemoryOrderer(config.OrderKeyFunc, 50000), nil

	case OrderMergeSort:
		if config.OrderKeyFunc == nil {
			return nil, fmt.Errorf("OrderKeyFunc required for OrderMergeSort")
		}
		// Use reasonable default buffer size
		return NewExternalMergeOrderer(config.TmpDir, config.OrderKeyFunc, 10000), nil

	case OrderSpillable:
		if config.OrderKeyFunc == nil {
			return nil, fmt.Errorf("OrderKeyFunc required for OrderSpillable")
		}
		bufferSize := config.SpillBufferSize
		if bufferSize <= 0 {
			bufferSize = 50000 // Default buffer size
		}
		spiller := config.Spiller
		if spiller == nil {
			spiller = spillers.NewGobSpiller() // Default spiller
		}
		return NewSpillableOrderer(config.OrderKeyFunc, bufferSize, config.TmpDir, spiller), nil

	default:
		return nil, fmt.Errorf("unknown ordering strategy: %v", config.OrderBy)
	}
}

// Write adds a row to the writer. The row will be processed according to the
// writer's ordering strategy before being written to output files.
func (w *UnifiedWriter) Write(row map[string]any) error {
	if w.closed {
		return ErrWriterClosed
	}

	if row == nil {
		return fmt.Errorf("%w: row cannot be nil", ErrInvalidRow)
	}

	// Add row to the ordering engine
	if err := w.orderer.Add(row); err != nil {
		return fmt.Errorf("add row to orderer: %w", err)
	}

	return nil
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
		if w.orderer != nil {
			w.orderer.Close()
		}
		if w.splitter != nil {
			w.splitter.Abort()
		}
	}()

	// Flush all rows from the orderer to the splitter
	err := w.orderer.Flush(ctx, func(rows []map[string]any) error {
		for _, row := range rows {
			if err := w.splitter.WriteRow(ctx, row); err != nil {
				return err
			}

			// Check for context cancellation periodically
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("flush orderer: %w", err)
	}

	// Close orderer to free resources
	if err := w.orderer.Close(); err != nil {
		return nil, fmt.Errorf("close orderer: %w", err)
	}
	w.orderer = nil

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

	if w.orderer != nil {
		w.orderer.Close()
		w.orderer = nil
	}

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

	stats.BytesPerRecord = w.config.BytesPerRecord

	return stats
}

// WriterStats contains current statistics about a writer's state.
type WriterStats struct {
	Closed         bool
	BytesPerRecord float64
}
