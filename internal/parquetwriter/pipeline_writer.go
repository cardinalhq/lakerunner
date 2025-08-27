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
	"io"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// PipelineWriter consumes data from a pipeline.Reader and writes it to Parquet files.
// This eliminates memory ownership issues by establishing clear consumption semantics.
type PipelineWriter interface {
	// ConsumeAll reads all data from the reader and writes to Parquet files.
	// It handles batching, memory management, and resource cleanup automatically.
	ConsumeAll(ctx context.Context, reader pipeline.Reader) ([]Result, error)

	// Abort stops processing and cleans up any temporary files.
	Abort()
}

// UnifiedPipelineWriter is a pipeline-compatible wrapper around UnifiedWriter.
type UnifiedPipelineWriter struct {
	writer *UnifiedWriter
	closed bool
}

// NewUnifiedPipelineWriter creates a pipeline writer from a WriterConfig.
func NewUnifiedPipelineWriter(config WriterConfig) (*UnifiedPipelineWriter, error) {
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		return nil, err
	}

	return &UnifiedPipelineWriter{
		writer: writer,
	}, nil
}

func (u *UnifiedPipelineWriter) ConsumeAll(ctx context.Context, reader pipeline.Reader) ([]Result, error) {
	if u.closed {
		return nil, ErrWriterClosed
	}

	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		batch, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Write all rows in the batch
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if err := u.writer.Write(row); err != nil {
				return nil, err
			}
		}
	}

	// Close and get results
	results, err := u.writer.Close(ctx)
	u.closed = true
	return results, err
}

func (u *UnifiedPipelineWriter) Abort() {
	if !u.closed {
		u.writer.Abort()
		u.closed = true
	}
}

// BatchingPipelineWriter buffers rows in batches before processing.
// This can improve performance when the underlying writer benefits from larger batches.
type BatchingPipelineWriter struct {
	writer    ParquetWriter
	batchSize int
	buffer    []map[string]any
	closed    bool
}

// NewBatchingPipelineWriter creates a writer that batches rows before writing.
func NewBatchingPipelineWriter(writer ParquetWriter, batchSize int) *BatchingPipelineWriter {
	if batchSize <= 0 {
		batchSize = 1000
	}

	return &BatchingPipelineWriter{
		writer:    writer,
		batchSize: batchSize,
		buffer:    make([]map[string]any, 0, batchSize),
	}
}

func (b *BatchingPipelineWriter) ConsumeAll(ctx context.Context, reader pipeline.Reader) ([]Result, error) {
	if b.closed {
		return nil, ErrWriterClosed
	}

	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		batch, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Add rows to buffer
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			b.buffer = append(b.buffer, row)

			// Flush buffer if full
			if len(b.buffer) >= b.batchSize {
				if err := b.flushBuffer(); err != nil {
					return nil, err
				}
			}
		}
	}

	// Flush any remaining buffered rows
	if err := b.flushBuffer(); err != nil {
		return nil, err
	}

	// Close and get results
	results, err := b.writer.Close(ctx)
	b.closed = true
	return results, err
}

func (b *BatchingPipelineWriter) flushBuffer() error {
	for _, row := range b.buffer {
		if err := b.writer.Write(row); err != nil {
			return err
		}
	}
	b.buffer = b.buffer[:0] // Reset buffer
	return nil
}

func (b *BatchingPipelineWriter) Abort() {
	if !b.closed {
		b.writer.Abort()
		b.closed = true
	}
}

// ProcessPipeline is a convenience function that processes a pipeline reader through a writer.
func ProcessPipeline(ctx context.Context, reader pipeline.Reader, writer PipelineWriter) ([]Result, error) {
	return writer.ConsumeAll(ctx, reader)
}

// ProcessPipelineWithTransform applies transformations during processing.
func ProcessPipelineWithTransform(
	ctx context.Context,
	reader pipeline.Reader,
	writer PipelineWriter,
	transform func(pipeline.Row) (pipeline.Row, bool), // return false to drop row
) ([]Result, error) {
	transformReader := pipeline.Map(reader, transform)

	return writer.ConsumeAll(ctx, transformReader)
}

// ProcessPipelineWithFilter applies filtering during processing.
func ProcessPipelineWithFilter(
	ctx context.Context,
	reader pipeline.Reader,
	writer PipelineWriter,
	predicate func(pipeline.Row) bool,
) ([]Result, error) {
	filterReader := pipeline.Filter(reader, predicate)

	return writer.ConsumeAll(ctx, filterReader)
}

// ProcessPipelineWithSort applies sorting during processing.
func ProcessPipelineWithSort(
	ctx context.Context,
	reader pipeline.Reader,
	writer PipelineWriter,
	less func(a, b pipeline.Row) bool,
) ([]Result, error) {
	sortReader := pipeline.SortInMemory(reader, less)

	return writer.ConsumeAll(ctx, sortReader)
}
