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
	"container/heap"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
)

func init() {
	// Register types for gob encoding/decoding in external merge sort
	gob.Register(map[string]any{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(string(""))
	gob.Register(bool(false))
}

// OrderingEngine handles row ordering for different strategies.
type OrderingEngine interface {
	// Add adds a row to the ordering engine for processing.
	Add(row map[string]any) error

	// Flush processes all buffered rows and calls the writer function with sorted batches.
	// The writer function should handle writing the rows to the final output.
	Flush(ctx context.Context, writer func([]map[string]any) error) error

	// Close cleans up any resources used by the ordering engine.
	Close() error
}

// PassthroughOrderer doesn't do any ordering - just passes rows through immediately.
type PassthroughOrderer struct {
	pendingRows []map[string]any
}

// NewPassthroughOrderer creates an ordering engine that doesn't change row order.
func NewPassthroughOrderer() *PassthroughOrderer {
	return &PassthroughOrderer{
		pendingRows: make([]map[string]any, 0),
	}
}

// Add buffers the row to be written on the next flush.
func (o *PassthroughOrderer) Add(row map[string]any) error {
	// Copy the row to avoid issues with reused maps
	rowCopy := make(map[string]any, len(row))
	for k, v := range row {
		rowCopy[k] = v
	}
	o.pendingRows = append(o.pendingRows, rowCopy)
	return nil
}

// Flush writes all pending rows in the order they were added.
func (o *PassthroughOrderer) Flush(ctx context.Context, writer func([]map[string]any) error) error {
	if len(o.pendingRows) == 0 {
		return nil
	}
	if err := writer(o.pendingRows); err != nil {
		return err
	}
	o.pendingRows = o.pendingRows[:0] // Clear but keep capacity
	return nil
}

// Close cleans up resources.
func (o *PassthroughOrderer) Close() error {
	o.pendingRows = nil
	return nil
}

// InMemoryOrderer buffers rows in memory and sorts them before writing.
type InMemoryOrderer struct {
	buffer    []map[string]any
	keyFunc   func(map[string]any) any
	maxBuffer int
}

// NewInMemoryOrderer creates an ordering engine that sorts rows in memory.
func NewInMemoryOrderer(keyFunc func(map[string]any) any, maxBuffer int) *InMemoryOrderer {
	if maxBuffer <= 0 {
		maxBuffer = 50000 // Reasonable default
	}
	return &InMemoryOrderer{
		buffer:    make([]map[string]any, 0, maxBuffer),
		keyFunc:   keyFunc,
		maxBuffer: maxBuffer,
	}
}

// Add adds a row to the in-memory buffer.
func (o *InMemoryOrderer) Add(row map[string]any) error {
	if len(o.buffer) >= o.maxBuffer {
		return fmt.Errorf("in-memory orderer buffer full (max %d rows)", o.maxBuffer)
	}

	// Copy the row to avoid issues with reused maps
	rowCopy := make(map[string]any, len(row))
	for k, v := range row {
		rowCopy[k] = v
	}
	o.buffer = append(o.buffer, rowCopy)
	return nil
}

// Flush sorts all buffered rows and writes them.
func (o *InMemoryOrderer) Flush(ctx context.Context, writer func([]map[string]any) error) error {
	if len(o.buffer) == 0 {
		return nil
	}

	// Sort the buffer using the key function
	slices.SortFunc(o.buffer, func(a, b map[string]any) int {
		keyA := o.keyFunc(a)
		keyB := o.keyFunc(b)

		// Generic comparison for comparable types
		switch ka := keyA.(type) {
		case int64:
			kb := keyB.(int64)
			if ka < kb {
				return -1
			} else if ka > kb {
				return 1
			}
			return 0
		case string:
			kb := keyB.(string)
			if ka < kb {
				return -1
			} else if ka > kb {
				return 1
			}
			return 0
		case float64:
			kb := keyB.(float64)
			if ka < kb {
				return -1
			} else if ka > kb {
				return 1
			}
			return 0
		default:
			// Fallback to string comparison for unknown types
			sa := fmt.Sprintf("%v", keyA)
			sb := fmt.Sprintf("%v", keyB)
			if sa < sb {
				return -1
			} else if sa > sb {
				return 1
			}
			return 0
		}
	})

	if err := writer(o.buffer); err != nil {
		return err
	}

	o.buffer = o.buffer[:0] // Clear but keep capacity
	return nil
}

// Close cleans up resources.
func (o *InMemoryOrderer) Close() error {
	o.buffer = nil
	return nil
}

// ExternalMergeOrderer uses external merge sort for large datasets.
type ExternalMergeOrderer struct {
	tmpDir     string
	chunks     []string
	keyFunc    func(map[string]any) any
	buffer     []map[string]any
	bufferSize int
	chunkCount int
}

// NewExternalMergeOrderer creates an ordering engine that uses external merge sort.
func NewExternalMergeOrderer(tmpDir string, keyFunc func(map[string]any) any, bufferSize int) *ExternalMergeOrderer {
	if bufferSize <= 0 {
		bufferSize = 10000 // Reasonable default
	}
	return &ExternalMergeOrderer{
		tmpDir:     tmpDir,
		keyFunc:    keyFunc,
		buffer:     make([]map[string]any, 0, bufferSize),
		bufferSize: bufferSize,
		chunks:     make([]string, 0),
	}
}

// Add adds a row to the current buffer, flushing to disk if buffer is full.
func (o *ExternalMergeOrderer) Add(row map[string]any) error {
	// Copy the row to avoid issues with reused maps
	rowCopy := make(map[string]any, len(row))
	for k, v := range row {
		rowCopy[k] = v
	}

	o.buffer = append(o.buffer, rowCopy)

	if len(o.buffer) >= o.bufferSize {
		return o.flushBuffer()
	}
	return nil
}

// flushBuffer sorts the current buffer and writes it to a temporary file.
func (o *ExternalMergeOrderer) flushBuffer() error {
	if len(o.buffer) == 0 {
		return nil
	}

	// Sort the buffer
	slices.SortFunc(o.buffer, func(a, b map[string]any) int {
		keyA := o.keyFunc(a)
		keyB := o.keyFunc(b)

		// Same comparison logic as InMemoryOrderer
		switch ka := keyA.(type) {
		case int64:
			kb := keyB.(int64)
			if ka < kb {
				return -1
			} else if ka > kb {
				return 1
			}
			return 0
		case string:
			kb := keyB.(string)
			if ka < kb {
				return -1
			} else if ka > kb {
				return 1
			}
			return 0
		case float64:
			kb := keyB.(float64)
			if ka < kb {
				return -1
			} else if ka > kb {
				return 1
			}
			return 0
		default:
			sa := fmt.Sprintf("%v", keyA)
			sb := fmt.Sprintf("%v", keyB)
			if sa < sb {
				return -1
			} else if sa > sb {
				return 1
			}
			return 0
		}
	})

	// Write to temporary file
	chunkFile, err := os.CreateTemp(o.tmpDir, fmt.Sprintf("merge-chunk-%d-*.gob", o.chunkCount))
	if err != nil {
		return fmt.Errorf("create chunk file: %w", err)
	}
	defer chunkFile.Close()

	encoder := gob.NewEncoder(chunkFile)
	for _, row := range o.buffer {
		if err := encoder.Encode(row); err != nil {
			os.Remove(chunkFile.Name())
			return fmt.Errorf("encode row to chunk: %w", err)
		}
	}

	o.chunks = append(o.chunks, chunkFile.Name())
	o.chunkCount++
	o.buffer = o.buffer[:0] // Clear but keep capacity

	return nil
}

// Flush merges all chunks and any remaining buffer data, then writes the sorted result.
func (o *ExternalMergeOrderer) Flush(ctx context.Context, writer func([]map[string]any) error) error {
	// Flush any remaining buffer data
	if err := o.flushBuffer(); err != nil {
		return err
	}

	if len(o.chunks) == 0 {
		return nil // Nothing to merge
	}

	// Use a merge heap to efficiently merge sorted chunks
	return o.mergeChunks(ctx, writer)
}

// chunkReader wraps a file and decoder for reading from a chunk.
type chunkReader struct {
	file    *os.File
	decoder *gob.Decoder
	current map[string]any
	key     any
	keyFunc func(map[string]any) any
	closed  bool
}

// next reads the next row from this chunk.
func (cr *chunkReader) next() error {
	if cr.closed {
		return io.EOF
	}

	var row map[string]any
	if err := cr.decoder.Decode(&row); err != nil {
		if errors.Is(err, io.EOF) {
			cr.closed = true
		}
		return err
	}

	cr.current = row
	cr.key = cr.keyFunc(row)
	return nil
}

// mergeChunks uses a heap-based merge to combine all sorted chunks.
func (o *ExternalMergeOrderer) mergeChunks(ctx context.Context, writer func([]map[string]any) error) error {
	// Open all chunk files
	readers := make([]*chunkReader, 0, len(o.chunks))
	defer func() {
		for _, r := range readers {
			if r.file != nil {
				r.file.Close()
				os.Remove(r.file.Name())
			}
		}
	}()

	for _, chunkPath := range o.chunks {
		file, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("open chunk %s: %w", chunkPath, err)
		}

		reader := &chunkReader{
			file:    file,
			decoder: gob.NewDecoder(file),
			keyFunc: o.keyFunc,
		}

		// Read first row from this chunk
		if err := reader.next(); err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("read first row from chunk %s: %w", chunkPath, err)
			}
			// Empty chunk, skip it
			file.Close()
			continue
		}

		readers = append(readers, reader)
	}

	if len(readers) == 0 {
		return nil
	}

	// Use heap to efficiently find the next smallest row across all chunks
	h := &mergeHeap{readers: readers}
	heap.Init(h)

	const batchSize = 1000
	batch := make([]map[string]any, 0, batchSize)

	for h.Len() > 0 {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Get the reader with the smallest current key
		reader := heap.Pop(h).(*chunkReader)
		batch = append(batch, reader.current)

		// Try to read the next row from this reader
		if err := reader.next(); err == nil {
			// Still has data, put it back in the heap
			heap.Push(h, reader)
		} else if !errors.Is(err, io.EOF) {
			return fmt.Errorf("read from chunk: %w", err)
		}
		// If EOF, this reader is done and won't be put back

		// Write batch when full
		if len(batch) >= batchSize {
			if err := writer(batch); err != nil {
				return err
			}
			batch = batch[:0] // Clear but keep capacity
		}
	}

	// Write any remaining rows
	if len(batch) > 0 {
		if err := writer(batch); err != nil {
			return err
		}
	}

	return nil
}

// Close cleans up all temporary files and resources.
func (o *ExternalMergeOrderer) Close() error {
	for _, chunkPath := range o.chunks {
		os.Remove(chunkPath)
	}
	o.chunks = nil
	o.buffer = nil
	return nil
}

// mergeHeap implements heap.Interface for merging sorted chunks.
type mergeHeap struct {
	readers []*chunkReader
}

func (h mergeHeap) Len() int { return len(h.readers) }

func (h mergeHeap) Less(i, j int) bool {
	keyI := h.readers[i].key
	keyJ := h.readers[j].key

	// Same comparison logic as the sorting functions
	switch ki := keyI.(type) {
	case int64:
		kj := keyJ.(int64)
		return ki < kj
	case string:
		kj := keyJ.(string)
		return ki < kj
	case float64:
		kj := keyJ.(float64)
		return ki < kj
	default:
		si := fmt.Sprintf("%v", keyI)
		sj := fmt.Sprintf("%v", keyJ)
		return si < sj
	}
}

func (h mergeHeap) Swap(i, j int) {
	h.readers[i], h.readers[j] = h.readers[j], h.readers[i]
}

func (h *mergeHeap) Push(x any) {
	h.readers = append(h.readers, x.(*chunkReader))
}

func (h *mergeHeap) Pop() any {
	old := h.readers
	n := len(old)
	item := old[n-1]
	h.readers = old[0 : n-1]
	return item
}
