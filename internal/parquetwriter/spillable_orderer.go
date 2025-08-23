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
	"io"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter/spillers"
)

// SpillableOrderer combines in-memory sorting with automatic spilling to disk
// when memory limits are exceeded. It uses a pluggable Spiller for disk operations.
type SpillableOrderer struct {
	keyFunc   func(map[string]any) any
	maxBuffer int
	tmpDir    string
	spiller   spillers.Spiller

	// Current in-memory buffer
	buffer []map[string]any

	// Spill files created when buffer overflows
	spillFiles []*spillers.SpillFile
}

// NewSpillableOrderer creates an orderer that keeps data in memory until maxBuffer
// is reached, then spills to disk using the provided Spiller.
func NewSpillableOrderer(keyFunc func(map[string]any) any, maxBuffer int, tmpDir string, spiller spillers.Spiller) *SpillableOrderer {
	if maxBuffer <= 0 {
		maxBuffer = 50000 // Reasonable default
	}
	if spiller == nil {
		spiller = spillers.NewGobSpiller() // Default spiller
	}

	return &SpillableOrderer{
		keyFunc:    keyFunc,
		maxBuffer:  maxBuffer,
		tmpDir:     tmpDir,
		spiller:    spiller,
		buffer:     make([]map[string]any, 0, maxBuffer),
		spillFiles: make([]*spillers.SpillFile, 0),
	}
}

// Add adds a row to the orderer, spilling to disk if the buffer is full.
func (o *SpillableOrderer) Add(row map[string]any) error {
	// Copy the row to avoid issues with reused maps
	rowCopy := make(map[string]any, len(row))
	for k, v := range row {
		rowCopy[k] = v
	}

	o.buffer = append(o.buffer, rowCopy)

	// Check if we need to spill
	if len(o.buffer) >= o.maxBuffer {
		return o.spillCurrentBuffer()
	}

	return nil
}

// spillCurrentBuffer sorts the current buffer and writes it to a spill file.
func (o *SpillableOrderer) spillCurrentBuffer() error {
	if len(o.buffer) == 0 {
		return nil
	}

	// Sort the buffer using the same logic as other orderers
	sortRowsByKey(o.buffer, o.keyFunc)

	// Write to spill file
	spillFile, err := o.spiller.WriteSpillFile(o.tmpDir, o.buffer, o.keyFunc)
	if err != nil {
		return fmt.Errorf("write spill file: %w", err)
	}

	o.spillFiles = append(o.spillFiles, spillFile)
	o.buffer = o.buffer[:0] // Clear but keep capacity

	return nil
}

// Flush processes all data (in-memory and spilled) and writes sorted results.
func (o *SpillableOrderer) Flush(ctx context.Context, writer func([]map[string]any) error) error {
	// If we only have in-memory data and no spill files, handle the simple case
	if len(o.spillFiles) == 0 {
		if len(o.buffer) == 0 {
			return nil
		}

		// Sort and write the buffer directly
		sortRowsByKey(o.buffer, o.keyFunc)
		if err := writer(o.buffer); err != nil {
			return err
		}
		o.buffer = o.buffer[:0] // Clear but keep capacity
		return nil
	}

	// We have spill files, so we need to do a merge

	// First, spill any remaining buffer data
	if err := o.spillCurrentBuffer(); err != nil {
		return err
	}

	// Merge all spill files
	return o.mergeSpillFiles(ctx, writer)
}

// mergeSpillFiles uses a heap-based merge to combine all spill files.
func (o *SpillableOrderer) mergeSpillFiles(ctx context.Context, writer func([]map[string]any) error) error {
	if len(o.spillFiles) == 0 {
		return nil
	}

	// Open all spill files for reading
	readers := make([]spillReaderWithKey, 0, len(o.spillFiles))
	defer func() {
		for _, r := range readers {
			r.reader.Close()
		}
	}()

	for _, spillFile := range o.spillFiles {
		reader, err := o.spiller.OpenSpillFile(spillFile, o.keyFunc)
		if err != nil {
			return fmt.Errorf("open spill file %s: %w", spillFile.Path, err)
		}

		// Read the first row from this reader
		row, err := reader.Next()
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("read first row from spill file %s: %w", spillFile.Path, err)
			}
			// Empty spill file, skip it
			reader.Close()
			continue
		}

		readers = append(readers, spillReaderWithKey{
			reader:  reader,
			current: row,
			key:     o.keyFunc(row),
		})
	}

	if len(readers) == 0 {
		return nil // All spill files were empty
	}

	// Use heap to efficiently find the next smallest row across all readers
	h := &spillMergeHeap{readers: readers}
	initSpillHeap(h)

	const batchSize = 1000
	batch := make([]map[string]any, 0, batchSize)

	for h.Len() > 0 {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Get the reader with the smallest current key
		readerWithKey := popSpillHeap(h)
		batch = append(batch, readerWithKey.current)

		// Try to read the next row from this reader
		row, err := readerWithKey.reader.Next()
		if err == nil {
			// Still has data, update and put it back in the heap
			readerWithKey.current = row
			readerWithKey.key = o.keyFunc(row)
			pushSpillHeap(h, readerWithKey)
		} else if err != io.EOF {
			return fmt.Errorf("read from spill file: %w", err)
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

// Close cleans up all spill files and resources.
func (o *SpillableOrderer) Close() error {
	var lastErr error

	// Clean up all spill files
	for _, spillFile := range o.spillFiles {
		if err := o.spiller.CleanupSpillFile(spillFile); err != nil {
			lastErr = err // Keep track of cleanup errors but continue cleaning
		}
	}

	o.spillFiles = nil
	o.buffer = nil

	return lastErr
}

// spillReaderWithKey wraps a SpillReader with its current row and key for heap operations.
type spillReaderWithKey struct {
	reader  spillers.SpillReader
	current map[string]any
	key     any
}

// spillMergeHeap implements a heap for merging sorted spill files.
type spillMergeHeap struct {
	readers []spillReaderWithKey
}

func (h spillMergeHeap) Len() int { return len(h.readers) }

func (h spillMergeHeap) Less(i, j int) bool {
	return compareKeys(h.readers[i].key, h.readers[j].key) < 0
}

func (h spillMergeHeap) Swap(i, j int) {
	h.readers[i], h.readers[j] = h.readers[j], h.readers[i]
}

// Heap operations implemented as functions to avoid importing container/heap
// (keeping the package more self-contained)

func initSpillHeap(h *spillMergeHeap) {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		downSpillHeap(h, i, n)
	}
}

func pushSpillHeap(h *spillMergeHeap, x spillReaderWithKey) {
	h.readers = append(h.readers, x)
	upSpillHeap(h, h.Len()-1)
}

func popSpillHeap(h *spillMergeHeap) spillReaderWithKey {
	n := h.Len() - 1
	h.Swap(0, n)
	downSpillHeap(h, 0, n)
	old := h.readers
	item := old[n]
	h.readers = old[0:n]
	return item
}

func upSpillHeap(h *spillMergeHeap, j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func downSpillHeap(h *spillMergeHeap, i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
