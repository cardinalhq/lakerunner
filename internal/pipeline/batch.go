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

package pipeline

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter("github.com/cardinalhq/lakerunner/internal/pipeline")

	bufferpoolGetsCounter metric.Int64Counter
	bufferpoolPutsCounter metric.Int64Counter
)

func init() {
	var err error

	bufferpoolGetsCounter, err = meter.Int64Counter(
		"lakerunner.pipeline.bufferpool.gets",
		metric.WithDescription("Total number of gets from the buffer pool"),
	)
	if err != nil {
		panic(err)
	}

	bufferpoolPutsCounter, err = meter.Int64Counter(
		"lakerunner.pipeline.bufferpool.puts",
		metric.WithDescription("Total number of puts back to the buffer pool"),
	)
	if err != nil {
		panic(err)
	}
}

// Batch is owned by the Reader that returns it.
// Consumers must not hold references after the next Next() call.
// If you must retain, copy rows you need (see copyRow / copyBatch).
//
// The Batch reuses underlying Row maps for memory efficiency. Access rows only
// through the provided methods - never retain references to Row objects returned
// by Get() as they may be reused. Use CopyRow() if you need to retain data.
type Batch struct {
	rows     []Row // Pre-allocated Row maps, some may be cleared/reused (private)
	validLen int   // Number of valid rows (≤ len(rows)) (private)
}

// batchPool provides memory-efficient batch reuse.
type batchPool struct {
	pool  sync.Pool
	sz    int
	alloc atomic.Uint64
	gets  atomic.Uint64
	puts  atomic.Uint64
}

// newBatchPool creates a new batch pool with the given batch size.
func newBatchPool(batchSize int) *batchPool {
	p := &batchPool{sz: batchSize}
	p.pool = sync.Pool{
		New: func() any {
			p.alloc.Add(1)
			rows := make([]Row, batchSize)
			for i := range rows {
				rows[i] = getPooledRow()
			}
			return &Batch{
				rows:     rows,
				validLen: 0,
			}
		},
	}
	return p
}

// Get returns a clean batch from the pool.
func (p *batchPool) Get() *Batch {
	p.gets.Add(1)
	bufferpoolGetsCounter.Add(context.Background(), 1)
	b := p.pool.Get().(*Batch)
	// Clear all Row maps but keep them allocated for reuse
	for i := range b.rows {
		for k := range b.rows[i] {
			delete(b.rows[i], k)
		}
	}
	b.validLen = 0
	return b
}

// Put returns a batch to the pool for reuse.
func (p *batchPool) Put(b *Batch) {
	p.puts.Add(1)
	bufferpoolPutsCounter.Add(context.Background(), 1)
	// Drop oversized batches to avoid unbounded growth
	if cap(b.rows) > p.sz*4 {
		for i := range b.rows {
			if b.rows[i] != nil {
				returnRowToPool(b.rows[i])
			}
		}
		return
	}
	// Keep the Row maps but reset validLen - they'll be cleared on next Get()
	b.validLen = 0
	p.pool.Put(b)
}

// BatchPoolStats contains counters for batch pool usage.
type BatchPoolStats struct {
	Allocations uint64
	Gets        uint64
	Puts        uint64
}

// LeakedBatches returns the number of batches that were gotten but never returned.
func (s BatchPoolStats) LeakedBatches() uint64 {
	return s.Gets - s.Puts
}

func (p *batchPool) stats() BatchPoolStats {
	return BatchPoolStats{
		Allocations: p.alloc.Load(),
		Gets:        p.gets.Load(),
		Puts:        p.puts.Load(),
	}
}

// Global batch pool for memory efficiency across all readers and workers
var globalBatchPool = newBatchPool(1000) // Default batch size

// rowPool provides memory-efficient Row map reuse
var rowPool = sync.Pool{
	New: func() any {
		return make(Row)
	},
}

// GetBatch returns a reusable batch from the global pool.
// The batch is clean and ready to use.
func GetBatch() *Batch {
	return globalBatchPool.Get()
}

// ReturnBatch returns a batch to the global pool for reuse.
// The batch should not be used after calling this function.
func ReturnBatch(batch *Batch) {
	if batch != nil {
		globalBatchPool.Put(batch)
	}
}

// GlobalBatchPoolStats returns usage counters for the global batch pool.
func GlobalBatchPoolStats() BatchPoolStats {
	return globalBatchPool.stats()
}

// CopyBatch creates a deep copy of a batch using lower-level primitives.
func CopyBatch(in *Batch) *Batch {
	return copyBatch(in)
}

// copyBatch creates a deep copy of a batch using maps.Copy primitive.
func copyBatch(in *Batch) *Batch {
	out := globalBatchPool.Get()
	for i := 0; i < in.Len(); i++ {
		sourceRow := in.Get(i)
		newRow := out.AddRow()
		maps.Copy(newRow, sourceRow)
	}
	return out
}

// Len returns the number of valid rows in the batch.
func (b *Batch) Len() int {
	return b.validLen
}

// Get returns the row at the given index. The returned Row must not be retained
// beyond the lifetime of this batch, as it may be reused. Use CopyRow() if you
// need to retain the data.
func (b *Batch) Get(index int) Row {
	if index < 0 || index >= b.validLen {
		return nil // Invalid index
	}
	return b.rows[index]
}

// AddRow adds a new row to the batch, reusing an existing Row map if available.
// Returns the Row map that should be populated. The returned Row must not be
// retained beyond the lifetime of this batch.
func (b *Batch) AddRow() Row {
	if b.validLen < len(b.rows) {
		row := b.rows[b.validLen]
		for k := range row {
			delete(row, k)
		}
		b.validLen++
		return row
	}

	row := getPooledRow()
	b.rows = append(b.rows, row)
	b.validLen++
	return row
}

// DeleteRow marks a row as deleted without losing the underlying map.
// The map is preserved for future reuse.
func (b *Batch) DeleteRow(index int) {
	if index < 0 || index >= b.validLen {
		return // Invalid index
	}

	// Clear the row data but keep the map
	row := b.rows[index]
	for k := range row {
		delete(row, k)
	}

	// Swap with last valid row to maintain contiguous valid rows
	if index < b.validLen-1 {
		b.rows[index], b.rows[b.validLen-1] = b.rows[b.validLen-1], b.rows[index]
	}
	b.validLen--
}

// TakeRow extracts a row from the batch and transfers ownership to the caller.
// The row at the given index is replaced with a fresh row from the pool.
// The caller is responsible for returning the row to the pool when done.
// Returns nil if index is invalid.
func (b *Batch) TakeRow(index int) Row {
	if index < 0 || index >= b.validLen {
		return nil // Invalid index
	}

	// Extract the current row
	extractedRow := b.rows[index]

	// Replace with a fresh row from the pool
	b.rows[index] = getPooledRow()

	// Return the extracted row - caller now owns it
	return extractedRow
}

// ReplaceRow replaces the row at the given index with a new row.
// The old row is returned to the pool and the new row takes its place.
// The batch takes ownership of the new row.
func (b *Batch) ReplaceRow(index int, newRow Row) {
	if index < 0 || index >= b.validLen || newRow == nil {
		return // Invalid index or nil row
	}

	// Return the old row to the pool
	oldRow := b.rows[index]
	returnRowToPool(oldRow)

	// Put the new row in its place
	b.rows[index] = newRow
}

// SwapRows efficiently swaps two rows within a batch without allocations.
// This is useful for readers that need to rearrange data without copying.
// Returns false if either index is invalid.
func (b *Batch) SwapRows(index1, index2 int) bool {
	if index1 < 0 || index1 >= b.validLen || index2 < 0 || index2 >= b.validLen {
		return false // Invalid indices
	}

	// Simple pointer swap
	b.rows[index1], b.rows[index2] = b.rows[index2], b.rows[index1]
	return true
}

// getPooledRow gets a clean row from the pool
func getPooledRow() Row {
	row := rowPool.Get().(Row)
	// Clear any leftover data
	for k := range row {
		delete(row, k)
	}
	return row
}

// returnRowToPool returns a row to the pool after clearing it
func returnRowToPool(row Row) {
	if row == nil {
		return
	}
	// Clear the row before returning to pool
	for k := range row {
		delete(row, k)
	}
	rowPool.Put(row)
}

// GetPooledRow gets a clean Row map from the global pool.
// The caller is responsible for returning it via ReturnPooledRow when done.
func GetPooledRow() Row {
	return getPooledRow()
}

// ReturnPooledRow returns a Row map to the global pool for reuse.
// The row is cleared before being pooled.
func ReturnPooledRow(row Row) {
	returnRowToPool(row)
}
