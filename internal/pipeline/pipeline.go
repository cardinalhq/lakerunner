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

// Package pipeline provides a streaming data processing pipeline with memory-safe row ownership.
// This package addresses memory ownership issues by establishing clear ownership semantics:
// batches are owned by the Reader that returns them, and consumers must copy data they wish to retain.
package pipeline

import (
	"maps"
	"sync"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// --- Core types & contracts ---------------------------------------------------

type Row = map[wkk.RowKey]any

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

// Reader is a pull-based iterator over Batches.
// Next returns (nil, io.EOF) when the stream ends.
type Reader interface {
	Next() (*Batch, error)
	Close() error
}

// ---- Memory helpers: pooling & copies ---------------------------------------

type batchPool struct {
	pool sync.Pool
	sz   int
}

func newBatchPool(batchSize int) *batchPool {
	return &batchPool{
		pool: sync.Pool{
			New: func() any {
				// Pre-allocate Row maps to reduce allocations
				rows := make([]Row, batchSize)
				for i := range rows {
					rows[i] = make(Row)
				}
				return &Batch{
					rows:     rows,
					validLen: 0,
				}
			},
		},
		sz: batchSize,
	}
}

func (p *batchPool) Get() *Batch {
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

func (p *batchPool) Put(b *Batch) {
	// Drop oversized batches to avoid unbounded growth
	if cap(b.rows) > p.sz*4 {
		return
	}
	// Keep the Row maps but reset validLen - they'll be cleared on next Get()
	b.validLen = 0
	p.pool.Put(b)
}

func copyRow(in Row) Row {
	out := make(Row, len(in))
	maps.Copy(out, in)
	return out
}

func copyBatch(in *Batch) *Batch {
	out := globalBatchPool.Get()
	for i := 0; i < in.Len(); i++ {
		sourceRow := in.Get(i)
		newRow := out.AddRow()
		maps.Copy(newRow, sourceRow)
	}
	return out
}

// NewBatchPool creates a new batch pool with the given batch size.
// CopyRow creates a deep copy of a row.
func CopyRow(in Row) Row {
	return copyRow(in)
}

// CopyBatch creates a deep copy of a batch.
func CopyBatch(in *Batch) *Batch {
	return copyBatch(in)
}

// ToStringMap converts a Row to map[string]any for compatibility with legacy interfaces.
func ToStringMap(row Row) map[string]any {
	result := make(map[string]any, len(row))
	for key, value := range row {
		result[string(key.Value())] = value
	}
	return result
}

// Global batch pool for memory efficiency across all readers and workers
var globalBatchPool = newBatchPool(1000) // Default batch size

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

// --- Batch manipulation methods ---

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

	row := make(Row)
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
