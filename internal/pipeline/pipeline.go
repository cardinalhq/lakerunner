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
	"container/heap"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sync"
)

// --- Core types & contracts ---------------------------------------------------

type Row = map[string]any

// Batch is owned by the Reader that returns it.
// Consumers must not hold references after the next Next() call.
// If you must retain, copy rows you need (see copyRow / copyBatch).
type Batch struct {
	Rows []Row
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
				return &Batch{Rows: make([]Row, 0, batchSize)}
			},
		},
		sz: batchSize,
	}
}

func (p *batchPool) Get() *Batch {
	b := p.pool.Get().(*Batch)
	b.Rows = b.Rows[:0]
	return b
}

func (p *batchPool) Put(b *Batch) {
	// Drop oversized batches to avoid unbounded growth
	if cap(b.Rows) > p.sz*4 {
		return
	}
	b.Rows = b.Rows[:0]
	p.pool.Put(b)
}

func copyRow(in Row) Row {
	out := make(Row, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyBatch(in *Batch) *Batch {
	out := &Batch{Rows: make([]Row, len(in.Rows))}
	for i := range in.Rows {
		out.Rows[i] = copyRow(in.Rows[i])
	}
	return out
}


// NewBatchPool creates a new batch pool with the given batch size.
func NewBatchPool(batchSize int) *batchPool {
	return newBatchPool(batchSize)
}

// CopyRow creates a deep copy of a row.
func CopyRow(in Row) Row {
	return copyRow(in)
}

// CopyBatch creates a deep copy of a batch.
func CopyBatch(in *Batch) *Batch {
	return copyBatch(in)
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
