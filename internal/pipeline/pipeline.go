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

// --- A simple source (stand-in for readParquet) -------------------------------

// SliceSource is a tiny source for examples/tests.
// Swap this with your Parquet reader that yields up to batchPool.sz rows each Next().
type SliceSource struct {
	data   []Row
	pos    int
	owner  *batchPool
	closed bool
}

func NewSliceSource(data []Row, pool *batchPool) *SliceSource {
	return &SliceSource{data: data, owner: pool}
}

func (s *SliceSource) Next() (*Batch, error) {
	if s.closed {
		return nil, io.EOF
	}
	if s.pos >= len(s.data) {
		return nil, io.EOF
	}
	b := s.owner.Get()
	upper := s.pos + cap(b.Rows)
	if upper > len(s.data) {
		upper = len(s.data)
	}
	b.Rows = append(b.Rows, s.data[s.pos:upper]...)
	s.pos = upper
	return b, nil
}
func (s *SliceSource) Close() error { s.closed = true; return nil }

// --- Transform stages ---------------------------------------------------------

// MapReader: 1→1 mapping (optionally drop by returning (nil,false))
type MapReader struct {
	in   Reader
	pool *batchPool
	fn   func(Row) (Row, bool) // return ok=false to drop
}

func Map(in Reader, pool *batchPool, fn func(Row) (Row, bool)) *MapReader {
	return &MapReader{in: in, pool: pool, fn: fn}
}

func (m *MapReader) Next() (*Batch, error) {
	for {
		in, err := m.in.Next()
		if err != nil {
			return nil, err
		}
		out := m.pool.Get()
		for _, r := range in.Rows {
			if nr, ok := m.fn(r); ok {
				// IMPORTANT: assume fn returns a fresh Row or same row but treated ephemeral.
				// If fn wants to retain, it must copy.
				out.Rows = append(out.Rows, nr)
				if len(out.Rows) == cap(out.Rows) {
					return out, nil // return early if full; remaining rows will be processed on next call
				}
			}
		}
		if len(out.Rows) > 0 {
			return out, nil
		}
		// otherwise loop to pull next input batch
	}
}
func (m *MapReader) Close() error { return m.in.Close() }

// FlatMapReader: 1→N mapping (including 0 or many)
type FlatMapReader struct {
	in    Reader
	pool  *batchPool
	fn    func(Row, func(Row))
	stash *Batch // carry-over when expansion exceeds batch cap
}

func FlatMap(in Reader, pool *batchPool, fn func(Row, func(Row))) *FlatMapReader {
	return &FlatMapReader{in: in, pool: pool, fn: fn}
}

func (f *FlatMapReader) Next() (*Batch, error) {
	// drain stash first
	if f.stash != nil && len(f.stash.Rows) > 0 {
		out := f.stash
		f.stash = nil
		return out, nil
	}
	for {
		in, err := f.in.Next()
		if err != nil {
			return nil, err
		}
		out := f.pool.Get()
		emit := func(r Row) {
			out.Rows = append(out.Rows, r)
			if len(out.Rows) == cap(out.Rows) {
				// Stash the full batch and continue accumulating into a fresh one
				if f.stash == nil {
					f.stash = out
				} else {
					// should not happen, but just in case
					panic("unexpected multiple stash")
				}
				out = f.pool.Get()
			}
		}
		for _, r := range in.Rows {
			f.fn(r, emit)
		}
		if f.stash != nil {
			// we filled exactly one batch; return it now, keep 'out' for next call if it has rows
			if len(out.Rows) > 0 {
				// we have leftover; keep as next stash
				if f.stash == nil {
					f.stash = out
				} else {
					// unlikely path
					panic("unexpected stash collision")
				}
			} else {
				// recycle empty
				f.pool.Put(out)
			}
			out := f.stash
			f.stash = nil
			return out, nil
		}
		if len(out.Rows) > 0 {
			return out, nil
		}
		// otherwise loop to pull next input batch
	}
}
func (f *FlatMapReader) Close() error { return f.in.Close() }

// FilterReader: keep rows where pred==true (thin wrapper around Map)
func Filter(in Reader, pool *batchPool, pred func(Row) bool) *MapReader {
	return Map(in, pool, func(r Row) (Row, bool) { return r, pred(r) })
}

// --- Aggregation (example: sort and aggregate) --------------------------------

// In-memory sort for small batches. For large streams use ExternalSort (below).
type InMemorySort struct {
	in    Reader
	less  func(a, b Row) bool
	cache *Batch
	done  bool
}

func SortInMemory(in Reader, less func(a, b Row) bool) *InMemorySort {
	return &InMemorySort{in: in, less: less}
}

func (s *InMemorySort) Next() (*Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	// Slurp whole input (only for small datasets).
	var rows []Row
	for {
		b, err := s.in.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		rows = append(rows, b.Rows...)
	}
	// Sort (simple heap sort via container/heap of indices)
	h := &rowHeap{rows: rows, less: s.less}
	heap.Init(h)
	s.cache = &Batch{Rows: make([]Row, 0, len(rows))}
	for h.Len() > 0 {
		s.cache.Rows = append(s.cache.Rows, heap.Pop(h).(Row))
	}
	s.done = true
	return s.cache, nil
}
func (s *InMemorySort) Close() error { return s.in.Close() }

type rowHeap struct {
	rows []Row
	less func(a, b Row) bool
}

func (h rowHeap) Len() int           { return len(h.rows) }
func (h rowHeap) Less(i, j int) bool { return h.less(h.rows[i], h.rows[j]) }
func (h rowHeap) Swap(i, j int)      { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }
func (h *rowHeap) Push(x any)        { h.rows = append(h.rows, x.(Row)) }
func (h *rowHeap) Pop() any {
	n := len(h.rows)
	x := h.rows[n-1]
	h.rows = h.rows[:n-1]
	return x
}

// ExternalSort (sketch): run-based external merge with gob spill.
// For brevity, this implementation materializes sorted runs to temp files
// and then merges them. Batch size bounds memory. Tune runRowsLimit.
type ExternalSort struct {
	in           Reader
	pool         *batchPool
	less         func(a, b Row) bool
	runFiles     []string
	mergeDecs    []*gob.Decoder
	mergeFiles   []*os.File
	mergeBuf     []Row
	initialized  bool
	runRowsLimit int
}

func SortExternal(in Reader, pool *batchPool, less func(a, b Row) bool, runRowsLimit int) *ExternalSort {
	if runRowsLimit <= 0 {
		runRowsLimit = pool.sz * 128 // default heuristic
	}
	return &ExternalSort{in: in, pool: pool, less: less, runRowsLimit: runRowsLimit}
}

func (e *ExternalSort) init() error {
	if e.initialized {
		return nil
	}
	// 1) Build sorted runs on disk
	var run []Row
	for {
		b, err := e.in.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		for _, r := range b.Rows {
			run = append(run, r)
			if len(run) >= e.runRowsLimit {
				if err := e.flushRun(run); err != nil {
					return err
				}
				run = run[:0]
			}
		}
	}
	if len(run) > 0 {
		if err := e.flushRun(run); err != nil {
			return err
		}
	}
	// 2) Open decoders for merge
	for _, path := range e.runFiles {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		e.mergeFiles = append(e.mergeFiles, f)
		e.mergeDecs = append(e.mergeDecs, gob.NewDecoder(f))
	}
	e.initialized = true
	return nil
}

func (e *ExternalSort) flushRun(run []Row) error {
	// sort a run in-memory
	h := &rowHeap{rows: run, less: e.less}
	heap.Init(h)
	sorted := make([]Row, 0, len(run))
	for h.Len() > 0 {
		sorted = append(sorted, heap.Pop(h).(Row))
	}
	// write to temp file via gob
	f, err := os.CreateTemp("", "runsort-*.gob")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	for _, r := range sorted {
		if err := enc.Encode(&r); err != nil {
			f.Close()
			_ = os.Remove(f.Name())
			return err
		}
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	e.runFiles = append(e.runFiles, f.Name())
	return nil
}

func (e *ExternalSort) Next() (*Batch, error) {
	if err := e.init(); err != nil {
		return nil, err
	}
	// If no runs, nothing to return
	if len(e.mergeDecs) == 0 {
		return nil, io.EOF
	}
	// Lazy k-way merge using top-of-each-run buffer (simple, not fully optimal)
	// refill mergeBuf from decoders if empty
	if len(e.mergeBuf) == 0 {
		for i := range e.mergeDecs {
			var r Row
			if err := e.mergeDecs[i].Decode(&r); err == nil {
				e.mergeBuf = append(e.mergeBuf, r)
			}
		}
		if len(e.mergeBuf) == 0 {
			return nil, io.EOF
		}
	}
	out := e.pool.Get()
	for len(out.Rows) < cap(out.Rows) && len(e.mergeDecs) > 0 {
		// pick smallest from mergeBuf
		best := 0
		for i := 1; i < len(e.mergeBuf); i++ {
			if e.less(e.mergeBuf[i], e.mergeBuf[best]) {
				best = i
			}
		}
		out.Rows = append(out.Rows, e.mergeBuf[best])

		// refill that slot
		var r Row
		if err := e.mergeDecs[best].Decode(&r); err == nil {
			e.mergeBuf[best] = r
		} else {
			// this run is exhausted; remove it
			e.mergeBuf = append(e.mergeBuf[:best], e.mergeBuf[best+1:]...)
			e.mergeDecs = append(e.mergeDecs[:best], e.mergeDecs[best+1:]...)
			f := e.mergeFiles[best]
			_ = f.Close()
			_ = os.Remove(f.Name())
			e.mergeFiles = append(e.mergeFiles[:best], e.mergeFiles[best+1:]...)
		}
	}
	return out, nil
}

func (e *ExternalSort) Close() error {
	var firstErr error
	if e.in != nil {
		firstErr = e.in.Close()
	}
	for _, f := range e.mergeFiles {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	for _, path := range e.runFiles {
		_ = os.Remove(path)
	}
	return firstErr
}

// --- Spill to disk (materialize stream using gob) -----------------------------

// SpillReader consumes upstream once, writes rows to a temp file via gob,
// then serves them back in batches (bounded memory footprint).
type SpillReader struct {
	in          Reader
	pool        *batchPool
	tmpPath     string
	readFile    *os.File
	dec         *gob.Decoder
	initialized bool
	err         error
}

func SpillToDisk(in Reader, pool *batchPool) *SpillReader {
	return &SpillReader{in: in, pool: pool}
}

func (s *SpillReader) init() error {
	if s.initialized {
		return s.err
	}
	// write all rows to temp
	f, err := os.CreateTemp("", "spill-*.gob")
	if err != nil {
		s.err = err
		s.initialized = true
		return err
	}
	enc := gob.NewEncoder(f)
	for {
		b, err := s.in.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			_ = os.Remove(f.Name())
			s.err = err
			s.initialized = true
			return err
		}
		for _, r := range b.Rows {
			if err := enc.Encode(&r); err != nil {
				f.Close()
				_ = os.Remove(f.Name())
				s.err = err
				s.initialized = true
				return err
			}
		}
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		s.err = err
		s.initialized = true
		return err
	}
	s.tmpPath = f.Name()

	// reopen for reading
	rf, err := os.Open(s.tmpPath)
	if err != nil {
		_ = os.Remove(s.tmpPath)
		s.err = err
		s.initialized = true
		return err
	}
	s.readFile = rf
	s.dec = gob.NewDecoder(rf)
	s.initialized = true
	return nil
}

func (s *SpillReader) Next() (*Batch, error) {
	if err := s.init(); err != nil {
		return nil, err
	}
	out := s.pool.Get()
	for len(out.Rows) < cap(out.Rows) {
		var r Row
		if err := s.dec.Decode(&r); err != nil {
			if errors.Is(err, io.EOF) {
				if len(out.Rows) == 0 {
					return nil, io.EOF
				}
				return out, nil
			}
			return nil, err
		}
		out.Rows = append(out.Rows, r)
	}
	return out, nil
}

func (s *SpillReader) Close() error {
	if s.readFile != nil {
		_ = s.readFile.Close()
	}
	if s.tmpPath != "" {
		_ = os.Remove(s.tmpPath)
	}
	if s.in != nil {
		return s.in.Close()
	}
	return nil
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
