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

package filereader

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// SortFunc is a function that compares two rows and returns:
// -1 if a should come before b
//
//	0 if a and b are equal
//	1 if a should come after b
type SortFunc func(a, b Row) int

// MemorySortingReader reads all rows from an underlying reader,
// then sorts them using a custom sort function and returns them in order.
// This is useful when you need sorted output with flexible sorting criteria.
//
// Memory Impact: HIGH - All rows are loaded into memory at once
// Disk I/O: None (pure in-memory operations)
// Stability: Records are only guaranteed to be sorted by the sort function;
//
//	if the sort function is not stable, the result will not be stable
type MemorySortingReader struct {
	reader    Reader
	sortFunc  SortFunc
	closed    bool
	rowCount  int64
	batchSize int

	// Buffered and sorted rows
	allRows      []Row
	currentIndex int
	sorted       bool
}

// NewMemorySortingReader creates a reader that buffers all rows,
// sorts them using the provided sort function, then returns them in order.
//
// Use this for smaller datasets that fit comfortably in memory.
// For large datasets, consider DiskSortingReader to avoid OOM issues.
func NewMemorySortingReader(reader Reader, sortFunc SortFunc, batchSize int) (*MemorySortingReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if sortFunc == nil {
		return nil, fmt.Errorf("sortFunc cannot be nil")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &MemorySortingReader{
		reader:    reader,
		sortFunc:  sortFunc,
		batchSize: batchSize,
		allRows:   make([]Row, 0, 1000), // Start with reasonable capacity
	}, nil
}

// loadAndSortAllRows reads all rows from the underlying reader and sorts them.
func (r *MemorySortingReader) loadAndSortAllRows() error {
	if r.sorted {
		return nil
	}

	// Read all batches from the underlying reader
	for {
		batch, err := r.reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read from underlying reader: %w", err)
		}

		// Convert and store rows from batch
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			// Deep copy the row since we're retaining it beyond the batch lifetime
			copiedRow := make(Row)
			for k, v := range row {
				copiedRow[k] = v
			}
			r.allRows = append(r.allRows, copiedRow)
		}

		// Return batch to pool since we're done with it
		pipeline.ReturnBatch(batch)
	}

	// Sort using the provided sort function
	slices.SortFunc(r.allRows, r.sortFunc)

	r.sorted = true
	return nil
}

// Next returns the next batch of sorted rows from the buffer.
func (r *MemorySortingReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	// Ensure all rows are loaded and sorted
	if !r.sorted {
		if err := r.loadAndSortAllRows(); err != nil {
			return nil, err
		}
	}

	// Check if we've exhausted all rows
	if r.currentIndex >= len(r.allRows) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	// Return rows from the sorted buffer
	for batch.Len() < r.batchSize && r.currentIndex < len(r.allRows) {
		// Use batch's AddRow to reuse maps
		row := batch.AddRow()
		for k, v := range r.allRows[r.currentIndex] {
			row[k] = v
		}
		r.currentIndex++
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		return batch, nil
	}

	return nil, io.EOF
}

// Close closes the reader and underlying reader.
func (r *MemorySortingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release buffer
	r.allRows = nil

	return r.reader.Close()
}

// TotalRowsReturned returns the number of rows that have been returned via Next().
func (r *MemorySortingReader) TotalRowsReturned() int64 {
	return r.rowCount
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (r *MemorySortingReader) GetOTELMetrics() (any, error) {
	if provider, ok := r.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}

// Common sort function constructors

// MetricSortKey represents the sort key for metrics: [name, tid, timestamp]
type MetricSortKey struct {
	Name      string
	Tid       int64
	Timestamp int64
	NameOk    bool
	TidOk     bool
	TsOk      bool
}

// MetricNameTidTimestampSortKeyFunc extracts metric sort keys from rows.
func MetricNameTidTimestampSortKeyFunc() SortKeyFunc {
	return func(row Row) any {
		key := MetricSortKey{}
		key.Name, key.NameOk = row[wkk.RowKeyCName].(string)
		key.Tid, key.TidOk = row[wkk.RowKeyCTID].(int64)
		key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
		return key
	}
}

// MetricNameTidTimestampSortFunc compares MetricSortKey instances.
func MetricNameTidTimestampSortFunc() func(a, b any) int {
	return func(a, b any) int {
		keyA := a.(MetricSortKey)
		keyB := b.(MetricSortKey)

		// Compare name field
		if !keyA.NameOk || !keyB.NameOk {
			if !keyA.NameOk && !keyB.NameOk {
				return 0
			}
			if !keyA.NameOk {
				return 1
			}
			return -1
		}
		if cmp := strings.Compare(keyA.Name, keyB.Name); cmp != 0 {
			return cmp
		}

		// Compare TID field
		if !keyA.TidOk || !keyB.TidOk {
			if !keyA.TidOk && !keyB.TidOk {
				return 0
			}
			if !keyA.TidOk {
				return 1
			}
			return -1
		}
		if keyA.Tid < keyB.Tid {
			return -1
		}
		if keyA.Tid > keyB.Tid {
			return 1
		}

		// Compare timestamp field
		if !keyA.TsOk || !keyB.TsOk {
			if !keyA.TsOk && !keyB.TsOk {
				return 0
			}
			if !keyA.TsOk {
				return 1
			}
			return -1
		}
		if keyA.Timestamp < keyB.Timestamp {
			return -1
		}
		if keyA.Timestamp > keyB.Timestamp {
			return 1
		}

		return 0
	}
}

// MetricNameTidTimestampSort creates a sort function that sorts by [metric_name, tid, timestamp].
// This is the most common sort pattern for metrics processing.
//
// Note: This function handles missing fields by sorting them before valid entries,
// but the overall sort is not stable for records with identical sort keys.
func MetricNameTidTimestampSort() SortFunc {
	return func(a, b Row) int {
		// Get metric name
		nameA, nameAOk := a[wkk.RowKeyCName].(string)
		nameB, nameBOk := b[wkk.RowKeyCName].(string)
		if !nameAOk || !nameBOk {
			if !nameAOk && !nameBOk {
				return 0
			}
			if !nameAOk {
				return 1
			}
			return -1
		}

		// Compare metric names first
		if cmp := strings.Compare(nameA, nameB); cmp != 0 {
			return cmp
		}

		// Get TID
		tidA, tidAOk := a[wkk.RowKeyCTID].(int64)
		tidB, tidBOk := b[wkk.RowKeyCTID].(int64)
		if !tidAOk || !tidBOk {
			if !tidAOk && !tidBOk {
				return 0
			}
			if !tidAOk {
				return 1
			}
			return -1
		}

		// Compare TIDs
		if tidA < tidB {
			return -1
		}
		if tidA > tidB {
			return 1
		}

		// Get timestamp
		tsA, tsAOk := a[wkk.RowKeyCTimestamp].(int64)
		tsB, tsBOk := b[wkk.RowKeyCTimestamp].(int64)
		if !tsAOk || !tsBOk {
			if !tsAOk && !tsBOk {
				return 0
			}
			if !tsAOk {
				return 1
			}
			return -1
		}

		// Compare timestamps
		if tsA < tsB {
			return -1
		}
		if tsA > tsB {
			return 1
		}

		return 0
	}
}

// TimestampSortKey represents timestamp-only sort key
type TimestampSortKey struct {
	Timestamp int64
	TsOk      bool
}

// TimestampSortKeyFunc extracts timestamp sort keys from rows.
func TimestampSortKeyFunc() SortKeyFunc {
	return func(row Row) any {
		key := TimestampSortKey{}
		key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
		return key
	}
}

// TimestampSortFunc compares TimestampSortKey instances.
func TimestampSortFunc() func(a, b any) int {
	return func(a, b any) int {
		keyA := a.(TimestampSortKey)
		keyB := b.(TimestampSortKey)

		if !keyA.TsOk || !keyB.TsOk {
			if !keyA.TsOk && !keyB.TsOk {
				return 0
			}
			if !keyA.TsOk {
				return 1
			}
			return -1
		}

		if keyA.Timestamp < keyB.Timestamp {
			return -1
		}
		if keyA.Timestamp > keyB.Timestamp {
			return 1
		}
		return 0
	}
}

// TimestampSort creates a sort function that sorts by timestamp only.
//
// Note: This function handles missing timestamps by sorting them before valid entries,
// but the overall sort is not stable for records with identical timestamps.
func TimestampSort() SortFunc {
	return func(a, b Row) int {
		tsA, tsAOk := a[wkk.RowKeyCTimestamp].(int64)
		tsB, tsBOk := b[wkk.RowKeyCTimestamp].(int64)
		if !tsAOk || !tsBOk {
			if !tsAOk && !tsBOk {
				return 0
			}
			if !tsAOk {
				return 1
			}
			return -1
		}

		if tsA < tsB {
			return -1
		}
		if tsA > tsB {
			return 1
		}
		return 0
	}
}
