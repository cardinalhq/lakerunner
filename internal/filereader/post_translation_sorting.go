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
)

// PostTranslationSortingReader reads all rows from an underlying reader,
// then sorts them and returns them in order. This is useful when you need
// sorted output but the sorting key is only available after translation.
type PostTranslationSortingReader struct {
	reader   Reader
	closed   bool
	rowCount int64

	// Buffered and sorted rows
	allRows      []map[string]any
	currentIndex int
	sorted       bool
}

// NewPostTranslationSortingReader creates a reader that buffers all rows,
// sorts them by [metric_name, tid, timestamp], then returns them in order.
func NewPostTranslationSortingReader(reader Reader) (*PostTranslationSortingReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	return &PostTranslationSortingReader{
		reader:  reader,
		allRows: make([]map[string]any, 0, 1000), // Start with reasonable capacity
	}, nil
}

// loadAndSortAllRows reads all rows from the underlying reader and sorts them.
func (r *PostTranslationSortingReader) loadAndSortAllRows() error {
	if r.sorted {
		return nil
	}

	// Read all rows from the underlying reader
	for {
		rows := make([]Row, 100)
		n, err := r.reader.Read(rows)

		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read from underlying reader: %w", err)
		}

		// Convert and store rows
		for i := 0; i < n; i++ {
			rowMap := make(map[string]any)
			for k, v := range rows[i] {
				rowMap[k] = v
			}
			r.allRows = append(r.allRows, rowMap)
		}

		if err == io.EOF {
			break
		}
	}

	// Sort by [metric_name, tid, timestamp]
	slices.SortFunc(r.allRows, func(a, b map[string]any) int {
		// Get metric name
		nameA, nameAOk := a["_cardinalhq.name"].(string)
		nameB, nameBOk := b["_cardinalhq.name"].(string)
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
		tidA, tidAOk := a["_cardinalhq.tid"].(int64)
		tidB, tidBOk := b["_cardinalhq.tid"].(int64)
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
		tsA, tsAOk := a["_cardinalhq.timestamp"].(int64)
		tsB, tsBOk := b["_cardinalhq.timestamp"].(int64)
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
	})

	r.sorted = true
	return nil
}

// Read returns sorted rows from the buffer.
func (r *PostTranslationSortingReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	// Ensure all rows are loaded and sorted
	if !r.sorted {
		if err := r.loadAndSortAllRows(); err != nil {
			return 0, err
		}
	}

	// Return rows from the sorted buffer
	n := 0
	for n < len(rows) && r.currentIndex < len(r.allRows) {
		resetRow(&rows[n])

		// Copy from buffer
		for k, v := range r.allRows[r.currentIndex] {
			rows[n][k] = v
		}

		n++
		r.currentIndex++
	}

	if n > 0 {
		r.rowCount += int64(n)
	}

	if r.currentIndex >= len(r.allRows) && n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

// Close closes the reader and underlying reader.
func (r *PostTranslationSortingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release buffer
	r.allRows = nil

	return r.reader.Close()
}

// RowCount returns the number of rows that have been read.
func (r *PostTranslationSortingReader) RowCount() int64 {
	return r.rowCount
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (r *PostTranslationSortingReader) GetOTELMetrics() (any, error) {
	if provider, ok := r.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
