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
	"context"
	"io"
	"sync"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// MetricFilteringReader wraps another reader and filters rows by metric name.
// It assumes the underlying reader provides data sorted by metric name.
// Once it sees the target metric, it returns those rows until the metric changes.
type MetricFilteringReader struct {
	source     Reader
	metricName string

	// State management
	mu           sync.Mutex
	foundMetric  bool
	passedMetric bool
	err          error
	closed       bool
	closedOnce   sync.Once

	// Buffered batches
	batchChan    chan *Batch
	doneChan     chan struct{}
	rowsReturned int64
}

// NewMetricFilteringReader creates a reader that filters for a specific metric name.
// It starts a goroutine to read ahead from the source.
func NewMetricFilteringReader(source Reader, metricName string) *MetricFilteringReader {
	r := &MetricFilteringReader{
		source:     source,
		metricName: metricName,
		batchChan:  make(chan *Batch, 10), // Buffer up to 10 batches
		doneChan:   make(chan struct{}),
	}

	// Start reading in the background
	go r.readLoop()

	return r
}

// readLoop runs in a goroutine and reads from the source until it finds the metric
func (r *MetricFilteringReader) readLoop() {
	defer close(r.batchChan)

	ctx := context.Background()

	for {
		select {
		case <-r.doneChan:
			return
		default:
		}

		// Check if we're closed
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		// Read a batch from source
		batch, err := r.source.Next(ctx)
		if err == io.EOF {
			// EOF is fine - this file might not have the metric
			// Only set error if we're expecting to find the metric
			return
		}
		if err != nil {
			r.mu.Lock()
			r.err = err
			r.mu.Unlock()
			return
		}

		// Check if we've already passed the metric (can stop early)
		r.mu.Lock()
		if r.passedMetric {
			r.mu.Unlock()
			pipeline.ReturnBatch(batch)
			return
		}
		r.mu.Unlock()

		// Filter the batch for our metric
		filteredBatch := r.filterBatch(batch)

		if filteredBatch != nil && filteredBatch.Len() > 0 {
			// Send the filtered batch to the channel
			select {
			case r.batchChan <- filteredBatch:
			case <-r.doneChan:
				pipeline.ReturnBatch(filteredBatch)
				return
			}
		}

		// Return the original batch if we didn't use it
		if filteredBatch == nil || filteredBatch != batch {
			pipeline.ReturnBatch(batch)
		}
	}
}

// filterBatch filters a batch for rows matching the metric name
func (r *MetricFilteringReader) filterBatch(batch *Batch) *Batch {
	if batch == nil || batch.Len() == 0 {
		return nil
	}

	// Create a new batch for filtered results
	var filteredRows []pipeline.Row
	needNewBatch := false

	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)

		// Get metric name from row
		nameVal, exists := row[wkk.RowKeyCName]
		if !exists {
			continue
		}

		name, ok := nameVal.(string)
		if !ok {
			continue
		}

		// Check if this is our target metric
		if name == r.metricName {
			r.mu.Lock()
			r.foundMetric = true
			r.mu.Unlock()

			if filteredRows == nil {
				filteredRows = make([]pipeline.Row, 0, batch.Len())
			}
			// Copy the row to our filtered batch
			rowCopy := make(pipeline.Row)
			for k, v := range row {
				rowCopy[k] = v
			}
			filteredRows = append(filteredRows, rowCopy)
			needNewBatch = true
		} else if r.foundMetric && name > r.metricName {
			// We've passed our metric (alphabetically), can stop
			r.mu.Lock()
			r.passedMetric = true
			r.mu.Unlock()
			break
		}
	}

	if !needNewBatch {
		return nil
	}

	// Create new batch with filtered rows
	newBatch := pipeline.GetBatch()
	for _, srcRow := range filteredRows {
		dstRow := newBatch.AddRow()
		for k, v := range srcRow {
			dstRow[k] = v
		}
	}

	return newBatch
}

// Next returns the next batch of filtered rows
func (r *MetricFilteringReader) Next(ctx context.Context) (*Batch, error) {
	// Check for any errors first
	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()
		return nil, err
	}
	r.mu.Unlock()

	// Wait for a batch from the reader goroutine
	select {
	case batch, ok := <-r.batchChan:
		if !ok {
			// Channel closed, return EOF (no data is fine)
			return nil, io.EOF
		}

		r.rowsReturned += int64(batch.Len())
		return batch, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close releases resources
func (r *MetricFilteringReader) Close() error {
	var closeErr error

	r.closedOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		// Signal the goroutine to stop
		close(r.doneChan)

		// Wait for goroutine to finish by draining the channel
		for batch := range r.batchChan {
			pipeline.ReturnBatch(batch)
		}

		// Close the source reader
		closeErr = r.source.Close()
	})

	return closeErr
}

// TotalRowsReturned returns the total number of rows returned
func (r *MetricFilteringReader) TotalRowsReturned() int64 {
	return r.rowsReturned
}

// GetSchema delegates to the wrapped reader.
func (r *MetricFilteringReader) GetSchema() *ReaderSchema {
	if r.source != nil {
		return r.source.GetSchema()
	}
	return NewReaderSchema()
}
