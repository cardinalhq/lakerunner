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
	"os"
	"slices"

	cbor2 "github.com/fxamacker/cbor/v2"

	"github.com/cardinalhq/lakerunner/internal/cbor"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// RowIndex represents a lightweight pointer to a CBOR-encoded row in the temp file.
// It stores only the extracted sort key plus file location info.
type RowIndex struct {
	SortKey    any // Extracted sort key for sorting
	FileOffset int64
	ByteLength int32
}

// DiskSortingReader reads all rows from an underlying reader, CBOR-encodes them to a temp file,
// sorts by index using a custom sort function, then returns them in sorted order.
// This provides memory-efficient sorting for large datasets that don't fit in RAM.
//
// SortKeyFunc extracts sort keys from rows. Return the key that will be passed to your SortFunc.
// The sort key should contain only the data needed for comparison to minimize memory usage.
type SortKeyFunc func(row Row) any

// Memory Impact: LOW-MODERATE - Only stores extracted sort keys in memory plus file offsets.
//
//	Much more memory-efficient than MemorySortingReader for large datasets.
//
// Disk I/O: 2x data size - Each row written once to temp CBOR file, then read once during output
// Stability: Records are only guaranteed to be sorted by the sort function;
//
//	if the sort function is not stable, the result will not be stable
type DiskSortingReader struct {
	reader      Reader
	sortKeyFunc SortKeyFunc
	sortFunc    func(a, b any) int // Sort function works on extracted keys, not full rows
	tempFile    *os.File
	encoder     *cbor2.Encoder
	cborConfig  *cbor.Config
	closed      bool
	rowCount    int64
	batchSize   int

	// Lightweight sorted indices pointing to CBOR data
	indices      []RowIndex
	currentIndex int
	sorted       bool
}

// NewDiskSortingReader creates a reader that uses disk-based sorting with CBOR encoding.
//
// Use this for large datasets that may not fit in memory. The temp file is automatically
// cleaned up when the reader is closed. CBOR encoding provides compact storage and
// fast serialization for the temporary data.
//
// The sortKeyFunc extracts sort keys from rows to minimize memory usage during sorting.
// The sortFunc compares the extracted keys (not full rows) and should return -1, 0, or 1.
func NewDiskSortingReader(reader Reader, sortKeyFunc SortKeyFunc, sortFunc func(a, b any) int, batchSize int) (*DiskSortingReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if sortKeyFunc == nil {
		return nil, fmt.Errorf("sortKeyFunc cannot be nil")
	}
	if sortFunc == nil {
		return nil, fmt.Errorf("sortFunc cannot be nil")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Create temp file for CBOR data
	tempFile, err := os.CreateTemp("", "lakerunner-sort-*.cbor")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	// Create CBOR config with optimized settings
	cborConfig, err := cbor.NewConfig()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("failed to create CBOR config: %w", err)
	}

	return &DiskSortingReader{
		reader:      reader,
		sortKeyFunc: sortKeyFunc,
		sortFunc:    sortFunc,
		tempFile:    tempFile,
		encoder:     cborConfig.NewEncoder(tempFile),
		cborConfig:  cborConfig,
		batchSize:   batchSize,
		indices:     make([]RowIndex, 0, 1000), // Start with reasonable capacity
	}, nil
}

// writeAndIndexAllRows reads all rows from the underlying reader, encodes them to disk, and builds index.
func (r *DiskSortingReader) writeAndIndexAllRows() error {
	if r.sorted {
		return nil
	}

	// Read all batches from the underlying reader and encode to temp file
	for {
		batch, err := r.reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read from underlying reader: %w", err)
		}

		// Encode and index each row in the batch
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if err := r.writeAndIndexRow(row); err != nil {
				return fmt.Errorf("failed to write and index row: %w", err)
			}
		}
	}

	// Sort indices using the provided sort function
	slices.SortFunc(r.indices, func(a, b RowIndex) int {
		return r.sortFunc(a.SortKey, b.SortKey)
	})

	r.sorted = true
	return nil
}

// writeAndIndexRow CBOR-encodes a row to the temp file and adds an index entry.
func (r *DiskSortingReader) writeAndIndexRow(row Row) error {
	// Get current file position
	offset, err := r.tempFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get file offset: %w", err)
	}

	// CBOR encode the row (convert to string-keyed map for serialization)
	startPos := offset
	stringKeyedRow := pipeline.ToStringMap(row)
	if err := r.encoder.Encode(stringKeyedRow); err != nil {
		return fmt.Errorf("failed to CBOR encode row: %w", err)
	}

	// Get end position to calculate length
	endPos, err := r.tempFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get end offset: %w", err)
	}

	byteLength := endPos - startPos
	if byteLength > int64(^uint32(0)) {
		return fmt.Errorf("encoded row too large: %d bytes", byteLength)
	}

	// Extract sort key from row using the provided function
	sortKey := r.sortKeyFunc(row)

	// Add index entry with extracted sort key only
	r.indices = append(r.indices, RowIndex{
		SortKey:    sortKey,
		FileOffset: startPos,
		ByteLength: int32(byteLength),
	})

	return nil
}

// Next returns the next batch of sorted rows by reading from the temp file in index order.
func (r *DiskSortingReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	// Ensure all rows are written to disk and sorted
	if !r.sorted {
		if err := r.writeAndIndexAllRows(); err != nil {
			return nil, err
		}
	}

	// Check if we've exhausted all indices
	if r.currentIndex >= len(r.indices) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	// Return rows from disk in sorted order
	for batch.Len() < r.batchSize && r.currentIndex < len(r.indices) {
		idx := r.indices[r.currentIndex]

		// Seek to the row position in temp file
		if _, err := r.tempFile.Seek(idx.FileOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to row offset %d: %w", idx.FileOffset, err)
		}

		// Read the CBOR bytes
		rowBytes := make([]byte, idx.ByteLength)
		if _, err := r.tempFile.Read(rowBytes); err != nil {
			return nil, fmt.Errorf("failed to read CBOR data at offset %d: %w", idx.FileOffset, err)
		}

		// Use shared CBOR package to decode with type conversion
		decodedRow, err := r.cborConfig.Decode(rowBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to decode CBOR row at offset %d: %w", idx.FileOffset, err)
		}

		// Create new row and copy decoded data
		row := make(Row)
		for k, v := range decodedRow {
			row[wkk.NewRowKey(k)] = v
		}

		batchRow := batch.AddRow()
		for k, v := range row {
			batchRow[k] = v
		}
		r.currentIndex++
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		return batch, nil
	}

	return nil, io.EOF
}

// Close closes the reader and cleans up temp file.
func (r *DiskSortingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Close underlying reader
	var readerErr error
	if r.reader != nil {
		readerErr = r.reader.Close()
	}

	// Clean up temp file
	var fileErr error
	if r.tempFile != nil {
		fileName := r.tempFile.Name()
		r.tempFile.Close()
		fileErr = os.Remove(fileName)
	}

	// Release indices
	r.indices = nil

	// Return first error encountered
	if readerErr != nil {
		return readerErr
	}
	return fileErr
}

// TotalRowsReturned returns the number of rows that have been returned via Next().
func (r *DiskSortingReader) TotalRowsReturned() int64 {
	return r.rowCount
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (r *DiskSortingReader) GetOTELMetrics() (any, error) {
	if provider, ok := r.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
