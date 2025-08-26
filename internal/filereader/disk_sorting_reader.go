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
	"strings"

	cbor2 "github.com/fxamacker/cbor/v2"

	"github.com/cardinalhq/lakerunner/internal/cbor"
)

// RowIndex represents a lightweight pointer to a CBOR-encoded row in the temp file.
type RowIndex struct {
	MetricName string
	TID        int64
	Timestamp  int64
	FileOffset int64
	ByteLength int32
}

// DiskSortingReader reads all rows from an underlying reader, CBOR-encodes them to a temp file,
// sorts by index, then returns them in sorted order. This provides memory-efficient sorting
// for large datasets that don't fit in RAM. See the internal/cbor package for CBOR type behavior.
type DiskSortingReader struct {
	reader     Reader
	tempFile   *os.File
	encoder    *cbor2.Encoder
	cborConfig *cbor.Config
	closed     bool
	rowCount   int64

	// Lightweight sorted indices pointing to CBOR data
	indices      []RowIndex
	currentIndex int
	sorted       bool
}

// NewDiskSortingReader creates a reader that uses disk-based sorting with CBOR encoding.
func NewDiskSortingReader(reader Reader) (*DiskSortingReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
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
		reader:     reader,
		tempFile:   tempFile,
		encoder:    cborConfig.NewEncoder(tempFile),
		cborConfig: cborConfig,
		indices:    make([]RowIndex, 0, 1000), // Start with reasonable capacity
	}, nil
}

// writeAndIndexAllRows reads all rows from the underlying reader, encodes them to disk, and builds index.
func (r *DiskSortingReader) writeAndIndexAllRows() error {
	if r.sorted {
		return nil
	}

	// Read all rows from the underlying reader and encode to temp file
	for {
		rows := make([]Row, 100)
		n, err := r.reader.Read(rows)

		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read from underlying reader: %w", err)
		}

		// Encode and index each row
		for i := 0; i < n; i++ {
			if err := r.writeAndIndexRow(rows[i]); err != nil {
				return fmt.Errorf("failed to write and index row: %w", err)
			}
		}

		if err == io.EOF {
			break
		}
	}

	// Sort indices by [metric_name, tid, timestamp]
	slices.SortFunc(r.indices, func(a, b RowIndex) int {
		// First compare by metric name
		if cmp := strings.Compare(a.MetricName, b.MetricName); cmp != 0 {
			return cmp
		}

		// Then by TID
		if a.TID < b.TID {
			return -1
		}
		if a.TID > b.TID {
			return 1
		}

		// Finally by timestamp
		if a.Timestamp < b.Timestamp {
			return -1
		}
		if a.Timestamp > b.Timestamp {
			return 1
		}

		return 0
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

	// Extract sort key components from row
	metricName, nameOk := row["_cardinalhq.name"].(string)
	tid, tidOk := row["_cardinalhq.tid"].(int64)
	timestamp, timestampOk := row["_cardinalhq.timestamp"].(int64)

	if !nameOk || !tidOk || !timestampOk {
		return fmt.Errorf("row missing required sort key fields: name=%v, tid=%v, timestamp=%v", nameOk, tidOk, timestampOk)
	}

	// CBOR encode the row
	startPos := offset
	if err := r.encoder.Encode(row); err != nil {
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

	// Add index entry
	r.indices = append(r.indices, RowIndex{
		MetricName: metricName,
		TID:        tid,
		Timestamp:  timestamp,
		FileOffset: startPos,
		ByteLength: int32(byteLength),
	})

	return nil
}

// Read returns sorted rows by reading from the temp file in index order.
func (r *DiskSortingReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	// Ensure all rows are written to disk and sorted
	if !r.sorted {
		if err := r.writeAndIndexAllRows(); err != nil {
			return 0, err
		}
	}

	// Return rows from disk in sorted order
	n := 0
	for n < len(rows) && r.currentIndex < len(r.indices) {
		idx := r.indices[r.currentIndex]

		// Seek to the row position in temp file
		if _, err := r.tempFile.Seek(idx.FileOffset, io.SeekStart); err != nil {
			return n, fmt.Errorf("failed to seek to row offset %d: %w", idx.FileOffset, err)
		}

		// Read the CBOR bytes
		rowBytes := make([]byte, idx.ByteLength)
		if _, err := r.tempFile.Read(rowBytes); err != nil {
			return n, fmt.Errorf("failed to read CBOR data at offset %d: %w", idx.FileOffset, err)
		}

		// Use shared CBOR package to decode with type conversion
		decodedRow, err := r.cborConfig.Decode(rowBytes)
		if err != nil {
			return n, fmt.Errorf("failed to decode CBOR row at offset %d: %w", idx.FileOffset, err)
		}

		// Reset and copy to output row
		resetRow(&rows[n])
		for k, v := range decodedRow {
			rows[n][k] = v
		}

		n++
		r.currentIndex++
	}

	if n > 0 {
		r.rowCount += int64(n)
	}

	if r.currentIndex >= len(r.indices) && n == 0 {
		return 0, io.EOF
	}

	return n, nil
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

// RowCount returns the number of rows that have been read.
func (r *DiskSortingReader) RowCount() int64 {
	return r.rowCount
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (r *DiskSortingReader) GetOTELMetrics() (any, error) {
	if provider, ok := r.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
