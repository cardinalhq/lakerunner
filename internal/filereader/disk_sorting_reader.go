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

	"github.com/fxamacker/cbor/v2"
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
// for large datasets that don't fit in RAM.
type DiskSortingReader struct {
	reader   Reader
	tempFile *os.File
	encoder  *cbor.Encoder
	decMode  cbor.DecMode // Store decode mode to create fresh decoders
	closed   bool
	rowCount int64

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

	// Configure CBOR encoding to preserve types
	encMode, err := cbor.EncOptions{
		Sort:          cbor.SortNone,          // Don't sort map keys - preserve order
		ShortestFloat: cbor.ShortestFloatNone, // Don't convert float types
		BigIntConvert: cbor.BigIntConvertNone, // Don't convert large integers
		Time:          cbor.TimeUnix,          // Encode times as Unix timestamps
		TimeTag:       cbor.EncTagNone,        // Don't add CBOR time tags
	}.EncMode()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("failed to create CBOR encoder: %w", err)
	}

	// Configure CBOR decoding to preserve types
	decMode, err := cbor.DecOptions{
		BigIntDec: cbor.BigIntDecodeValue,   // Preserve large integers
		IntDec:    cbor.IntDecConvertSigned, // Preserve signed integers
	}.DecMode()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, fmt.Errorf("failed to create CBOR decoder: %w", err)
	}

	return &DiskSortingReader{
		reader:   reader,
		tempFile: tempFile,
		encoder:  encMode.NewEncoder(tempFile),
		decMode:  decMode,
		indices:  make([]RowIndex, 0, 1000), // Start with reasonable capacity
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

		// Create a new decoder for this position to avoid state issues
		decoder := r.decMode.NewDecoder(r.tempFile)

		// Decode CBOR row
		var decodedRow map[string]any
		if err := decoder.Decode(&decodedRow); err != nil {
			return n, fmt.Errorf("failed to decode CBOR row at offset %d: %w", idx.FileOffset, err)
		}

		// Reset and copy to output row, with type conversion if needed
		resetRow(&rows[n])
		for k, v := range decodedRow {
			rows[n][k] = r.convertCBORTypes(v)
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

// convertCBORTypes converts CBOR-decoded values back to expected types.
// CBOR has some limitations in preserving exact Go types, so we fix them here.
func (r *DiskSortingReader) convertCBORTypes(value any) any {
	switch v := value.(type) {
	case []interface{}:
		// Convert []interface{} back to []float64 if all elements are numeric
		if len(v) == 0 {
			return []float64{} // Preserve empty slice type
		}

		// Check if all elements are numbers
		allNumbers := true
		for _, elem := range v {
			switch elem.(type) {
			case float64, int64, uint64, int, uint:
				// These are all numeric types
			default:
				allNumbers = false
			}
		}

		if allNumbers {
			// Convert to []float64
			result := make([]float64, len(v))
			for i, elem := range v {
				switch e := elem.(type) {
				case float64:
					result[i] = e
				case int64:
					result[i] = float64(e)
				case uint64:
					result[i] = float64(e)
				case int:
					result[i] = float64(e)
				case uint:
					result[i] = float64(e)
				}
			}
			return result
		}
		return v // Return as-is if not all numbers
	default:
		return v // Return unchanged for other types
	}
}
