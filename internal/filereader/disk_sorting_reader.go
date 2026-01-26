// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/rowcodec"
)

// RowIndex represents a lightweight pointer to a binary-encoded row in the temp file.
// It stores only the extracted sort key plus file location info.
type RowIndex struct {
	SortKey    SortKey // Extracted sort key for sorting
	FileOffset int64
	ByteLength int32
}

// DiskSortingReader reads all rows from an underlying reader, encodes them to a temp file,
// sorts by index using a custom sort function, then returns them in sorted order.
// This provides memory-efficient sorting for large datasets that don't fit in RAM.

// Memory Impact: LOW-MODERATE - Only stores extracted sort keys in memory plus file offsets.
//
//	Much more memory-efficient than MemorySortingReader for large datasets.
//
// Disk I/O: 2x data size - Each row written once to temp binary file, then read once during output
// Stability: Records are only guaranteed to be sorted by the sort function;
//
//	if the sort function is not stable, the result will not be stable
type DiskSortingReader struct {
	reader      Reader
	keyProvider SortKeyProvider // Provider to create sort keys
	tempFile    *os.File
	codec       *rowcodec.SpillCodec
	writer      *bufio.Writer
	closed      bool
	rowCount    int64
	batchSize   int
	offset      int64

	lenScratch [4]byte
	payloadBuf bytes.Buffer
	readBuf    []byte

	// Lightweight sorted indices pointing to binary data
	indices      []RowIndex
	currentIndex int
	sorted       bool
}

var _ Reader = (*DiskSortingReader)(nil)

// NewDiskSortingReader creates a reader that uses disk-based sorting with custom binary encoding.
//
// Use this for large datasets that may not fit in memory. The temp file is automatically
// cleaned up when the reader is closed. Custom binary encoding provides efficient storage and
// serialization for the temporary data with no reflection overhead.
//
// The keyProvider creates sort keys from rows to minimize memory usage during sorting.
func NewDiskSortingReader(reader Reader, keyProvider SortKeyProvider, batchSize int) (*DiskSortingReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if keyProvider == nil {
		return nil, fmt.Errorf("keyProvider cannot be nil")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Create temp file for binary data
	tempFile, err := os.CreateTemp("", "*.bin")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	return &DiskSortingReader{
		reader:      reader,
		keyProvider: keyProvider,
		tempFile:    tempFile,
		codec:       rowcodec.NewSpillCodec(),
		writer:      bufio.NewWriterSize(tempFile, 64*1024),
		batchSize:   batchSize,
		indices:     make([]RowIndex, 0, 1000), // Start with reasonable capacity
	}, nil
}

// writeAndIndexAllRows reads all rows from the underlying reader, encodes them to disk, and builds index.
func (r *DiskSortingReader) writeAndIndexAllRows(ctx context.Context) error {
	if r.sorted {
		return nil
	}

	// Read all batches from the underlying reader and encode to temp file
	for {
		batch, err := r.reader.Next(ctx)
		if err != nil {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read from underlying reader: %w", err)
		}

		// Track rows read from underlying reader
		rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "DiskSortingReader"),
		))

		// Encode and index each row in the batch
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if err := r.writeAndIndexRow(row); err != nil {
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("failed to write and index row: %w", err)
			}
		}

		pipeline.ReturnBatch(batch)
	}

	if err := r.writer.Flush(); err != nil {
		return fmt.Errorf("flush spill file: %w", err)
	}

	// Sort indices using the SortKey Compare method
	slices.SortFunc(r.indices, func(a, b RowIndex) int {
		return a.SortKey.Compare(b.SortKey)
	})

	// After sorting is complete, release all sort keys back to their pools
	for i := range r.indices {
		r.indices[i].SortKey.Release()
		r.indices[i].SortKey = nil // Prevent double-release
	}

	r.sorted = true
	return nil
}

// writeAndIndexRow encodes a row to the temp file and adds an index entry.
func (r *DiskSortingReader) writeAndIndexRow(row pipeline.Row) error {
	r.payloadBuf.Reset()
	payloadLen, err := r.codec.EncodeRowTo(&r.payloadBuf, row)
	if err != nil {
		return fmt.Errorf("failed to encode row: %w", err)
	}
	if payloadLen != int32(r.payloadBuf.Len()) {
		return fmt.Errorf("payload length mismatch: expected %d got %d", payloadLen, r.payloadBuf.Len())
	}
	binary.LittleEndian.PutUint32(r.lenScratch[:], uint32(payloadLen))
	if _, err := r.writer.Write(r.lenScratch[:]); err != nil {
		return fmt.Errorf("failed to write length prefix: %w", err)
	}
	if _, err := r.writer.Write(r.payloadBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write encoded payload: %w", err)
	}

	// Extract sort key from row using the provider
	sortKey := r.keyProvider.MakeKey(row)

	// Add index entry with extracted sort key only
	r.indices = append(r.indices, RowIndex{
		SortKey:    sortKey,
		FileOffset: r.offset,
		ByteLength: payloadLen,
	})

	r.offset += int64(4) + int64(payloadLen)

	return nil
}

func (r *DiskSortingReader) growReadBuf(size int32) []byte {
	if int(size) > cap(r.readBuf) {
		r.readBuf = make([]byte, size)
	}
	return r.readBuf[:size]
}

// Next returns the next batch of sorted rows by reading from the temp file in index order.
func (r *DiskSortingReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	// Ensure all rows are written to disk and sorted
	if !r.sorted {
		if err := r.writeAndIndexAllRows(ctx); err != nil {
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
		section := io.NewSectionReader(r.tempFile, idx.FileOffset, int64(4)+int64(idx.ByteLength))
		if _, err := io.ReadFull(section, r.lenScratch[:]); err != nil {
			return nil, fmt.Errorf("failed to read length at offset %d: %w", idx.FileOffset, err)
		}
		expectedLen := binary.LittleEndian.Uint32(r.lenScratch[:])
		if int32(expectedLen) != idx.ByteLength {
			return nil, fmt.Errorf("length mismatch at offset %d: index %d file %d", idx.FileOffset, idx.ByteLength, expectedLen)
		}

		payload := r.growReadBuf(idx.ByteLength)
		if _, err := io.ReadFull(section, payload); err != nil {
			return nil, fmt.Errorf("failed to read encoded data at offset %d: %w", idx.FileOffset, err)
		}

		// Decode directly into the batch row to avoid allocation
		batchRow := batch.AddRow()
		if err := r.codec.DecodeRowFrom(bytes.NewReader(payload), batchRow); err != nil {
			return nil, fmt.Errorf("failed to decode row at offset %d: %w", idx.FileOffset, err)
		}

		r.currentIndex++
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		// Track rows output to downstream
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "DiskSortingReader"),
		))
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
	var flushErr, closeErr, removeErr error
	if r.tempFile != nil {
		fileName := r.tempFile.Name()
		flushErr = r.writer.Flush()
		closeErr = r.tempFile.Close()
		removeErr = os.Remove(fileName)
	}

	// Release any remaining sort keys back to their pools
	for i := range r.indices {
		if r.indices[i].SortKey != nil {
			r.indices[i].SortKey.Release()
		}
	}

	// Release indices
	r.indices = nil

	// Return first error encountered
	if readerErr != nil {
		return readerErr
	}
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

// TotalRowsReturned returns the number of rows that have been returned via Next().
func (r *DiskSortingReader) TotalRowsReturned() int64 {
	return r.rowCount
}

// GetSchema delegates to the wrapped reader.
func (r *DiskSortingReader) GetSchema() *ReaderSchema {
	if r.reader != nil {
		return r.reader.GetSchema()
	}
	return NewReaderSchema()
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (r *DiskSortingReader) GetOTELMetrics() (any, error) {
	if provider, ok := r.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
