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
	"fmt"
	"io"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func createTestRowsForSorting(numRows int) []Row {
	rows := make([]Row, numRows)

	for i := 0; i < numRows; i++ {
		rows[i] = Row{
			wkk.RowKeyCName:                          fmt.Sprintf("metric_%d", i%10), // 10 different metric names
			wkk.RowKeyCTID:                           int64(100 + i%5),               // 5 different TIDs per metric
			wkk.RowKeyCTimestamp:                     int64(1000 + i*1000),           // Sequential timestamps
			wkk.RowKeyRollupAvg:                      float64(50.0 + float64(i)),
			wkk.RowKeyRollupCount:                    float64(1),
			wkk.RowKeyRollupSum:                      float64(50.0 + float64(i)),
			wkk.RowKeySketch:                         []byte{byte(i % 256)},
			wkk.NewRowKey("_cardinalhq.description"): fmt.Sprintf("Test metric %d", i),
			wkk.NewRowKey("_cardinalhq.unit"):        "percent",
			wkk.NewRowKey("service.name"):            fmt.Sprintf("service_%d", i%3),
			wkk.NewRowKey("host"):                    fmt.Sprintf("host_%d", i%5),
		}
	}

	return rows
}

func benchmarkSortingReader(b *testing.B, createReaderFunc func([]Row) (Reader, error)) {
	// Create test data
	testRows := createTestRowsForSorting(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := createReaderFunc(testRows)
		if err != nil {
			b.Fatalf("Failed to create reader: %v", err)
		}

		// Read all rows
		rowsRead := 0
		for {
			batch, err := reader.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read error: %v", err)
			}
			rowsRead += batch.Len()
		}

		_ = reader.Close()

		// Sanity check
		if i == 0 && rowsRead != 1000 {
			b.Fatalf("Expected 1000 rows, got %d", rowsRead)
		}
	}
}

func BenchmarkMemorySortingReader(b *testing.B) {
	benchmarkSortingReader(b, func(rows []Row) (Reader, error) {
		mockReader := NewMockReader(rows)
		return NewMemorySortingReader(mockReader, &NonPooledMetricSortKeyProvider{}, 1000)
	})
}

func BenchmarkDiskSortingReader(b *testing.B) {
	benchmarkSortingReader(b, func(rows []Row) (Reader, error) {
		mockReader := NewMockReader(rows)
		return NewDiskSortingReader(mockReader, &NonPooledMetricSortKeyProvider{}, 1000)
	})
}

func benchmarkSortingReaderWithSize(b *testing.B, numRows int, createReaderFunc func([]Row) (Reader, error)) {
	// Create test data
	testRows := createTestRowsForSorting(numRows)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := createReaderFunc(testRows)
		if err != nil {
			b.Fatalf("Failed to create reader: %v", err)
		}

		// Read all rows
		rowsRead := 0
		for {
			batch, err := reader.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read error: %v", err)
			}
			rowsRead += batch.Len()
		}

		_ = reader.Close()

		// Sanity check
		if i == 0 && rowsRead != numRows {
			b.Fatalf("Expected %d rows, got %d", numRows, rowsRead)
		}
	}
}

func BenchmarkMemorySortingReader_LargeDataset(b *testing.B) {
	benchmarkSortingReaderWithSize(b, 10000, func(rows []Row) (Reader, error) {
		mockReader := NewMockReader(rows)
		return NewMemorySortingReader(mockReader, &NonPooledMetricSortKeyProvider{}, 1000)
	})
}

func BenchmarkDiskSortingReader_LargeDataset(b *testing.B) {
	benchmarkSortingReaderWithSize(b, 10000, func(rows []Row) (Reader, error) {
		mockReader := NewMockReader(rows)
		return NewDiskSortingReader(mockReader, &NonPooledMetricSortKeyProvider{}, 1000)
	})
}

func BenchmarkMemorySortingReader_VeryLargeDataset(b *testing.B) {
	benchmarkSortingReaderWithSize(b, 100000, func(rows []Row) (Reader, error) {
		mockReader := NewMockReader(rows)
		return NewMemorySortingReader(mockReader, &NonPooledMetricSortKeyProvider{}, 1000)
	})
}

func BenchmarkDiskSortingReader_VeryLargeDataset(b *testing.B) {
	benchmarkSortingReaderWithSize(b, 100000, func(rows []Row) (Reader, error) {
		mockReader := NewMockReader(rows)
		return NewDiskSortingReader(mockReader, &NonPooledMetricSortKeyProvider{}, 1000)
	})
}
