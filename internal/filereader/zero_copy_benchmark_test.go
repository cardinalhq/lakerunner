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

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// mockReaderForBenchmark provides controlled data for benchmarking
type mockReaderForBenchmark struct {
	rowsPerBatch int
	totalRows    int
	rowsEmitted  int
	currentBatch *pipeline.Batch
}

func newMockReaderForBenchmark(rowsPerBatch, totalRows int) *mockReaderForBenchmark {
	return &mockReaderForBenchmark{
		rowsPerBatch: rowsPerBatch,
		totalRows:    totalRows,
		rowsEmitted:  0,
	}
}

func (m *mockReaderForBenchmark) Next(ctx context.Context) (*pipeline.Batch, error) {
	if m.rowsEmitted >= m.totalRows {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()
	rowsInThisBatch := m.rowsPerBatch
	if m.rowsEmitted+rowsInThisBatch > m.totalRows {
		rowsInThisBatch = m.totalRows - m.rowsEmitted
	}

	// Create rows with different aggregation keys to test real aggregation
	for i := 0; i < rowsInThisBatch; i++ {
		row := batch.AddRow()
		// Create 100 different metrics, each with 10 rows to aggregate
		metricID := (m.rowsEmitted + i) / 10
		row[wkk.RowKeyCName] = fmt.Sprintf("metric_%d", metricID%100)
		row[wkk.RowKeyCTID] = int64(metricID % 10)
		row[wkk.RowKeyCTimestamp] = int64(1000 * ((m.rowsEmitted + i) / 100))
		row[wkk.RowKeyCMetricType] = "gauge"
		row[wkk.RowKeyRollupSum] = float64(i)
		row[wkk.RowKeyRollupCount] = float64(1)
		row[wkk.RowKeyRollupMin] = float64(i)
		row[wkk.RowKeyRollupMax] = float64(i)
		row[wkk.RowKeyRollupAvg] = float64(i)
		// Add some metadata fields to make rows more realistic
		row[wkk.NewRowKey("service")] = "benchmark-service"
		row[wkk.NewRowKey("host")] = "benchmark-host"
		row[wkk.NewRowKey("environment")] = "benchmark"
	}

	m.rowsEmitted += rowsInThisBatch
	return batch, nil
}

func (m *mockReaderForBenchmark) Close() error {
	if m.currentBatch != nil {
		pipeline.ReturnBatch(m.currentBatch)
		m.currentBatch = nil
	}
	return nil
}

func (m *mockReaderForBenchmark) TotalRowsReturned() int64 {
	return int64(m.rowsEmitted)
}

func BenchmarkAggregatingMetricsReader_ZeroCopy(b *testing.B) {
	// Benchmark with different data sizes
	sizes := []struct {
		name         string
		totalRows    int
		rowsPerBatch int
	}{
		{"Small_1K", 1000, 100},
		{"Medium_10K", 10000, 1000},
		{"Large_100K", 100000, 1000},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create mock reader with data
				mockReader := newMockReaderForBenchmark(size.rowsPerBatch, size.totalRows)

				// Create aggregating reader
				aggReader, err := NewAggregatingMetricsReader(mockReader, 10000, size.rowsPerBatch) // 10s aggregation
				if err != nil {
					b.Fatalf("failed to create aggregating reader: %v", err)
				}

				// Process all data
				totalRows := 0
				for {
					batch, err := aggReader.Next(context.Background())
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatalf("unexpected error: %v", err)
					}
					totalRows += batch.Len()
					pipeline.ReturnBatch(batch)
				}

				aggReader.Close()
			}

			b.ReportMetric(float64(size.totalRows), "rows/op")
		})
	}
}

func BenchmarkBatchOperations(b *testing.B) {
	b.Run("TakeRow", func(b *testing.B) {
		batch := pipeline.GetBatch()
		defer pipeline.ReturnBatch(batch)

		// Add some rows
		for i := 0; i < 100; i++ {
			row := batch.AddRow()
			row[wkk.RowKeyCName] = "test"
			row[wkk.RowKeyCTID] = int64(i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Take a row and return it
			row := batch.TakeRow(i % batch.Len())
			if row != nil {
				pipeline.ReturnPooledRow(row)
			}
		}
	})

	b.Run("ReplaceRow", func(b *testing.B) {
		batch := pipeline.GetBatch()
		defer pipeline.ReturnBatch(batch)

		// Add some rows
		for i := 0; i < 100; i++ {
			row := batch.AddRow()
			row[wkk.RowKeyCName] = "test"
			row[wkk.RowKeyCTID] = int64(i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Get a new row and replace
			newRow := pipeline.GetPooledRow()
			newRow[wkk.RowKeyCName] = "replaced"
			batch.ReplaceRow(i%batch.Len(), newRow)
		}
	})
}
