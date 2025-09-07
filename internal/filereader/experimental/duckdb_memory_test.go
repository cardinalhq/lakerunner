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

//go:build experimental && memoryanalysis

package experimental

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// TestDuckDBMemoryPatterns tests different batch sizes to understand
// DuckDB's memory behavior with LIMIT/OFFSET
func TestDuckDBMemoryPatterns(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	batchSizes := []int{10, 50, 100, 500, 1000}

	for _, batchSize := range batchSizes {
		t.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(t *testing.T) {
			// Stabilize memory
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			baseline, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline memory: %v", err)
			}

			// Create batched reader with specific batch size
			reader, err := NewDuckDBParquetBatchedReader([]string{testFile}, batchSize)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}

			// Read first batch only
			batch, err := reader.Next(context.TODO())
			if err != nil {
				reader.Close()
				t.Fatalf("Failed to read first batch: %v", err)
			}

			// Measure memory after first batch
			firstBatch, err := getSystemMemStats()
			if err != nil {
				reader.Close()
				t.Fatalf("Failed to get memory stats: %v", err)
			}

			reader.Close()
			runtime.GC()
			runtime.GC()

			growth := firstBatch.ProcessRSS - baseline.ProcessRSS
			memPerRow := float64(growth) / float64(batch.Len()) / 1024

			t.Logf("Batch size %d: %d rows, %.2f MB growth, %.1f KB per row",
				batchSize, batch.Len(), float64(growth)/1024/1024, memPerRow)
		})
	}
}

// TestDuckDBvsArrowMemoryComparison compares memory efficiency
// of the batched DuckDB vs Arrow reader
func TestDuckDBvsArrowMemoryComparison(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	readers := []struct {
		name    string
		factory func() (filereader.Reader, error)
	}{
		{"Arrow", func() (filereader.Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			reader, err := NewArrowCookedReader(context.TODO(), file, 100)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
		{"DuckDB_Batched", func() (filereader.Reader, error) {
			return NewDuckDBParquetBatchedReader([]string{testFile}, 100)
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name+"_SingleBatch", func(t *testing.T) {
			runtime.GC()
			runtime.GC()
			time.Sleep(50 * time.Millisecond)

			baseline, _ := getSystemMemStats()

			r, err := reader.factory()
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}

			batch, err := r.Next(context.TODO())
			if err != nil {
				r.Close()
				t.Fatalf("Failed to read batch: %v", err)
			}

			afterRead, _ := getSystemMemStats()
			r.Close()

			growth := afterRead.ProcessRSS - baseline.ProcessRSS
			t.Logf("%s: %d rows, %.2f MB RSS growth, %.1f KB per row",
				reader.name,
				batch.Len(),
				float64(growth)/1024/1024,
				float64(growth)/float64(batch.Len())/1024)
		})
	}
}
