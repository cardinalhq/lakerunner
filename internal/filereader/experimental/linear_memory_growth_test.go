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
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// fileClosingReader wraps a reader to close the underlying file when the reader is closed
type fileClosingReader struct {
	filereader.Reader
	file io.Closer
}

func (f *fileClosingReader) Close() error {
	err1 := f.Reader.Close()
	err2 := f.file.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// TestLinearMemoryGrowth compares memory growth patterns between 5 and 25 iterations
// to verify if memory leaks scale linearly with the number of operations.
func TestLinearMemoryGrowth(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	readers := []struct {
		name    string
		factory func() (filereader.Reader, error)
	}{
		{"ParquetRaw", func() (filereader.Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			stat, err := file.Stat()
			if err != nil {
				file.Close()
				return nil, err
			}
			reader, err := filereader.NewParquetRawReader(file, stat.Size(), 1000)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
		{"DuckDB", func() (filereader.Reader, error) {
			return NewDuckDBParquetRawReader([]string{testFile}, 1000)
		}},
		{"Arrow", func() (filereader.Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			reader, err := NewArrowCookedReader(context.TODO(), file, 1000)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name+"_LinearityTest", func(t *testing.T) {
			// Test with 5 iterations
			growth5 := measureMemoryGrowthOverIterations(t, reader.factory, 5)

			// Stabilize memory before second test
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			// Test with 25 iterations
			growth25 := measureMemoryGrowthOverIterations(t, reader.factory, 25)

			// Calculate expected vs actual ratios
			expectedRatio := float64(25) / float64(5) // Should be 5.0 for linear growth
			actualRSSRatio := float64(growth25.rssGrowth) / float64(growth5.rssGrowth)
			actualNativeRatio := float64(growth25.nativeGrowth) / float64(growth5.nativeGrowth)

			t.Logf("\n=== %s Linearity Analysis ===", reader.name)
			t.Logf("5 iterations:  RSS=%.2f MB, Native=%.2f MB",
				float64(growth5.rssGrowth)/1024/1024,
				float64(growth5.nativeGrowth)/1024/1024)
			t.Logf("25 iterations: RSS=%.2f MB, Native=%.2f MB",
				float64(growth25.rssGrowth)/1024/1024,
				float64(growth25.nativeGrowth)/1024/1024)
			t.Logf("Expected ratio: %.1fx", expectedRatio)
			t.Logf("Actual RSS ratio: %.2fx", actualRSSRatio)
			t.Logf("Actual Native ratio: %.2fx", actualNativeRatio)

			// Check linearity (allow 20% tolerance due to measurement noise)
			tolerance := 0.2
			if actualRSSRatio < expectedRatio*(1-tolerance) || actualRSSRatio > expectedRatio*(1+tolerance) {
				t.Logf("⚠️  RSS growth is NON-LINEAR (%.2fx vs expected %.1fx)", actualRSSRatio, expectedRatio)
			} else {
				t.Logf("✅ RSS growth is LINEAR (%.2fx ≈ %.1fx)", actualRSSRatio, expectedRatio)
			}

			if actualNativeRatio < expectedRatio*(1-tolerance) || actualNativeRatio > expectedRatio*(1+tolerance) {
				t.Logf("⚠️  Native growth is NON-LINEAR (%.2fx vs expected %.1fx)", actualNativeRatio, expectedRatio)
			} else {
				t.Logf("✅ Native growth is LINEAR (%.2fx ≈ %.1fx)", actualNativeRatio, expectedRatio)
			}
		})
	}
}

type memoryGrowthResult struct {
	rssGrowth    int64
	heapGrowth   int64
	nativeGrowth int64
}

func measureMemoryGrowthOverIterations(t *testing.T, factory func() (filereader.Reader, error), iterations int) memoryGrowthResult {
	// Establish baseline
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	baseline, err := getSystemMemStats()
	if err != nil {
		t.Fatalf("Failed to get baseline memory stats: %v", err)
	}

	// Run iterations
	for i := 0; i < iterations; i++ {
		reader, err := factory()
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}

		// Process one batch
		batch, err := reader.Next(context.TODO())
		if err != nil {
			t.Fatalf("Failed to read batch: %v", err)
		}
		if batch.Len() == 0 {
			t.Fatal("No rows read")
		}

		// Clean up
		reader.Close()
		runtime.GC()
	}

	// Final cleanup
	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	final, err := getSystemMemStats()
	if err != nil {
		t.Fatalf("Failed to get final memory stats: %v", err)
	}

	rssGrowth := final.ProcessRSS - baseline.ProcessRSS
	heapGrowth := int64(final.GoHeapAlloc - baseline.GoHeapAlloc)
	nativeGrowth := rssGrowth - heapGrowth

	return memoryGrowthResult{
		rssGrowth:    rssGrowth,
		heapGrowth:   heapGrowth,
		nativeGrowth: nativeGrowth,
	}
}

// TestMemoryGrowthPerIteration shows the per-iteration memory cost for each reader
func TestMemoryGrowthPerIteration(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	readers := []struct {
		name    string
		factory func() (filereader.Reader, error)
	}{
		{"ParquetRaw", func() (filereader.Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			stat, err := file.Stat()
			if err != nil {
				file.Close()
				return nil, err
			}
			reader, err := filereader.NewParquetRawReader(file, stat.Size(), 1000)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
		{"DuckDB", func() (filereader.Reader, error) {
			return NewDuckDBParquetRawReader([]string{testFile}, 1000)
		}},
		{"Arrow", func() (filereader.Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			reader, err := NewArrowCookedReader(context.TODO(), file, 1000)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
	}

	t.Logf("\n=== Per-Iteration Memory Cost Analysis ===")

	for _, reader := range readers {
		t.Run(reader.name+"_PerIterationCost", func(t *testing.T) {
			iterations := 10
			growth := measureMemoryGrowthOverIterations(t, reader.factory, iterations)

			rssPerIteration := float64(growth.rssGrowth) / float64(iterations) / 1024 / 1024
			nativePerIteration := float64(growth.nativeGrowth) / float64(iterations) / 1024 / 1024

			t.Logf("%s: %.2f MB RSS per iteration (%.2f MB native)",
				reader.name, rssPerIteration, nativePerIteration)
		})
	}
}
