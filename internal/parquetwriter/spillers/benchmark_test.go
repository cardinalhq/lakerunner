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

package spillers

import (
	"fmt"
	"os"
	"testing"
)

// Benchmark data generators for different data patterns
func generateMetricsData(count int) []map[string]any {
	rows := make([]map[string]any, count)
	for i := 0; i < count; i++ {
		rows[i] = map[string]any{
			"_cardinalhq.name":      fmt.Sprintf("cpu.utilization.%d", i%100),
			"_cardinalhq.tid":       int64(i % 1000),
			"_cardinalhq.timestamp": int64(1640995200000 + i*60000), // 1-minute intervals
			"value":                 float64(i%100) + 0.5,
			"service.name":          fmt.Sprintf("service-%d", i%10),
			"host.name":             fmt.Sprintf("host-%d", i%50),
		}
	}
	return rows
}

func generateLogsData(count int) []map[string]any {
	rows := make([]map[string]any, count)
	severities := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

	for i := 0; i < count; i++ {
		rows[i] = map[string]any{
			"_cardinalhq.name":      "log_record",
			"_cardinalhq.tid":       int64(i % 500),
			"_cardinalhq.timestamp": int64(1640995200000 + i*1000), // 1-second intervals
			"severity_text":         severities[i%len(severities)],
			"severity_number":       int64(5 + (i%len(severities))*4), // 5,9,13,17,21
			"body":                  fmt.Sprintf("Log message %d with some details", i),
			"service.name":          fmt.Sprintf("app-%d", i%20),
			"trace.id":              fmt.Sprintf("trace-%016x", i),
		}
	}
	return rows
}

func generateMixedTypesData(count int) []map[string]any {
	rows := make([]map[string]any, count)
	for i := 0; i < count; i++ {
		rows[i] = map[string]any{
			"_cardinalhq.name":      "mixed_data",
			"_cardinalhq.tid":       int64(i % 100),
			"_cardinalhq.timestamp": int64(1640995200000 + i*1000),
			"string_field":          fmt.Sprintf("value-%d", i),
			"int64_field":           int64(i * 12345),
			"float64_field":         float64(i) * 3.14159,
			"bool_field":            i%2 == 0,
			"byte_slice":            []byte{byte(i % 256), byte((i + 1) % 256), byte((i + 2) % 256)},
			"float64_slice":         []float64{float64(i), float64(i + 1), float64(i + 2)},
			"string_slice":          []string{fmt.Sprintf("item-%d", i), fmt.Sprintf("item-%d", i+1)},
			"nested_map": map[string]any{
				"inner_field": fmt.Sprintf("inner-%d", i),
				"inner_count": int64(i % 10),
			},
		}
	}
	return rows
}

// Key function for sorting
func benchmarkKeyFunc(row map[string]any) any {
	name := row["_cardinalhq.name"].(string)
	tid := row["_cardinalhq.tid"].(int64)
	timestamp := row["_cardinalhq.timestamp"].(int64)
	return fmt.Sprintf("%s_%d_%d", name, tid, timestamp)
}

// Benchmark GOB spiller write performance
func BenchmarkGobSpiller_Write(b *testing.B) {
	spiller := NewGobSpiller()
	tmpDir := os.TempDir()

	benchmarks := []struct {
		name string
		data []map[string]any
	}{
		{"Metrics_1K", generateMetricsData(1000)},
		{"Metrics_10K", generateMetricsData(10000)},
		{"Logs_1K", generateLogsData(1000)},
		{"Logs_10K", generateLogsData(10000)},
		{"Mixed_1K", generateMixedTypesData(1000)},
		{"Mixed_10K", generateMixedTypesData(10000)},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				spillFile, err := spiller.WriteSpillFile(tmpDir, bm.data, benchmarkKeyFunc)
				if err != nil {
					b.Fatal(err)
				}
				_ = spiller.CleanupSpillFile(spillFile)
			}
		})
	}
}

// Benchmark CBOR spiller write performance
func BenchmarkCborSpiller_Write(b *testing.B) {
	spiller, err := NewCborSpiller()
	if err != nil {
		b.Fatal(err)
	}
	tmpDir := os.TempDir()

	benchmarks := []struct {
		name string
		data []map[string]any
	}{
		{"Metrics_1K", generateMetricsData(1000)},
		{"Metrics_10K", generateMetricsData(10000)},
		{"Logs_1K", generateLogsData(1000)},
		{"Logs_10K", generateLogsData(10000)},
		{"Mixed_1K", generateMixedTypesData(1000)},
		{"Mixed_10K", generateMixedTypesData(10000)},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				spillFile, err := spiller.WriteSpillFile(tmpDir, bm.data, benchmarkKeyFunc)
				if err != nil {
					b.Fatal(err)
				}
				_ = spiller.CleanupSpillFile(spillFile)
			}
		})
	}
}

// Benchmark GOB spiller read performance
func BenchmarkGobSpiller_Read(b *testing.B) {
	spiller := NewGobSpiller()
	tmpDir := os.TempDir()

	benchmarks := []struct {
		name string
		data []map[string]any
	}{
		{"Metrics_1K", generateMetricsData(1000)},
		{"Metrics_10K", generateMetricsData(10000)},
		{"Logs_1K", generateLogsData(1000)},
		{"Logs_10K", generateLogsData(10000)},
		{"Mixed_1K", generateMixedTypesData(1000)},
		{"Mixed_10K", generateMixedTypesData(10000)},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup: create spill file once
			spillFile, err := spiller.WriteSpillFile(tmpDir, bm.data, benchmarkKeyFunc)
			if err != nil {
				b.Fatal(err)
			}
			defer spiller.CleanupSpillFile(spillFile)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader, err := spiller.OpenSpillFile(spillFile, benchmarkKeyFunc)
				if err != nil {
					b.Fatal(err)
				}

				// Read all rows
				for {
					_, err := reader.Next()
					if err != nil {
						break // EOF or error
					}
				}
				reader.Close()
			}
		})
	}
}

// Benchmark CBOR spiller read performance
func BenchmarkCborSpiller_Read(b *testing.B) {
	spiller, err := NewCborSpiller()
	if err != nil {
		b.Fatal(err)
	}
	tmpDir := os.TempDir()

	benchmarks := []struct {
		name string
		data []map[string]any
	}{
		{"Metrics_1K", generateMetricsData(1000)},
		{"Metrics_10K", generateMetricsData(10000)},
		{"Logs_1K", generateLogsData(1000)},
		{"Logs_10K", generateLogsData(10000)},
		{"Mixed_1K", generateMixedTypesData(1000)},
		{"Mixed_10K", generateMixedTypesData(10000)},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Setup: create spill file once
			spillFile, err := spiller.WriteSpillFile(tmpDir, bm.data, benchmarkKeyFunc)
			if err != nil {
				b.Fatal(err)
			}
			defer spiller.CleanupSpillFile(spillFile)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader, err := spiller.OpenSpillFile(spillFile, benchmarkKeyFunc)
				if err != nil {
					b.Fatal(err)
				}

				// Read all rows
				for {
					_, err := reader.Next()
					if err != nil {
						break // EOF or error
					}
				}
				reader.Close()
			}
		})
	}
}

// Benchmark file size comparison
func BenchmarkSpiller_FileSize(b *testing.B) {
	data := generateMixedTypesData(10000) // 10K rows of mixed data
	tmpDir := os.TempDir()

	b.Run("GOB_FileSize", func(b *testing.B) {
		spiller := NewGobSpiller()
		spillFile, err := spiller.WriteSpillFile(tmpDir, data, benchmarkKeyFunc)
		if err != nil {
			b.Fatal(err)
		}
		defer spiller.CleanupSpillFile(spillFile)

		// Get file size
		stat, err := os.Stat(spillFile.Path)
		if err != nil {
			b.Fatal(err)
		}

		b.ReportMetric(float64(stat.Size()), "bytes")
		b.ReportMetric(float64(stat.Size())/float64(len(data)), "bytes/row")
	})

	b.Run("CBOR_FileSize", func(b *testing.B) {
		spiller, err := NewCborSpiller()
		if err != nil {
			b.Fatal(err)
		}

		spillFile, err := spiller.WriteSpillFile(tmpDir, data, benchmarkKeyFunc)
		if err != nil {
			b.Fatal(err)
		}
		defer spiller.CleanupSpillFile(spillFile)

		// Get file size
		stat, err := os.Stat(spillFile.Path)
		if err != nil {
			b.Fatal(err)
		}

		b.ReportMetric(float64(stat.Size()), "bytes")
		b.ReportMetric(float64(stat.Size())/float64(len(data)), "bytes/row")
	})
}

// Benchmark roundtrip performance (write + read)
func BenchmarkSpiller_Roundtrip(b *testing.B) {
	data := generateMetricsData(5000) // 5K rows
	tmpDir := os.TempDir()

	b.Run("GOB_Roundtrip", func(b *testing.B) {
		spiller := NewGobSpiller()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Write
			spillFile, err := spiller.WriteSpillFile(tmpDir, data, benchmarkKeyFunc)
			if err != nil {
				b.Fatal(err)
			}

			// Read
			reader, err := spiller.OpenSpillFile(spillFile, benchmarkKeyFunc)
			if err != nil {
				b.Fatal(err)
			}

			rowCount := 0
			for {
				_, err := reader.Next()
				if err != nil {
					break
				}
				rowCount++
			}
			reader.Close()

			if rowCount != len(data) {
				b.Fatalf("Expected %d rows, got %d", len(data), rowCount)
			}

			// Cleanup
			spiller.CleanupSpillFile(spillFile)
		}
	})

	b.Run("CBOR_Roundtrip", func(b *testing.B) {
		spiller, err := NewCborSpiller()
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Write
			spillFile, err := spiller.WriteSpillFile(tmpDir, data, benchmarkKeyFunc)
			if err != nil {
				b.Fatal(err)
			}

			// Read
			reader, err := spiller.OpenSpillFile(spillFile, benchmarkKeyFunc)
			if err != nil {
				b.Fatal(err)
			}

			rowCount := 0
			for {
				_, err := reader.Next()
				if err != nil {
					break
				}
				rowCount++
			}
			reader.Close()

			if rowCount != len(data) {
				b.Fatalf("Expected %d rows, got %d", len(data), rowCount)
			}

			// Cleanup
			spiller.CleanupSpillFile(spillFile)
		}
	})
}
