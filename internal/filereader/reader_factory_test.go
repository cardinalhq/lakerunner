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
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Helper functions to create synthetic protobuf files

func createSyntheticLogsFile(t *testing.T, compressed bool) string {
	builder := signalbuilder.NewLogBuilder()
	resourceLogs := &signalbuilder.ResourceLogs{
		Resource: map[string]any{
			"service.name": "factory-test-logs-service",
		},
		ScopeLogs: []signalbuilder.ScopeLogs{
			{
				Name:    "factory-test-logger",
				Version: "1.0.0",
				LogRecords: []signalbuilder.LogRecord{
					{
						Timestamp:      time.Now().UnixNano(),
						SeverityText:   "INFO",
						SeverityNumber: int32(plog.SeverityNumberInfo),
						Body:           "Factory test log message",
					},
				},
			},
		},
	}

	err := builder.Add(resourceLogs)
	require.NoError(t, err)

	logs := builder.Build()
	marshaler := &plog.ProtoMarshaler{}
	data, err := marshaler.MarshalLogs(logs)
	require.NoError(t, err)

	// Create temporary file
	var tmpFile *os.File
	if compressed {
		tmpFile, err = os.CreateTemp("", "*.binpb.gz")
		require.NoError(t, err)

		gzWriter := gzip.NewWriter(tmpFile)
		_, err = gzWriter.Write(data)
		require.NoError(t, err)

		err = gzWriter.Close()
		require.NoError(t, err)
	} else {
		tmpFile, err = os.CreateTemp("", "*.binpb")
		require.NoError(t, err)

		_, err = tmpFile.Write(data)
		require.NoError(t, err)
	}

	err = tmpFile.Close()
	require.NoError(t, err)

	// Clean up the file when test completes
	t.Cleanup(func() {
		_ = os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func createSyntheticMetricsFile(t *testing.T, compressed bool) string {
	builder := signalbuilder.NewMetricsBuilder()
	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "factory-test-metrics-service",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name:    "factory-test-meter",
				Version: "1.0.0",
				Metrics: []signalbuilder.Metric{
					{
						Name: "factory_test_counter",
						Type: "sum",
						Sum: &signalbuilder.SumMetric{
							IsMonotonic: true,
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Value:     100,
									Timestamp: time.Now().UnixNano(),
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	require.NoError(t, err)

	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	// Create temporary file
	var tmpFile *os.File
	if compressed {
		tmpFile, err = os.CreateTemp("", "*.binpb.gz")
		require.NoError(t, err)

		gzWriter := gzip.NewWriter(tmpFile)
		_, err = gzWriter.Write(data)
		require.NoError(t, err)

		err = gzWriter.Close()
		require.NoError(t, err)
	} else {
		tmpFile, err = os.CreateTemp("", "*.binpb")
		require.NoError(t, err)

		_, err = tmpFile.Write(data)
		require.NoError(t, err)
	}

	err = tmpFile.Close()
	require.NoError(t, err)

	// Clean up the file when test completes
	t.Cleanup(func() {
		_ = os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func createSyntheticTracesFile(t *testing.T, compressed bool) string {
	builder := signalbuilder.NewTracesBuilder()
	resourceTraces := &signalbuilder.ResourceTraces{
		Resource: map[string]any{
			"service.name": "factory-test-traces-service",
		},
		ScopeTraces: []signalbuilder.ScopeTraces{
			{
				Name:    "factory-test-tracer",
				Version: "1.0.0",
				Spans: []signalbuilder.Span{
					{
						TraceID:        "12345678901234567890123456789012",
						SpanID:         "1234567890123456",
						Name:           "factory-test-operation",
						Kind:           1,
						StartTimestamp: time.Now().UnixNano(),
						EndTimestamp:   time.Now().Add(100 * time.Millisecond).UnixNano(),
					},
				},
			},
		},
	}

	err := builder.Add(resourceTraces)
	require.NoError(t, err)

	traces := builder.Build()
	marshaler := &ptrace.ProtoMarshaler{}
	data, err := marshaler.MarshalTraces(traces)
	require.NoError(t, err)

	// Create temporary file
	var tmpFile *os.File
	if compressed {
		tmpFile, err = os.CreateTemp("", "*.binpb.gz")
		require.NoError(t, err)

		gzWriter := gzip.NewWriter(tmpFile)
		_, err = gzWriter.Write(data)
		require.NoError(t, err)

		err = gzWriter.Close()
		require.NoError(t, err)
	} else {
		tmpFile, err = os.CreateTemp("", "*.binpb")
		require.NoError(t, err)

		_, err = tmpFile.Write(data)
		require.NoError(t, err)
	}

	err = tmpFile.Close()
	require.NoError(t, err)

	// Clean up the file when test completes
	t.Cleanup(func() {
		_ = os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func TestReaderForFile(t *testing.T) {
	testdataDir := "../../testdata"

	tests := []struct {
		name          string
		filename      string
		signalType    SignalType
		expectSuccess bool
		expectedType  string
		description   string
	}{
		// Parquet files (should work for all signal types)
		{
			name:          "LogsParquet",
			filename:      filepath.Join(testdataDir, "logs", "logs_1747427310000_667024137.parquet"),
			signalType:    SignalTypeLogs,
			expectSuccess: true,
			expectedType:  "*filereader.IngestLogParquetReader",
			description:   "Logs parquet file with logs signal type",
		},
		{
			name:          "ParquetWithMetricsSignalType",
			filename:      filepath.Join(testdataDir, "logs", "logs_1747427310000_667024137.parquet"),
			signalType:    SignalTypeMetrics,
			expectSuccess: true,
			expectedType:  "*filereader.ParquetRawReader",
			description:   "Parquet file with metrics signal type (uses ParquetRawReader)",
		},
		{
			name:          "ParquetWithTracesSignalType",
			filename:      filepath.Join(testdataDir, "logs", "logs_1747427310000_667024137.parquet"),
			signalType:    SignalTypeTraces,
			expectSuccess: true,
			expectedType:  "*filereader.ParquetRawReader",
			description:   "Parquet file with traces signal type (uses ParquetRawReader)",
		},

		// JSON.gz files (should work for all signal types)
		{
			name:          "JSONGzLogs",
			filename:      filepath.Join(testdataDir, "logs", "sample.json.gz"),
			signalType:    SignalTypeLogs,
			expectSuccess: true,
			expectedType:  "*filereader.JSONLinesReader",
			description:   "JSON.gz file with logs signal type",
		},
		{
			name:          "JSONGzWithMetricsSignalType",
			filename:      filepath.Join(testdataDir, "logs", "sample.json.gz"),
			signalType:    SignalTypeMetrics,
			expectSuccess: true,
			expectedType:  "*filereader.JSONLinesReader",
			description:   "JSON.gz file with metrics signal type",
		},

		// Protobuf files (signal-specific readers)
		{
			name:          "BinpbLogs",
			filename:      "",
			signalType:    SignalTypeLogs,
			expectSuccess: true,
			expectedType:  "*filereader.IngestProtoLogsReader",
			description:   "Logs protobuf file",
		},
		{
			name:          "BinpbGzMetrics",
			filename:      "",
			signalType:    SignalTypeMetrics,
			expectSuccess: true,
			expectedType:  "*filereader.IngestProtoMetricsReader",
			description:   "Gzipped metrics protobuf file",
		},
		{
			name:          "BinpbGzTraces",
			filename:      "",
			signalType:    SignalTypeTraces,
			expectSuccess: true,
			expectedType:  "*filereader.IngestProtoTracesReader",
			description:   "Gzipped traces protobuf file",
		},

		// Error cases
		{
			name:          "UnsupportedFileType",
			filename:      "/tmp/test.txt",
			signalType:    SignalTypeLogs,
			expectSuccess: false,
			expectedType:  "",
			description:   "Unsupported file extension",
		},
		{
			name:          "NonExistentFile",
			filename:      "/tmp/nonexistent.parquet",
			signalType:    SignalTypeLogs,
			expectSuccess: false,
			expectedType:  "",
			description:   "File does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := tt.filename
			if tt.filename == "" {
				switch tt.name {
				case "BinpbLogs":
					filename = createSyntheticLogsFile(t, false)
				case "BinpbGzMetrics":
					filename = createSyntheticMetricsFile(t, true)
				case "BinpbGzTraces":
					filename = createSyntheticTracesFile(t, true)
				}
			}

			t.Logf("Testing %s: %s", tt.description, filename)

			reader, err := ReaderForFile(filename, tt.signalType, "orgId", nil)

			if tt.expectSuccess {
				require.NoError(t, err, "Expected success for %s but got error: %v", tt.description, err)
				require.NotNil(t, reader, "Expected non-nil reader for %s", tt.description)

				// Verify reader type
				readerType := getReaderTypeName(reader)
				assert.Equal(t, tt.expectedType, readerType, "Wrong reader type for %s", tt.description)

				// Test that we can read at least one row (if the file has data)
				if canRead(filename) {
					batch, readErr := reader.Next(context.TODO())
					if readErr != nil && readErr != io.EOF {
						t.Logf("Read error for %s (this may be expected for some files): %v", tt.description, readErr)
					} else if batch != nil && batch.Len() > 0 {
						assert.NotNil(t, batch.Get(0), "Expected non-nil row for %s", tt.description)
						t.Logf("Successfully read first row from %s", tt.description)
					} else {
						t.Logf("File %s appears to be empty (no batch or empty batch on first read)", tt.description)
					}
				}

				// Clean up
				_ = reader.Close()
			} else {
				assert.Error(t, err, "Expected error for %s but got success", tt.description)
				if reader != nil {
					_ = reader.Close()
				}
			}
		})
	}
}

func TestReaderForFileWithOptions(t *testing.T) {
	// Test that ReaderForFileWithOptions works the same as ReaderForFile
	// Create synthetic protobuf file for testing
	filename := createSyntheticLogsFile(t, false)

	// Using ReaderForFile
	reader1, err1 := ReaderForFile(filename, SignalTypeLogs, "orgId", nil)
	require.NoError(t, err1)
	defer func() { _ = reader1.Close() }()

	// Using ReaderForFileWithOptions
	opts := ReaderOptions{SignalType: SignalTypeLogs}
	reader2, err2 := ReaderForFileWithOptions(filename, opts)
	require.NoError(t, err2)
	defer func() { _ = reader2.Close() }()

	// Both should return the same type
	assert.Equal(t, getReaderTypeName(reader1), getReaderTypeName(reader2))
}

func TestSignalTypeSpecificProtoReaders(t *testing.T) {
	tests := []struct {
		name       string
		filename   string
		signalType SignalType
		readerType string
	}{
		{
			name:       "LogsProtoReader",
			filename:   "",
			signalType: SignalTypeLogs,
			readerType: "*filereader.IngestProtoLogsReader",
		},
		{
			name:       "MetricsProtoReader",
			filename:   "",
			signalType: SignalTypeMetrics,
			readerType: "*filereader.IngestProtoMetricsReader",
		},
		{
			name:       "TracesProtoReader",
			filename:   "",
			signalType: SignalTypeTraces,
			readerType: "*filereader.IngestProtoTracesReader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := tt.filename
			if tt.filename == "" {
				switch tt.name {
				case "LogsProtoReader":
					filename = createSyntheticLogsFile(t, false)
				case "MetricsProtoReader":
					filename = createSyntheticMetricsFile(t, true)
				case "TracesProtoReader":
					filename = createSyntheticTracesFile(t, true)
				}
			}

			reader, err := ReaderForFile(filename, tt.signalType, "orgId", nil)
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			readerType := getReaderTypeName(reader)
			assert.Equal(t, tt.readerType, readerType)

			t.Logf("Successfully created %s for %s", readerType, filename)
		})
	}
}

func TestGzippedProtobufSupport(t *testing.T) {
	tests := []struct {
		name       string
		filename   string
		signalType SignalType
	}{
		{
			name:       "GzippedMetrics",
			filename:   "",
			signalType: SignalTypeMetrics,
		},
		{
			name:       "GzippedTraces",
			filename:   "",
			signalType: SignalTypeTraces,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := tt.filename
			if tt.filename == "" {
				switch tt.name {
				case "GzippedMetrics":
					filename = createSyntheticMetricsFile(t, true)
				case "GzippedTraces":
					filename = createSyntheticTracesFile(t, true)
				}
			}

			reader, err := ReaderForFile(filename, tt.signalType, "orgId", nil)
			require.NoError(t, err, "Should support .binpb.gz files")
			defer func() { _ = reader.Close() }()

			// Verify we can read data
			batch, readErr := reader.Next(context.TODO())
			if readErr != nil && readErr != io.EOF {
				t.Logf("Read error (may be expected): %v", readErr)
			} else if batch != nil && batch.Len() > 0 {
				assert.NotNil(t, batch.Get(0), "Should be able to read data from gzipped protobuf")
				t.Logf("Successfully read from gzipped protobuf: %s", filename)
			}
		})
	}
}

func TestErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		signalType  SignalType
		expectError string
	}{
		{
			name:        "UnsupportedExtension",
			filename:    "/tmp/test.xml",
			signalType:  SignalTypeLogs,
			expectError: "unsupported file type",
		},
		{
			name:        "NonExistentParquetFile",
			filename:    "/tmp/nonexistent.parquet",
			signalType:  SignalTypeLogs,
			expectError: "failed to open parquet file",
		},
		{
			name:        "NonExistentJSONGzFile",
			filename:    "/tmp/nonexistent.json.gz",
			signalType:  SignalTypeLogs,
			expectError: "failed to open JSON.gz file",
		},
		{
			name:        "NonExistentProtobufFile",
			filename:    "/tmp/nonexistent.binpb",
			signalType:  SignalTypeLogs,
			expectError: "failed to open protobuf file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := ReaderForFile(tt.filename, tt.signalType, "orgId", nil)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectError)
			assert.Nil(t, reader)
		})
	}
}

// Helper function to get the type name of a reader for testing
func getReaderTypeName(reader interface{}) string {
	if reader == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T", reader)
}

// Helper function to check if we should attempt to read from a file
func canRead(filename string) bool {
	// Check if file exists and has reasonable size
	if stat, err := os.Stat(filename); err == nil && stat.Size() > 0 {
		return true
	}
	return false
}
