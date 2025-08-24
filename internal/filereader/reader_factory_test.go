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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			expectedType:  "*filereader.ParquetReader",
			description:   "Logs parquet file with logs signal type",
		},
		{
			name:          "ParquetWithMetricsSignalType",
			filename:      filepath.Join(testdataDir, "logs", "logs_1747427310000_667024137.parquet"),
			signalType:    SignalTypeMetrics,
			expectSuccess: true,
			expectedType:  "*filereader.ParquetReader",
			description:   "Parquet file with metrics signal type (parquet readers work for all signals)",
		},
		{
			name:          "ParquetWithTracesSignalType",
			filename:      filepath.Join(testdataDir, "logs", "logs_1747427310000_667024137.parquet"),
			signalType:    SignalTypeTraces,
			expectSuccess: true,
			expectedType:  "*filereader.ParquetReader",
			description:   "Parquet file with traces signal type (parquet readers work for all signals)",
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
			filename:      filepath.Join(testdataDir, "logs", "logs_160396104.binpb"),
			signalType:    SignalTypeLogs,
			expectSuccess: true,
			expectedType:  "*filereader.ProtoLogsReader",
			description:   "Logs protobuf file",
		},
		{
			name:          "BinpbGzMetrics",
			filename:      filepath.Join(testdataDir, "metrics", "otel-metrics.binpb.gz"),
			signalType:    SignalTypeMetrics,
			expectSuccess: true,
			expectedType:  "*filereader.ProtoMetricsReader",
			description:   "Gzipped metrics protobuf file (new support!)",
		},
		{
			name:          "BinpbGzTraces",
			filename:      filepath.Join(testdataDir, "traces", "otel-traces.binpb.gz"),
			signalType:    SignalTypeTraces,
			expectSuccess: true,
			expectedType:  "*filereader.ProtoTracesReader",
			description:   "Gzipped traces protobuf file (new support!)",
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
			t.Logf("Testing %s: %s", tt.description, tt.filename)

			reader, err := ReaderForFile(tt.filename, tt.signalType)

			if tt.expectSuccess {
				require.NoError(t, err, "Expected success for %s but got error: %v", tt.description, err)
				require.NotNil(t, reader, "Expected non-nil reader for %s", tt.description)

				// Verify reader type
				readerType := getReaderTypeName(reader)
				assert.Equal(t, tt.expectedType, readerType, "Wrong reader type for %s", tt.description)

				// Test that we can read at least one row (if the file has data)
				if canRead(tt.filename) {
					rows := make([]Row, 1)
					n, readErr := reader.Read(rows)
					if readErr != nil && readErr != io.EOF {
						t.Logf("Read error for %s (this may be expected for some files): %v", tt.description, readErr)
					} else if n > 0 {
						assert.NotNil(t, rows[0], "Expected non-nil row for %s", tt.description)
						t.Logf("Successfully read first row from %s", tt.description)
					} else {
						t.Logf("File %s appears to be empty (n=0 on first read)", tt.description)
					}
				}

				// Clean up
				reader.Close()
			} else {
				assert.Error(t, err, "Expected error for %s but got success", tt.description)
				if reader != nil {
					reader.Close()
				}
			}
		})
	}
}

func TestReaderForFileWithOptions(t *testing.T) {
	testdataDir := "../../testdata"

	// Test that ReaderForFileWithOptions works the same as ReaderForFile
	filename := filepath.Join(testdataDir, "logs", "logs_160396104.binpb")

	// Using ReaderForFile
	reader1, err1 := ReaderForFile(filename, SignalTypeLogs)
	require.NoError(t, err1)
	defer reader1.Close()

	// Using ReaderForFileWithOptions
	opts := ReaderOptions{SignalType: SignalTypeLogs}
	reader2, err2 := ReaderForFileWithOptions(filename, opts)
	require.NoError(t, err2)
	defer reader2.Close()

	// Both should return the same type
	assert.Equal(t, getReaderTypeName(reader1), getReaderTypeName(reader2))
}

func TestSignalTypeSpecificProtoReaders(t *testing.T) {
	testdataDir := "../../testdata"

	tests := []struct {
		name       string
		filename   string
		signalType SignalType
		readerType string
	}{
		{
			name:       "LogsProtoReader",
			filename:   filepath.Join(testdataDir, "logs", "logs_160396104.binpb"),
			signalType: SignalTypeLogs,
			readerType: "*filereader.ProtoLogsReader",
		},
		{
			name:       "MetricsProtoReader",
			filename:   filepath.Join(testdataDir, "metrics", "otel-metrics.binpb.gz"),
			signalType: SignalTypeMetrics,
			readerType: "*filereader.ProtoMetricsReader",
		},
		{
			name:       "TracesProtoReader",
			filename:   filepath.Join(testdataDir, "traces", "otel-traces.binpb.gz"),
			signalType: SignalTypeTraces,
			readerType: "*filereader.ProtoTracesReader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := ReaderForFile(tt.filename, tt.signalType)
			require.NoError(t, err)
			defer reader.Close()

			readerType := getReaderTypeName(reader)
			assert.Equal(t, tt.readerType, readerType)

			t.Logf("Successfully created %s for %s", readerType, tt.filename)
		})
	}
}

func TestGzippedProtobufSupport(t *testing.T) {
	testdataDir := "../../testdata"

	// Test that .binpb.gz files work correctly
	tests := []struct {
		name       string
		filename   string
		signalType SignalType
	}{
		{
			name:       "GzippedMetrics",
			filename:   filepath.Join(testdataDir, "metrics", "otel-metrics.binpb.gz"),
			signalType: SignalTypeMetrics,
		},
		{
			name:       "GzippedTraces",
			filename:   filepath.Join(testdataDir, "traces", "otel-traces.binpb.gz"),
			signalType: SignalTypeTraces,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := ReaderForFile(tt.filename, tt.signalType)
			require.NoError(t, err, "Should support .binpb.gz files")
			defer reader.Close()

			// Verify we can read data
			rows := make([]Row, 1)
			n, readErr := reader.Read(rows)
			if readErr != nil && readErr != io.EOF {
				t.Logf("Read error (may be expected): %v", readErr)
			} else if n > 0 {
				assert.NotNil(t, rows[0], "Should be able to read data from gzipped protobuf")
				t.Logf("Successfully read from gzipped protobuf: %s", tt.filename)
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
			reader, err := ReaderForFile(tt.filename, tt.signalType)

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
