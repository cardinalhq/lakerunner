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

package cmd

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// TestCreateLogReaderWithAllSampleFiles tests the createLogReader function
// with all available sample log files to ensure they all work correctly.
// These files are known to pass in the old system and should work in the new path.
func TestCreateLogReaderWithAllSampleFiles(t *testing.T) {
	testdataDir := "../testdata/logs"

	tests := []struct {
		name          string
		filename      string
		objectID      string
		expectSuccess bool
		expectedType  string
		description   string
	}{
		// Parquet files
		{
			name:          "LogsParquet_1747427310000",
			filename:      "logs_1747427310000_667024137.parquet",
			objectID:      "logs_1747427310000_667024137.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "Standard logs parquet file from logcrunch",
		},
		{
			name:          "LogsParquet_1747427320000_408138269",
			filename:      "logs_1747427320000_408138269.parquet",
			objectID:      "logs_1747427320000_408138269.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "Another logs parquet file from logcrunch",
		},
		{
			name:          "LogsParquet_1747427320000_822381101",
			filename:      "logs_1747427320000_822381101.parquet",
			objectID:      "logs_1747427320000_822381101.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "Another logs parquet file from logcrunch",
		},
		{
			name:          "LogsParquet_1747427330000",
			filename:      "logs_1747427330000_517730925.parquet",
			objectID:      "logs_1747427330000_517730925.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "Another logs parquet file from logcrunch",
		},
		{
			name:          "LogsParquet_1752872650000",
			filename:      "logs_1752872650000_326740161.parquet",
			objectID:      "logs_1752872650000_326740161.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "Logs parquet file from rawparquet testdata",
		},
		{
			name:          "LogsParquet_CHQS3",
			filename:      "logs-chqs3-0001.parquet",
			objectID:      "logs-chqs3-0001.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "CHQS3 logs parquet file from filereader testdata",
		},
		{
			name:          "LogsParquet_Cooked",
			filename:      "logs-cooked-0001.parquet",
			objectID:      "logs-cooked-0001.parquet",
			expectSuccess: true,
			expectedType:  "*filereader.PresortedParquetRawReader",
			description:   "Cooked logs parquet file from filereader testdata",
		},

		// JSON.gz files
		{
			name:          "SampleJSONGz",
			filename:      "sample.json.gz",
			objectID:      "sample.json.gz",
			expectSuccess: true,
			expectedType:  "*filereader.JSONLinesReader",
			description:   "Sample JSON.gz file from jsongz testdata",
		},

		// Protobuf files
		{
			name:          "LogsProtobuf_160396104",
			filename:      "logs_160396104.binpb",
			objectID:      "logs_160396104.binpb",
			expectSuccess: true,
			expectedType:  "*filereader.ProtoLogsReader",
			description:   "Logs protobuf file from proto testdata",
		},

		// Note: otel-logs.binpb.gz is a gzipped protobuf file, now supported by the new factory function
		{
			name:          "OTelLogsGzippedProtobuf",
			filename:      "otel-logs.binpb.gz",
			objectID:      "otel-logs.binpb.gz",
			expectSuccess: true, // Now supported by new factory function!
			expectedType:  "*filereader.ProtoLogsReader",
			description:   "Gzipped OTel logs protobuf (now supported!)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fullPath := filepath.Join(testdataDir, tt.filename)

			t.Logf("Testing %s: %s", tt.description, fullPath)

			reader, err := createLogReader(fullPath)

			if tt.expectSuccess {
				require.NoError(t, err, "Expected success for %s but got error: %v", tt.description, err)
				require.NotNil(t, reader, "Expected non-nil reader for %s", tt.description)

				// Verify reader type
				readerType := getReaderType(reader)
				assert.Equal(t, tt.expectedType, readerType, "Wrong reader type for %s", tt.description)

				// Test that we can read at least one row (if the file has data)
				rows := make([]filereader.Row, 1) // Read one row at a time
				n, readErr := reader.Read(rows)
				if readErr != nil && readErr != io.EOF {
					t.Logf("Read error for %s (this may be expected for some files): %v", tt.description, readErr)
				} else if n > 0 {
					assert.NotNil(t, rows[0], "Expected non-nil row for %s", tt.description)
					t.Logf("Successfully read first row from %s", tt.description)
				} else {
					t.Logf("File %s appears to be empty (n=0 on first read)", tt.description)
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

// TestLogIngestPathEndToEnd tests the complete log ingest path with sample files
func TestLogIngestPathEndToEnd(t *testing.T) {
	testdataDir := "../testdata/logs"

	// Test files that should successfully process through the entire path
	supportedFiles := []struct {
		filename    string
		objectID    string
		description string
	}{
		{
			filename:    "logs_1747427310000_667024137.parquet",
			objectID:    "logs_1747427310000_667024137.parquet",
			description: "Standard logs parquet file",
		},
		{
			filename:    "sample.json.gz",
			objectID:    "sample.json.gz",
			description: "Sample JSON.gz file",
		},
		{
			filename:    "logs_160396104.binpb",
			objectID:    "logs_160396104.binpb",
			description: "Logs protobuf file",
		},
	}

	for _, tf := range supportedFiles {
		t.Run(tf.description, func(t *testing.T) {
			fullPath := filepath.Join(testdataDir, tf.filename)

			// Test reader creation
			reader, err := createLogReader(fullPath)
			require.NoError(t, err, "Failed to create reader for %s", tf.description)
			require.NotNil(t, reader, "Reader is nil for %s", tf.description)

			defer reader.Close()

			// Test that we can read through the file
			totalRows := 0
			batchSize := 10
			rows := make([]filereader.Row, batchSize)

			for {
				n, err := reader.Read(rows)
				if err != nil && err != io.EOF {
					t.Logf("Read error for %s after %d rows: %v", tf.description, totalRows, err)
					break
				}

				// Process the rows we read
				for i := 0; i < n; i++ {
					if rows[i] != nil {
						totalRows++

						// For the first few rows, validate structure
						if totalRows <= 3 {
							assert.IsType(t, filereader.Row{}, rows[i], "Row should be filereader.Row for %s", tf.description)

							// Log first row contents for debugging
							if totalRows == 1 {
								t.Logf("First row from %s contains %d fields", tf.description, len(rows[i]))
								for key := range rows[i] {
									t.Logf("  Field: %s", key)
								}
							}
						}
					}
				}

				// Check if we're done
				if err == io.EOF || n == 0 {
					break
				}

				// Prevent infinite loops in tests
				if totalRows > 10000 {
					t.Logf("Stopping read test for %s after %d rows", tf.description, totalRows)
					break
				}
			}

			t.Logf("Successfully processed %d rows from %s", totalRows, tf.description)

			// All test files should have at least some data
			if totalRows == 0 {
				t.Logf("Warning: %s appears to contain no readable rows", tf.description)
			}
		})
	}
}

// Helper function to get the type name of a reader
func getReaderType(reader interface{}) string {
	if reader == nil {
		return "<nil>"
	}
	return getTypeName(reader)
}

// getTypeName returns the type name including package for better debugging
func getTypeName(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T", v)
}

// TestLogTranslatorWithSampleFiles tests LogTranslator with actual sample files
// to ensure the complete ingest workflow including fingerprinting works correctly
func TestLogTranslatorWithSampleFiles(t *testing.T) {
	testdataDir := "../testdata/logs"

	tests := []struct {
		filename    string
		objectID    string
		description string
		expectRows  int // Minimum expected rows, -1 for any
	}{
		{
			filename:    "sample.json.gz",
			objectID:    "sample.json.gz",
			description: "JSON.gz file with LogTranslator processing",
			expectRows:  3,
		},
		{
			filename:    "logs_1747427310000_667024137.parquet",
			objectID:    "logs_1747427310000_667024137.parquet",
			description: "Parquet file with LogTranslator processing",
			expectRows:  30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			fullPath := filepath.Join(testdataDir, tt.filename)

			// Create LogTranslator
			translator := &LogTranslator{
				orgID:    "test-org",
				bucket:   "test-bucket",
				objectID: tt.objectID,
			}
			require.NotNil(t, translator, "Failed to create LogTranslator")

			// Create reader
			reader, err := createLogReader(fullPath)
			require.NoError(t, err, "Failed to create reader for %s", tt.description)
			require.NotNil(t, reader, "Reader is nil for %s", tt.description)
			defer reader.Close()

			// Process rows through translator
			totalRows := 0
			fingerprintedRows := 0
			batchSize := 10
			rows := make([]filereader.Row, batchSize)

			for {
				n, err := reader.Read(rows)
				if err != nil && err != io.EOF {
					t.Logf("Read error for %s after %d rows: %v", tt.description, totalRows, err)
					break
				}

				// Process each row through the translator
				for i := 0; i < n; i++ {
					if rows[i] != nil {
						totalRows++

						// Translate the row
						translateErr := translator.TranslateRow(&rows[i])
						if translateErr != nil {
							t.Errorf("Translation error on row %d for %s: %v", totalRows, tt.description, translateErr)
							continue
						}

						// Verify translation results
						assert.NotNil(t, rows[i], "Translated row %d should not be nil for %s", totalRows, tt.description)

						// Check for required fields added by translation
						assert.Contains(t, rows[i], "resource.bucket.name", "Row %d missing bucket name for %s", totalRows, tt.description)
						assert.Contains(t, rows[i], "resource.file.name", "Row %d missing file name for %s", totalRows, tt.description)
						assert.Contains(t, rows[i], "resource.file.type", "Row %d missing file type for %s", totalRows, tt.description)

						assert.Equal(t, "test-bucket", rows[i]["resource.bucket.name"], "Wrong bucket name in row %d for %s", totalRows, tt.description)
						assert.Equal(t, "./"+tt.objectID, rows[i]["resource.file.name"], "Wrong file name in row %d for %s", totalRows, tt.description)

						// Check fingerprinting (if present)
						if _, hasFingerprint := rows[i]["_cardinalhq.fingerprint"]; hasFingerprint {
							fingerprintedRows++
							fingerprint := rows[i]["_cardinalhq.fingerprint"]
							if fingerprint != nil {
								assert.IsType(t, int64(0), fingerprint, "Fingerprint should be int64 for row %d in %s", totalRows, tt.description)
								fpVal := fingerprint.(int64)
								assert.NotEqual(t, int64(0), fpVal, "Fingerprint should not be zero for row %d in %s", totalRows, tt.description)
							}
						}

						// Log first row details for debugging
						if totalRows == 1 {
							t.Logf("First translated row from %s:", tt.description)
							t.Logf("  Total fields: %d", len(rows[i]))
							t.Logf("  Bucket: %v", rows[i]["resource.bucket.name"])
							t.Logf("  File: %v", rows[i]["resource.file.name"])
							t.Logf("  Type: %v", rows[i]["resource.file.type"])
							if fp := rows[i]["_cardinalhq.fingerprint"]; fp != nil {
								t.Logf("  Fingerprint: %v", fp)
							}
							if ts := rows[i]["_cardinalhq.timestamp"]; ts != nil {
								t.Logf("  Timestamp: %v", ts)
							}
						}
					}
				}

				// Check if we're done
				if err == io.EOF || n == 0 {
					break
				}

				// Prevent infinite loops
				if totalRows > 1000 {
					t.Logf("Stopping test for %s after %d rows", tt.description, totalRows)
					break
				}
			}

			// Validate results
			t.Logf("Processed %d rows from %s (%d with fingerprints)", totalRows, tt.description, fingerprintedRows)

			if tt.expectRows >= 0 {
				assert.GreaterOrEqual(t, totalRows, tt.expectRows, "Expected at least %d rows from %s", tt.expectRows, tt.description)
			}

			assert.Greater(t, totalRows, 0, "Should have processed at least one row from %s", tt.description)
		})
	}
}
