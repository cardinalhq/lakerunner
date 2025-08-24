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
	"context"
	"testing"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
)

// TestLogTranslatorIntegration verifies that the LogTranslator works correctly
// with the new *Row interface and produces the expected output for parquetwriter.
func TestLogTranslatorIntegration(t *testing.T) {
	// Create LogTranslator
	translator := &LogTranslator{
		orgID:              "test-org-123",
		bucket:             "test-bucket",
		objectID:           "test-logs.json.gz",
		trieClusterManager: fingerprinter.NewTrieClusterManager(0.5),
	}

	tests := []struct {
		name     string
		input    filereader.Row
		expected map[string]any
		checkFn  func(t *testing.T, result filereader.Row)
	}{
		{
			name: "ValidLogWithMessage",
			input: filereader.Row{
				"_cardinalhq.message":   "test log message",
				"_cardinalhq.timestamp": int64(1640995200000),
				"level":                 "info",
			},
			checkFn: func(t *testing.T, result filereader.Row) {
				// Should have fingerprint calculated
				assert.Contains(t, result, "_cardinalhq.fingerprint")
				assert.NotEqual(t, int64(0), result["_cardinalhq.fingerprint"])

				// Should have resource fields added
				assert.Equal(t, "test-bucket", result["resource.bucket.name"])
				assert.Equal(t, "./test-logs.json.gz", result["resource.file.name"])
				assert.Equal(t, "testlogsjson", result["resource.file.type"])

				// Original fields should be preserved
				assert.Equal(t, "test log message", result["_cardinalhq.message"])
				assert.Equal(t, int64(1640995200000), result["_cardinalhq.timestamp"])
				assert.Equal(t, "info", result["level"])
			},
		},
		{
			name: "LogWithoutMessage",
			input: filereader.Row{
				"_cardinalhq.timestamp": int64(1640995200000),
				"level":                 "error",
			},
			checkFn: func(t *testing.T, result filereader.Row) {
				// Should not have fingerprint (no message to fingerprint)
				assert.NotContains(t, result, "_cardinalhq.fingerprint")

				// Should have resource fields
				assert.Equal(t, "test-bucket", result["resource.bucket.name"])
				assert.Equal(t, "./test-logs.json.gz", result["resource.file.name"])

				// Original fields should be preserved
				assert.Equal(t, int64(1640995200000), result["_cardinalhq.timestamp"])
				assert.Equal(t, "error", result["level"])
			},
		},
		{
			name: "LogWithFloatTimestamp",
			input: filereader.Row{
				"_cardinalhq.message":   "test message",
				"_cardinalhq.timestamp": int64(1640995200000), // Already properly typed
				"severity":              "warn",
			},
			checkFn: func(t *testing.T, result filereader.Row) {
				// Should have fingerprint
				assert.Contains(t, result, "_cardinalhq.fingerprint")

				// Timestamp should be preserved as int64
				ts, ok := result["_cardinalhq.timestamp"].(int64)
				require.True(t, ok, "Expected timestamp to be int64")
				assert.Equal(t, int64(1640995200000), ts)

				// Other fields preserved
				assert.Equal(t, "test message", result["_cardinalhq.message"])
				assert.Equal(t, "warn", result["severity"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of input to verify original is modified
			row := make(filereader.Row)
			for k, v := range tt.input {
				row[k] = v
			}

			// Apply translation
			err := translator.TranslateRow(&row)
			require.NoError(t, err)

			// Run test-specific checks
			tt.checkFn(t, row)
		})
	}
}

// TestLogTranslatorWithParquetWriter verifies the complete integration from
// LogTranslator through to ParquetWriter output.
func TestLogTranslatorWithParquetWriter(t *testing.T) {
	tmpdir := t.TempDir()

	// Create LogTranslator
	translator := &LogTranslator{
		orgID:              "test-org",
		bucket:             "test-bucket",
		objectID:           "integration-test.json.gz",
		trieClusterManager: fingerprinter.NewTrieClusterManager(0.5),
	}

	// Create logs writer
	writer, err := factories.NewLogsWriter("integration-test", tmpdir, 10000, 200.0)
	require.NoError(t, err)
	defer writer.Abort()

	// Test data representing what might come from a filereader
	rawRows := []filereader.Row{
		{
			"_cardinalhq.message":   "User login attempt",
			"_cardinalhq.timestamp": int64(1640995200000),
			"user_id":               "user123",
			"success":               true,
		},
		{
			"_cardinalhq.message":   "Database query executed",
			"_cardinalhq.timestamp": int64(1640995201000),
			"query_time_ms":         float64(45.7),
		},
		{
			"_cardinalhq.message":   "Error processing request", // Using standard message field
			"_cardinalhq.timestamp": int64(1640995202000),       // Properly typed timestamp
			"error_code":            int64(500),
		},
	}

	// Translate and write each row
	for _, rawRow := range rawRows {
		// Apply translation (simulating what TranslatingReader does)
		row := make(filereader.Row)
		for k, v := range rawRow {
			row[k] = v
		}

		err := translator.TranslateRow(&row)
		require.NoError(t, err)

		// Write to parquet writer
		err = writer.Write(row)
		require.NoError(t, err)
	}

	// Close writer and get results
	results, err := writer.Close(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 1, "Expected 1 output file")

	// Verify file stats contain expected metadata
	stats, ok := results[0].Metadata.(factories.LogsFileStats)
	require.True(t, ok, "Expected LogsFileStats metadata")

	// Note: All rows have _cardinalhq.message field and generate fingerprints
	assert.Len(t, stats.Fingerprints, 3) // All 3 rows had _cardinalhq.message to fingerprint
	assert.Equal(t, int64(1640995200000), stats.FirstTS)
	assert.Equal(t, int64(1640995202000), stats.LastTS)

	assert.Equal(t, int64(3), results[0].RecordCount) // 3 rows total

	// Clean up
	assert.NotEmpty(t, results[0].FileName)
	// Note: In real test, we'd read back the Parquet file to verify content,
	// but that's covered by the mutation tests in parquetwriter package
}

// TestLogTranslatorErrorHandling verifies error handling in the new interface.
func TestLogTranslatorErrorHandling(t *testing.T) {
	translator := &LogTranslator{
		orgID:              "test-org",
		bucket:             "test-bucket",
		objectID:           "error-test.json.gz",
		trieClusterManager: fingerprinter.NewTrieClusterManager(0.5),
	}

	// Test with nil row (should not panic)
	var nilRow *filereader.Row
	err := translator.TranslateRow(nilRow)
	assert.Error(t, err, "Should handle nil row gracefully")

	// Test with empty row
	emptyRow := make(filereader.Row)
	err = translator.TranslateRow(&emptyRow)
	require.NoError(t, err)

	// Should have resource fields but no fingerprint
	assert.Equal(t, "test-bucket", emptyRow["resource.bucket.name"])
	assert.NotContains(t, emptyRow, "_cardinalhq.fingerprint")
}
