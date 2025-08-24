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

package factories

import (
	"context"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

func TestNewLogsWriter(t *testing.T) {
	tmpdir := t.TempDir()

	writer, err := NewLogsWriter("logs-test", tmpdir, 10000, 67)
	require.NoError(t, err, "Failed to create logs writer")
	defer writer.Abort()

	// Verify it's configured for merge sort ordering
	assert.Equal(t, parquetwriter.OrderMergeSort, writer.Config().OrderBy)

	// Test timestamp ordering - write out of order
	testData := []map[string]any{
		{"_cardinalhq.timestamp": int64(3000), "_cardinalhq.message": "third", "_cardinalhq.name": "log.events"},
		{"_cardinalhq.timestamp": int64(1000), "_cardinalhq.message": "first", "_cardinalhq.name": "log.events"},
		{"_cardinalhq.timestamp": int64(4000), "_cardinalhq.message": "fourth", "_cardinalhq.name": "log.events"},
		{"_cardinalhq.timestamp": int64(2000), "_cardinalhq.message": "second", "_cardinalhq.name": "log.events"},
	}

	for _, row := range testData {
		require.NoError(t, ValidateLogsRow(row), "Row validation failed")
		require.NoError(t, writer.Write(row), "Failed to write row")
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	require.NoError(t, err, "Failed to close writer")

	require.Len(t, results, 1, "Expected 1 file")

	// Verify the file is timestamp-ordered
	file, err := os.Open(results[0].FileName)
	require.NoError(t, err, "Failed to open result file")
	defer file.Close()
	defer os.Remove(results[0].FileName)

	// For verification, we'll use a schema discovered from the written data
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"_cardinalhq.message":   parquet.String(),
		"_cardinalhq.name":      parquet.String(),
	}
	schema := parquet.NewSchema("logs-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var lastTimestamp int64
	rowCount := 0
	for {
		rows := make([]map[string]any, 1)
		rows[0] = make(map[string]any)

		n, err := reader.Read(rows)
		if n == 0 {
			break
		}
		if err != nil && err.Error() != "EOF" {
			require.NoError(t, err, "Failed to read from parquet")
		}

		ts := rows[0]["_cardinalhq.timestamp"].(int64)
		if rowCount > 0 {
			assert.GreaterOrEqual(t, ts, lastTimestamp, "Timestamps not in order")
		}
		lastTimestamp = ts
		rowCount++
	}

	assert.Equal(t, 4, rowCount, "Expected 4 rows")

	// Check stats
	if stats, ok := results[0].Metadata.(LogsFileStats); ok {
		// With comprehensive fingerprinting, we expect multiple fingerprints per row
		// Each row will generate fingerprints for _cardinalhq.name, _cardinalhq.message trigrams, etc.
		assert.Greater(t, len(stats.Fingerprints), 3, "Expected multiple fingerprints from comprehensive fingerprinting")
		assert.Equal(t, int64(1000), stats.FirstTS, "Expected first timestamp 1000")
		assert.Equal(t, int64(4000), stats.LastTS, "Expected last timestamp 4000")

		// Verify all fingerprints are unique (no duplicates in the final list)
		fingerprintSet := make(map[int64]bool)
		for _, fp := range stats.Fingerprints {
			assert.False(t, fingerprintSet[fp], "Found duplicate fingerprint: %d", fp)
			fingerprintSet[fp] = true
		}
	} else {
		assert.Fail(t, "Expected LogsFileStats metadata")
	}
}

func TestValidateLogsRow(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]any
		wantErr bool
	}{
		{
			name:    "valid row",
			row:     map[string]any{"_cardinalhq.timestamp": int64(123456)},
			wantErr: false,
		},
		{
			name:    "missing timestamp",
			row:     map[string]any{"message": "test"},
			wantErr: true,
		},
		{
			name:    "empty row",
			row:     map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLogsRow(tt.row)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLogsStatsAccumulator(t *testing.T) {
	provider := &LogsStatsProvider{}
	accumulator := provider.NewAccumulator().(*LogsStatsAccumulator)

	// Add some test data with comprehensive log fields for fingerprinting
	testData := []map[string]any{
		{
			"_cardinalhq.timestamp": int64(1000),
			"_cardinalhq.message":   "User login successful",
			"_cardinalhq.name":      "log.events",
			"_cardinalhq.level":     "info",
		},
		{
			"_cardinalhq.timestamp": int64(3000),
			"_cardinalhq.message":   "Database error occurred",
			"_cardinalhq.name":      "log.events",
			"_cardinalhq.level":     "error",
		},
		{
			"_cardinalhq.timestamp": int64(2000),
			"_cardinalhq.message":   "User login successful", // Same message as first row
			"_cardinalhq.name":      "log.events",
			"_cardinalhq.level":     "info",
		},
		{
			"_cardinalhq.timestamp": int64(4000),
			"_cardinalhq.message":   "Connection timeout",
			"_cardinalhq.name":      "log.events",
			"_cardinalhq.level":     "warning",
		},
	}

	for _, row := range testData {
		accumulator.Add(row)
	}

	stats := accumulator.Finalize().(LogsFileStats)

	// With comprehensive fingerprinting, we expect many more unique fingerprints
	// Each row generates fingerprints for various dimensions and their combinations
	assert.Greater(t, len(stats.Fingerprints), 10, "Expected many unique fingerprints from comprehensive fingerprinting")

	assert.Equal(t, int64(1000), stats.FirstTS, "Expected first timestamp 1000")
	assert.Equal(t, int64(4000), stats.LastTS, "Expected last timestamp 4000")

	// Verify all fingerprints are unique (no duplicates)
	fingerprintSet := make(map[int64]bool)
	for _, fp := range stats.Fingerprints {
		assert.False(t, fingerprintSet[fp], "Found duplicate fingerprint: %d", fp)
		fingerprintSet[fp] = true
	}

	// Verify we have the expected number of unique fingerprints
	assert.Equal(t, len(stats.Fingerprints), len(fingerprintSet), "Fingerprints list should contain only unique values")
}
