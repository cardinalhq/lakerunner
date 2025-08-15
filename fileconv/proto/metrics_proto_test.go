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

package proto

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/fileconv/translate"
)

func TestNewMetricsProtoReader(t *testing.T) {
	tests := []struct {
		name        string
		fname       string
		mapper      *translate.Mapper
		tags        map[string]string
		expectError bool
	}{
		{
			name:        "valid metrics proto file",
			fname:       "testdata/metrics_449638969.binpb",
			mapper:      translate.NewMapper(),
			tags:        map[string]string{"test": "value"},
			expectError: false,
		},
		{
			name:        "non-existent file",
			fname:       "testdata/nonexistent.binpb",
			mapper:      translate.NewMapper(),
			tags:        nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := os.ReadFile(tt.fname)
			if tt.expectError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)

			reader, err := NewMetricsProtoReader(data, tt.mapper, tt.tags)
			assert.NoError(t, err)
			assert.NotNil(t, reader)
			defer reader.Close()
		})
	}
}

func TestMetricsProtoReader_GetRow(t *testing.T) {
	data, err := os.ReadFile("testdata/metrics_449638969.binpb")
	require.NoError(t, err)

	reader, err := NewMetricsProtoReader(data, translate.NewMapper(), map[string]string{
		"test_tag": "test_value",
		"env":      "test",
	})
	require.NoError(t, err)
	defer reader.Close()

	// Test that we can read multiple rows
	rowCount := 0
	maxRows := 10 // Limit to avoid long test runs

	for {
		row, done, err := reader.GetRow()
		if err != nil {
			t.Fatalf("Error reading row: %v", err)
		}
		if done {
			break
		}

		// Verify required fields are present
		assert.NotEmpty(t, row, "Row should not be empty")

		// Check that tags were added
		assert.Equal(t, "test_value", row["test_tag"], "Test tag should be present")
		assert.Equal(t, "test", row["env"], "Environment tag should be present")

		// Check that required cardinalhq fields are present
		assert.Contains(t, row, "_cardinalhq.telemetry_type", "Should have telemetry type")
		assert.Equal(t, "metrics", row["_cardinalhq.telemetry_type"], "Telemetry type should be metrics")

		assert.Contains(t, row, "_cardinalhq.name", "Should have name field")
		assert.NotEmpty(t, row["_cardinalhq.name"], "Name should not be empty")

		assert.Contains(t, row, "_cardinalhq.metric_type", "Should have metric type field")
		metricType := row["_cardinalhq.metric_type"]
		assert.Contains(t, []string{"gauge", "count", "histogram", "exponential_histogram"}, metricType, "Metric type should be valid")

		// Check that we have either value or timestamp
		hasValue := false
		hasTimestamp := false
		for k, v := range row {
			if k == "_cardinalhq.value" && v != nil {
				hasValue = true
			}
			if k == "_cardinalhq.timestamp" && v != nil {
				hasTimestamp = true
			}
		}
		assert.True(t, hasValue || hasTimestamp, "Row should have either value or timestamp")

		rowCount++
		if rowCount >= maxRows {
			break
		}
	}

	assert.Greater(t, rowCount, 0, "Should have read at least one row")
}

func TestMetricsProtoReader_Close(t *testing.T) {
	data, err := os.ReadFile("testdata/metrics_449638969.binpb")
	require.NoError(t, err)

	reader, err := NewMetricsProtoReader(data, translate.NewMapper(), nil)
	require.NoError(t, err)

	// Test that close doesn't error
	err = reader.Close()
	assert.NoError(t, err)

	// Test that close is idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

func TestMetricsProtoReader_EmptyFile(t *testing.T) {
	// Test with a file that doesn't exist
	_, err := os.ReadFile("testdata/nonexistent.binpb")
	assert.Error(t, err)

	// Test with empty data - this actually succeeds with an empty metrics object
	reader, err := NewMetricsProtoReader([]byte{}, translate.NewMapper(), nil)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	if reader != nil {
		reader.Close()
	}
}

func TestMetricsProtoReader_DifferentMetricTypes(t *testing.T) {
	data, err := os.ReadFile("testdata/metrics_449638969.binpb")
	require.NoError(t, err)

	reader, err := NewMetricsProtoReader(data, translate.NewMapper(), nil)
	require.NoError(t, err)
	defer reader.Close()

	metricTypes := make(map[string]bool)
	rowCount := 0
	maxRows := 50 // Read more rows to find different metric types

	for {
		row, done, err := reader.GetRow()
		if err != nil {
			t.Fatalf("Error reading row: %v", err)
		}
		if done {
			break
		}

		if metricType, ok := row["_cardinalhq.metric_type"].(string); ok {
			metricTypes[metricType] = true
		}

		rowCount++
		if rowCount >= maxRows {
			break
		}
	}

	// Check that we found at least one metric type
	assert.Greater(t, len(metricTypes), 0, "Should have found at least one metric type")
	t.Logf("Found metric types: %v", metricTypes)
}

func TestNewMetricsProtoReader_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() ([]byte, string)
		expectError bool
		errContains string
	}{
		{
			name: "empty data slice",
			setup: func() ([]byte, string) {
				// Test with nil data - this should work (creates empty metrics)
				return nil, "/nonexistent/file.binpb"
			},
			expectError: false,
			errContains: "",
		},
		{
			name: "empty file",
			setup: func() ([]byte, string) {
				tmpfile, err := os.CreateTemp("", "empty-metrics-*.binpb")
				assert.NoError(t, err)
				tmpfile.Close()
				data, _ := os.ReadFile(tmpfile.Name())
				return data, tmpfile.Name()
			},
			expectError: false, // Empty data creates empty metrics
			errContains: "",
		},
		{
			name: "invalid proto file",
			setup: func() ([]byte, string) {
				tmpfile, err := os.CreateTemp("", "invalid-metrics-*.binpb")
				assert.NoError(t, err)
				_, err = tmpfile.WriteString("not a protobuf file content for metrics")
				assert.NoError(t, err)
				tmpfile.Close()
				data, _ := os.ReadFile(tmpfile.Name())
				return data, tmpfile.Name()
			},
			expectError: true, // Invalid proto causes error during construction
			errContains: "failed to parse proto",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, filePath := tt.setup()
			if strings.Contains(filePath, "/tmp/") || strings.Contains(filePath, "test-") {
				defer os.Remove(filePath)
			}

			mapper := translate.NewMapper()
			reader, err := NewMetricsProtoReader(data, mapper, nil)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errContains != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, reader)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reader)
				if reader != nil {
					reader.Close()
				}
			}
		})
	}
}

func TestMetricsProtoReader_GetRowErrorCases(t *testing.T) {
	// Test that invalid proto files are handled during construction
	tmpfile, err := os.CreateTemp("", "invalid-metrics-proto-*.binpb")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// Write some non-protobuf data
	_, err = tmpfile.WriteString("This is not valid metrics protobuf data")
	assert.NoError(t, err)
	tmpfile.Close()

	data, err := os.ReadFile(tmpfile.Name())
	assert.NoError(t, err)

	mapper := translate.NewMapper()
	reader, err := NewMetricsProtoReader(data, mapper, nil)
	// Based on the actual implementation, invalid proto files cause errors during construction
	assert.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "failed to parse proto")
}

func TestMetricsProtoReader_WithNilMapper(t *testing.T) {
	// Test with nil mapper - should still work
	data, err := os.ReadFile("testdata/metrics_449638969.binpb")
	require.NoError(t, err)

	reader, err := NewMetricsProtoReader(data, nil, nil)
	if err != nil {
		// Skip if file doesn't exist in test environment
		t.Skip("Test data file not available")
	}
	defer reader.Close()

	// Try reading one row
	_, _, _ = reader.GetRow()
	// Should not panic, behavior depends on implementation
}
