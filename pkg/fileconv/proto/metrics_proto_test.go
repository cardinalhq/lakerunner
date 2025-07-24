// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"testing"

	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			reader, err := NewMetricsProtoReader(tt.fname, tt.mapper, tt.tags)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, reader)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reader)
				defer reader.Close()
			}
		})
	}
}

func TestMetricsProtoReader_GetRow(t *testing.T) {
	reader, err := NewMetricsProtoReader("testdata/metrics_449638969.binpb", translate.NewMapper(), map[string]string{
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
	reader, err := NewMetricsProtoReader("testdata/metrics_449638969.binpb", translate.NewMapper(), nil)
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
	reader, err := NewMetricsProtoReader("testdata/nonexistent.binpb", translate.NewMapper(), nil)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestMetricsProtoReader_DifferentMetricTypes(t *testing.T) {
	reader, err := NewMetricsProtoReader("testdata/metrics_449638969.binpb", translate.NewMapper(), nil)
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
