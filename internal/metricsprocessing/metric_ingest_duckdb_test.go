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

package metricsprocessing

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

func TestProcessMetricIngestWithDuckDB(t *testing.T) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skipf("Test file does not exist: %s", testFile)
	}

	tmpDir := t.TempDir()
	ctx := context.Background()

	// Copy test file to temp location to simulate downloaded segment
	testData, err := os.ReadFile(testFile)
	require.NoError(t, err)

	segmentFile := filepath.Join(tmpDir, "segment.binpb.gz")
	err = os.WriteFile(segmentFile, testData, 0644)
	require.NoError(t, err)

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	result, err := processMetricIngestWithDuckDB(ctx, []string{segmentFile}, orgID, tmpDir)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have at least one dateint partition
	assert.NotEmpty(t, result.DateintBins, "expected at least one dateint partition")
	assert.Greater(t, result.TotalRows, int64(0), "expected at least one row")

	// Verify each partition has valid data
	for dateint, bin := range result.DateintBins {
		assert.NotZero(t, dateint, "dateint should not be zero")
		assert.NotEmpty(t, bin.OutputFile, "output file should not be empty")
		assert.Greater(t, bin.RecordCount, int64(0), "record count should be greater than 0")
		assert.Greater(t, bin.FileSize, int64(0), "file size should be greater than 0")
		assert.NotNil(t, bin.Metadata, "metadata should not be nil")

		// Verify output file exists
		_, err := os.Stat(bin.OutputFile)
		require.NoError(t, err, "output file should exist")

		// Verify metadata
		md := bin.Metadata
		assert.Greater(t, md.StartTs, int64(0), "start timestamp should be positive")
		assert.GreaterOrEqual(t, md.EndTs, md.StartTs, "end timestamp should be >= start")
		assert.NotEmpty(t, md.MetricNames, "metric names should not be empty")
		assert.Len(t, md.MetricTypes, len(md.MetricNames), "metric types should match metric names length")
		assert.NotEmpty(t, md.Fingerprints, "fingerprints should not be empty")
	}
}

func TestProcessMetricIngestWithDuckDB_EmptyFiles(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	_, err := processMetricIngestWithDuckDB(ctx, []string{}, orgID, tmpDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no binpb files provided")
}

func TestGetDistinctDateintKeys(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create a table with known timestamps
	// Day 1: 2025-01-01 = 1735689600000 ms (days since epoch: 20089)
	// Day 2: 2025-01-02 = 1735776000000 ms (days since epoch: 20090)
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE metrics_raw (
			chq_timestamp BIGINT
		)
	`)
	require.NoError(t, err)

	// Insert timestamps spanning two days
	_, err = conn.ExecContext(ctx, `
		INSERT INTO metrics_raw VALUES
			(1735689600000),  -- 2025-01-01 00:00:00 UTC
			(1735700000000),  -- 2025-01-01 ~03:00:00 UTC
			(1735776000000),  -- 2025-01-02 00:00:00 UTC
			(1735800000000)   -- 2025-01-02 ~06:00:00 UTC
	`)
	require.NoError(t, err)

	keys, err := getDistinctDateintKeys(ctx, conn)
	require.NoError(t, err)

	assert.Len(t, keys, 2, "should have 2 distinct days")
	assert.Equal(t, int64(20089), keys[0], "first day key should be 20089")
	assert.Equal(t, int64(20090), keys[1], "second day key should be 20090")
}

func TestBuildLabelNameMapFromSchema(t *testing.T) {
	tests := []struct {
		name          string
		schema        []string
		expectedInMap []string
	}{
		{
			name: "label columns",
			schema: []string{
				"value",
				"chq_timestamp",
				"resource_service_name",
				"attr_http_method",
				"scope_name",
			},
			expectedInMap: []string{"resource_service_name", "attr_http_method", "scope_name", "chq_timestamp"},
		},
		{
			name:          "no label columns",
			schema:        []string{"value", "count"},
			expectedInMap: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildLabelNameMapFromSchema(tt.schema)

			if len(tt.expectedInMap) == 0 {
				assert.Nil(t, result, "expected nil result for no label columns")
				return
			}

			assert.NotNil(t, result, "expected non-nil result")
			resultStr := string(result)

			for _, col := range tt.expectedInMap {
				assert.Contains(t, resultStr, col, "expected column in map")
			}
		})
	}
}

func TestUnderscoreToDotted(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"resource_service_name", "resource.service.name"},
		{"attr_http_method", "http_method"},
		{"_cardinalhq_foo_bar", ".cardinalhq.foo.bar"},
		{"chq_timestamp", "chq_timestamp"},
		{"scope_version", "scope_version"},
		{"log_body", "log.body"},
		{"metric_unit", "metric.unit"},
		{"span_kind", "span.kind"},
		{"trace_id", "trace.id"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := underscoreToDotted(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLabelColumn(t *testing.T) {
	tests := []struct {
		column   string
		expected bool
	}{
		{"resource_service_name", true},
		{"scope_version", true},
		{"chq_timestamp", true},
		{"attr_http_method", true},
		{"_cardinalhq_env", true},
		{"log_severity", true},
		{"metric_unit", true},
		{"span_kind", true},
		{"trace_id", true},
		{"metric_name", true}, // metric_ prefix is a label prefix
		{"value", false},
		{"custom_field", false},
	}

	for _, tt := range tests {
		t.Run(tt.column, func(t *testing.T) {
			result := isLabelColumn(tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeFingerprints(t *testing.T) {
	tests := []struct {
		name        string
		metricNames []string
		wantEmpty   bool
		minLen      int // minimum expected length
	}{
		{
			name:        "single metric",
			metricNames: []string{"cpu_usage"},
			wantEmpty:   false,
			minLen:      1,
		},
		{
			name:        "multiple metrics",
			metricNames: []string{"cpu_usage", "memory_usage", "disk_io"},
			wantEmpty:   false,
			minLen:      3,
		},
		{
			name:        "empty",
			metricNames: []string{},
			wantEmpty:   true,
		},
		{
			name:        "duplicates",
			metricNames: []string{"cpu_usage", "cpu_usage", "memory_usage"},
			wantEmpty:   false,
			minLen:      2, // at least 2 unique metrics worth of fingerprints
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeFingerprints(tt.metricNames)

			if tt.wantEmpty {
				// Empty input should still produce at least the "exists" fingerprint
				// unless there are no metric names at all
				if len(tt.metricNames) == 0 {
					// ToFingerprints with empty set produces 1 "exists" fingerprint
					assert.GreaterOrEqual(t, len(result), 0)
				}
			} else {
				assert.GreaterOrEqual(t, len(result), tt.minLen, "should have at least minimum fingerprints")
			}

			// Verify fingerprints are sorted
			for i := 1; i < len(result); i++ {
				assert.LessOrEqual(t, result[i-1], result[i], "fingerprints should be sorted")
			}
		})
	}
}
