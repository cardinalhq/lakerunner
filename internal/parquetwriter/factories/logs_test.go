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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestLogsStatsAccumulator_StreamFieldTracking(t *testing.T) {
	customerDomainField := wkk.RowKeyValue(wkk.RowKeyResourceCustomerDomain)
	serviceNameField := wkk.RowKeyValue(wkk.RowKeyResourceServiceName)

	tests := []struct {
		name                      string
		rows                      []pipeline.Row
		expectedStreamIdField     *string
		expectedStreamValues      []string
		expectedMissingFieldCount int64
	}{
		{
			name: "tracks resource_customer_domain",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedStreamIdField:     stringPtr(customerDomainField),
			expectedStreamValues:      []string{"example.com"},
			expectedMissingFieldCount: 0,
		},
		{
			name: "tracks resource_service_name when no customer_domain",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceServiceName: "my-service"},
			},
			expectedStreamIdField:     stringPtr(serviceNameField),
			expectedStreamValues:      []string{"my-service"},
			expectedMissingFieldCount: 0,
		},
		{
			name: "customer_domain takes priority over service_name",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceServiceName: "my-service"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedStreamIdField:     stringPtr(customerDomainField),
			expectedStreamValues:      []string{"example.com"},
			expectedMissingFieldCount: 0,
		},
		{
			name: "tracks multiple unique values for same field",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyResourceCustomerDomain: "other.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890125), wkk.RowKeyResourceCustomerDomain: "example.com"}, // duplicate
			},
			expectedStreamIdField:     stringPtr(customerDomainField),
			expectedStreamValues:      []string{"example.com", "other.com"},
			expectedMissingFieldCount: 0,
		},
		{
			name: "clears service_name values when customer_domain appears",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceServiceName: "service-a"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyResourceServiceName: "service-b"},
				{wkk.RowKeyCTimestamp: int64(1234567890125), wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedStreamIdField:     stringPtr(customerDomainField),
			expectedStreamValues:      []string{"example.com"}, // service-a and service-b cleared
			expectedMissingFieldCount: 0,
		},
		{
			name: "ignores service_name after customer_domain seen",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyResourceServiceName: "my-service"}, // ignored
			},
			expectedStreamIdField:     stringPtr(customerDomainField),
			expectedStreamValues:      []string{"example.com"},
			expectedMissingFieldCount: 0,
		},
		{
			name: "counts missing fields",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123)}, // missing both
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890125)}, // missing both
			},
			expectedStreamIdField:     stringPtr(customerDomainField),
			expectedStreamValues:      []string{"example.com"},
			expectedMissingFieldCount: 2,
		},
		{
			name: "all rows missing fields",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123)},
				{wkk.RowKeyCTimestamp: int64(1234567890124)},
			},
			expectedStreamIdField:     nil,
			expectedStreamValues:      nil,
			expectedMissingFieldCount: 2,
		},
		{
			name: "ignores empty string values",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyResourceCustomerDomain: ""},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyResourceServiceName: ""},
			},
			expectedStreamIdField:     nil,
			expectedStreamValues:      nil,
			expectedMissingFieldCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &LogsStatsProvider{}
			acc := provider.NewAccumulator()

			for _, row := range tt.rows {
				acc.Add(row)
			}

			result := acc.Finalize()
			stats, ok := result.(LogsFileStats)
			require.True(t, ok, "Finalize should return LogsFileStats")

			// Check stream field
			if tt.expectedStreamIdField == nil {
				assert.Nil(t, stats.StreamIdField, "StreamIdField should be nil")
			} else {
				require.NotNil(t, stats.StreamIdField, "StreamIdField should not be nil")
				assert.Equal(t, *tt.expectedStreamIdField, *stats.StreamIdField, "StreamIdField mismatch")
			}

			// Check stream values
			if tt.expectedStreamValues == nil {
				assert.Nil(t, stats.StreamValues, "StreamValues should be nil")
			} else {
				actualValues := stats.StreamValues
				sort.Strings(actualValues)
				expectedValues := make([]string, len(tt.expectedStreamValues))
				copy(expectedValues, tt.expectedStreamValues)
				sort.Strings(expectedValues)
				assert.Equal(t, expectedValues, actualValues, "StreamValues mismatch")
			}

			// Check missing field count
			assert.Equal(t, tt.expectedMissingFieldCount, stats.MissingStreamFieldCount, "MissingStreamFieldCount mismatch")
		})
	}
}

func TestLogsStatsAccumulator_FingerprintsAndTimestamps(t *testing.T) {
	provider := &LogsStatsProvider{}
	acc := provider.NewAccumulator()

	// Add rows with timestamps
	rows := []pipeline.Row{
		{wkk.RowKeyCTimestamp: int64(1234567890100), wkk.RowKeyResourceServiceName: "service-a"},
		{wkk.RowKeyCTimestamp: int64(1234567890200), wkk.RowKeyResourceServiceName: "service-b"},
		{wkk.RowKeyCTimestamp: int64(1234567890050), wkk.RowKeyResourceServiceName: "service-c"}, // Earlier timestamp
	}

	for _, row := range rows {
		acc.Add(row)
	}

	result := acc.Finalize()
	stats, ok := result.(LogsFileStats)
	require.True(t, ok, "Finalize should return LogsFileStats")

	// Verify timestamp tracking
	assert.Equal(t, int64(1234567890050), stats.FirstTS, "FirstTS should be minimum timestamp")
	assert.Equal(t, int64(1234567890200), stats.LastTS, "LastTS should be maximum timestamp")
}

func TestLogsFileStats_Structure(t *testing.T) {
	field := "resource_service_name"
	stats := LogsFileStats{
		FirstTS:                 1234567890000,
		LastTS:                  1234567890999,
		Fingerprints:            []int64{100, 200, 300},
		StreamIdField:           &field,
		StreamValues:            []string{"service-a", "service-b"},
		MissingStreamFieldCount: 5,
	}

	assert.Equal(t, int64(1234567890000), stats.FirstTS)
	assert.Equal(t, int64(1234567890999), stats.LastTS)
	assert.Len(t, stats.Fingerprints, 3)
	require.NotNil(t, stats.StreamIdField)
	assert.Equal(t, "resource_service_name", *stats.StreamIdField)
	assert.Len(t, stats.StreamValues, 2)
	assert.Contains(t, stats.StreamValues, "service-a")
	assert.Contains(t, stats.StreamValues, "service-b")
	assert.Equal(t, int64(5), stats.MissingStreamFieldCount)
}

func TestLogsStatsAccumulator_AggregationCounts(t *testing.T) {
	tests := []struct {
		name           string
		rows           []pipeline.Row
		expectedCounts map[LogAggKey]int64
	}{
		{
			name: "single row single bucket",
			rows: []pipeline.Row{
				{
					wkk.RowKeyCTimestamp:             int64(1234567890000), // bucket: 1234567890000
					wkk.RowKeyCLevel:                 "info",
					wkk.RowKeyResourceCustomerDomain: "example.com",
				},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}: 1,
			},
		},
		{
			name: "multiple rows same bucket",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890001), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567899999), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}: 3,
			},
		},
		{
			name: "different buckets (10s apart)",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567900000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}: 1,
				{TimestampBucket: 1234567900000, LogLevel: "info", StreamId: "example.com"}: 1,
			},
		},
		{
			name: "different log levels",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890001), wkk.RowKeyCLevel: "error", wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890002), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}:  2,
				{TimestampBucket: 1234567890000, LogLevel: "error", StreamId: "example.com"}: 1,
			},
		},
		{
			name: "different stream ids",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "example.com"},
				{wkk.RowKeyCTimestamp: int64(1234567890001), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "other.com"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "example.com"}: 1,
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "other.com"}:   1,
			},
		},
		{
			name: "uses service_name when no customer_domain",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceServiceName: "my-service"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "my-service"}: 1,
			},
		},
		{
			name: "empty log level",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyResourceCustomerDomain: "example.com"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "", StreamId: "example.com"}: 1,
			},
		},
		{
			name: "empty stream id",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info"},
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: ""}: 1,
			},
		},
		{
			name: "bucket boundary test - 10s = 10000ms",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(9999), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "a.com"},  // bucket 0
				{wkk.RowKeyCTimestamp: int64(10000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "a.com"}, // bucket 10000
				{wkk.RowKeyCTimestamp: int64(19999), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "a.com"}, // bucket 10000
				{wkk.RowKeyCTimestamp: int64(20000), wkk.RowKeyCLevel: "info", wkk.RowKeyResourceCustomerDomain: "a.com"}, // bucket 20000
			},
			expectedCounts: map[LogAggKey]int64{
				{TimestampBucket: 0, LogLevel: "info", StreamId: "a.com"}:     1,
				{TimestampBucket: 10000, LogLevel: "info", StreamId: "a.com"}: 2,
				{TimestampBucket: 20000, LogLevel: "info", StreamId: "a.com"}: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &LogsStatsProvider{}
			acc := provider.NewAccumulator()

			for _, row := range tt.rows {
				acc.Add(row)
			}

			result := acc.Finalize()
			stats, ok := result.(LogsFileStats)
			require.True(t, ok, "Finalize should return LogsFileStats")

			assert.Equal(t, len(tt.expectedCounts), len(stats.AggCounts), "AggCounts length mismatch")
			for key, expectedCount := range tt.expectedCounts {
				actualCount, exists := stats.AggCounts[key]
				assert.True(t, exists, "Missing key: %+v", key)
				assert.Equal(t, expectedCount, actualCount, "Count mismatch for key %+v", key)
			}
		})
	}
}

func TestLogsStatsAccumulator_AggregationWithConfiguredStreamField(t *testing.T) {
	// Test aggregation when a specific stream field is configured
	provider := &LogsStatsProvider{StreamField: "resource_k8s_namespace_name"}
	acc := provider.NewAccumulator()

	namespaceKey := wkk.NewRowKey("resource_k8s_namespace_name")
	rows := []pipeline.Row{
		{wkk.RowKeyCTimestamp: int64(1234567890000), wkk.RowKeyCLevel: "info", namespaceKey: "production"},
		{wkk.RowKeyCTimestamp: int64(1234567890001), wkk.RowKeyCLevel: "info", namespaceKey: "staging"},
		{wkk.RowKeyCTimestamp: int64(1234567890002), wkk.RowKeyCLevel: "info", namespaceKey: "production"},
	}

	for _, row := range rows {
		acc.Add(row)
	}

	result := acc.Finalize()
	stats, ok := result.(LogsFileStats)
	require.True(t, ok)

	expectedCounts := map[LogAggKey]int64{
		{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "production"}: 2,
		{TimestampBucket: 1234567890000, LogLevel: "info", StreamId: "staging"}:    1,
	}

	assert.Equal(t, len(expectedCounts), len(stats.AggCounts))
	for key, expectedCount := range expectedCounts {
		assert.Equal(t, expectedCount, stats.AggCounts[key], "Count mismatch for key %+v", key)
	}
}

func TestGetAggFields(t *testing.T) {
	tests := []struct {
		name        string
		streamField string
		expected    []string
	}{
		{
			name:        "empty stream field uses default",
			streamField: "",
			expected:    []string{"log_level", "resource_customer_domain"},
		},
		{
			name:        "custom stream field",
			streamField: "resource_service_name",
			expected:    []string{"log_level", "resource_service_name"},
		},
		{
			name:        "k8s namespace field",
			streamField: "resource_k8s_namespace_name",
			expected:    []string{"log_level", "resource_k8s_namespace_name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetAggFields(tt.streamField)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
