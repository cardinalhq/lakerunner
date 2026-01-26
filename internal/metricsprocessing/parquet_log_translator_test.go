// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestParquetLogTranslator_TranslateRow_NilRow(t *testing.T) {
	err := translateParquetLogRow(context.Background(), nil, "test-bucket", "test.parquet")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "row cannot be nil")
}

func TestParquetLogTranslator_TranslateRow_TimestampDetection(t *testing.T) {
	tests := []struct {
		name           string
		inputRow       pipeline.Row
		expectedMs     int64
		expectedNs     int64
		shouldFindTime bool
	}{
		{
			name: "nanosecond precision timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp_ns"): int64(1234567890123456789),
				wkk.NewRowKey("message"):      "test message",
			},
			expectedMs:     1234567890123, // ns / 1000000
			expectedNs:     1234567890123456789,
			shouldFindTime: true,
		},
		{
			name: "microsecond precision timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp_us"): int64(1234567890123456),
				wkk.NewRowKey("message"):      "test message",
			},
			expectedMs:     1234567890123,       // us / 1000
			expectedNs:     1234567890123456000, // us * 1000
			shouldFindTime: true,
		},
		{
			name: "millisecond precision timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000, // ms * 1000000
			shouldFindTime: true,
		},
		{
			name: "time.Time object",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): time.Unix(1234567890, 123456789),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890123,       // UnixMilli()
			expectedNs:     1234567890123456789, // UnixNano()
			shouldFindTime: true,
		},
		{
			name: "@timestamp field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("@timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):    "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000,
			shouldFindTime: true,
		},
		{
			name: "no timestamp field - should use current time",
			inputRow: pipeline.Row{
				wkk.NewRowKey("message"): "test message",
			},
			expectedMs:     0, // Will check that it's close to current time
			expectedNs:     0,
			shouldFindTime: false,
		},
		{
			name: "timestamp_millis field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp_millis"): int64(1234567890123),
				wkk.NewRowKey("message"):          "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000,
			shouldFindTime: true,
		},
		{
			name: "float64 timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): float64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000,
			shouldFindTime: true,
		},
		{
			name: "int32 timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int32(1234567890),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890,
			expectedNs:     1234567890000000,
			shouldFindTime: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Create a copy of the row to avoid mutation
			row := make(pipeline.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			beforeTranslate := time.Now()
			err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
			afterTranslate := time.Now()
			require.NoError(t, err)

			// Check that timestamp was set correctly
			tsMs, exists := row[wkk.RowKeyCTimestamp]
			assert.True(t, exists, "Should have chq_timestamp")

			tsNs, exists := row[wkk.NewRowKey("chq_tsns")]
			assert.True(t, exists, "Should have chq_tsns")

			if tt.shouldFindTime {
				assert.Equal(t, tt.expectedMs, tsMs, "Millisecond timestamp mismatch")
				assert.Equal(t, tt.expectedNs, tsNs, "Nanosecond timestamp mismatch")
			} else {
				// For cases without timestamp, check it's close to current time
				actualMs := tsMs.(int64)
				actualNs := tsNs.(int64)

				beforeMs := beforeTranslate.UnixMilli()
				afterMs := afterTranslate.UnixMilli()

				assert.GreaterOrEqual(t, actualMs, beforeMs)
				assert.LessOrEqual(t, actualMs, afterMs)

				// Check nanosecond consistency
				assert.GreaterOrEqual(t, actualNs, beforeTranslate.UnixNano())
				assert.LessOrEqual(t, actualNs, afterTranslate.UnixNano())
			}
		})
	}
}

func TestParquetLogTranslator_TranslateRow_MessageDetection(t *testing.T) {
	tests := []struct {
		name            string
		inputRow        pipeline.Row
		expectedMessage string
	}{
		{
			name: "message field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMessage: "test message",
		},
		{
			name: "msg field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("msg"):       "test msg",
			},
			expectedMessage: "test msg",
		},
		{
			name: "body field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("body"):      "test body",
			},
			expectedMessage: "test body",
		},
		{
			name: "log field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("log"):       "test log",
			},
			expectedMessage: "test log",
		},
		{
			name: "byte slice message",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   []byte("byte message"),
			},
			expectedMessage: "byte message",
		},
		{
			name: "no message field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("level"):     "info",
			},
			expectedMessage: "",
		},
		{
			name: "empty message",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "",
			},
			expectedMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Create a copy of the row
			row := make(pipeline.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
			require.NoError(t, err)

			// Check message was set correctly
			msg, exists := row[wkk.RowKeyCMessage]
			assert.True(t, exists, "Should have log_message")
			assert.Equal(t, tt.expectedMessage, msg)
		})
	}
}

func TestParquetLogTranslator_TranslateRow_FlattenedFields(t *testing.T) {
	tests := []struct {
		name     string
		inputRow pipeline.Row
		expected map[string]any
	}{
		{
			name: "already flattened fields from reader",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"):      int64(1234567890123),
				wkk.NewRowKey("message"):        "test message",
				wkk.NewRowKey("metadata_user"):  "john",
				wkk.NewRowKey("metadata_age"):   int64(30),
				wkk.NewRowKey("metadata_city"):  "NYC",
				wkk.NewRowKey("service_name"):   "api",
				wkk.NewRowKey("service_region"): "us-west-2",
			},
			expected: map[string]any{
				"resource_metadata_user":  "john",
				"resource_metadata_age":   int64(30),
				"resource_metadata_city":  "NYC",
				"resource_service_name":   "api",
				"resource_service_region": "us-west-2",
			},
		},
		{
			name: "special fields are not prefixed",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"):     int64(1234567890123),
				wkk.NewRowKey("message"):       "test message",
				wkk.NewRowKey("chq_timestamp"): int64(1234567890123),
				wkk.NewRowKey("chq_message"):   "test message",
				wkk.NewRowKey("other_field"):   "value",
			},
			expected: map[string]any{
				"resource_other_field": "value",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Create a copy of the row
			row := make(pipeline.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
			require.NoError(t, err)

			// Check that fields were prefixed correctly
			for expectedKey, expectedValue := range tt.expected {
				key := wkk.NewRowKey(expectedKey)
				actualValue, exists := row[key]
				assert.True(t, exists, "Should have key %s", expectedKey)
				assert.Equal(t, expectedValue, actualValue, "Value mismatch for key %s", expectedKey)
			}

			// Verify original keys were removed (fields that get resource_ prefix)
			for k := range tt.inputRow {
				keyStr := wkk.RowKeyValue(k)
				// Special fields and already-prefixed fields should remain unchanged
				if keyStr == "timestamp" || keyStr == "message" ||
					strings.HasPrefix(keyStr, "chq_") ||
					strings.HasPrefix(keyStr, "resource_") ||
					strings.HasPrefix(keyStr, "_") {
					continue
				}
				// Other fields should be removed (they now have resource_ prefix)
				_, exists := row[k]
				assert.False(t, exists, "Original key %s should be removed after adding resource_ prefix", keyStr)
			}
		})
	}
}

func TestParquetLogTranslator_TranslateRow_SpecialFieldsNotDuplicated(t *testing.T) {

	row := pipeline.Row{
		// These should be detected and not duplicated as attributes
		wkk.NewRowKey("timestamp"):  int64(1234567890123),
		wkk.NewRowKey("message"):    "test message",
		wkk.NewRowKey("@timestamp"): int64(9999999999999), // Should be ignored as duplicate
		wkk.NewRowKey("msg"):        "duplicate msg",      // Should be ignored as duplicate

		// These should remain as attributes
		wkk.NewRowKey("level"):   "info",
		wkk.NewRowKey("service"): "api",

		// Fields starting with underscore should be skipped
		wkk.NewRowKey("_internal"): "skip me",

		// chq fields should be skipped
		wkk.NewRowKey("chq_test"): "skip me too",

		// resource fields should be kept as-is
		wkk.NewRowKey("resource_old"): "keep",
	}

	err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
	require.NoError(t, err)

	// Check that special fields were not duplicated as regular attributes
	_, hasTimestamp := row[wkk.NewRowKey("@timestamp")]
	assert.False(t, hasTimestamp, "@timestamp should not be in final row")

	_, hasMsg := row[wkk.NewRowKey("msg")]
	assert.False(t, hasMsg, "msg should not be in final row")

	// Check that underscore fields were skipped
	_, hasInternal := row[wkk.NewRowKey("_internal")]
	assert.False(t, hasInternal, "_internal should be skipped")

	// Check that existing resource field was kept
	oldResource, hasOldResource := row[wkk.NewRowKey("resource_old")]
	assert.True(t, hasOldResource, "resource_old should be kept")
	assert.Equal(t, "keep", oldResource)

	// Check that level was used for detection and not promoted to resource.level
	_, hasLevel := row[wkk.NewRowKey("resource_level")]
	assert.False(t, hasLevel, "Level field used for detection should not be promoted to resource.level")

	// Check that level was correctly set in log_level
	cardinalLevel, hasCardinalLevel := row[wkk.NewRowKey("log_level")]
	assert.True(t, hasCardinalLevel, "Should have log_level")
	assert.Equal(t, "INFO", cardinalLevel, "Should have level as uppercase")

	service, hasService := row[wkk.NewRowKey("resource_service")]
	assert.True(t, hasService, "Should have resource.service attribute")
	assert.Equal(t, "api", service)
}

func TestParquetLogTranslator_TranslateRow_RequiredFields(t *testing.T) {

	row := pipeline.Row{
		wkk.NewRowKey("timestamp"): int64(1234567890123),
		wkk.NewRowKey("message"):   "test message",
	}

	// Use a specific objectID to verify resource fields are set correctly
	objectID := "logs/2024/01/data.parquet"
	err := translateParquetLogRow(context.Background(), &row, "test-bucket", objectID)
	require.NoError(t, err)

	// Check required CardinalhQ fields
	telemetryType, exists := row[wkk.RowKeyCTelemetryType]
	assert.True(t, exists, "Should have chq_telemetry_type")
	assert.Equal(t, "logs", telemetryType)

	name, exists := row[wkk.RowKeyCName]
	assert.True(t, exists, "Should have metric_name")
	assert.Equal(t, "log_events", name)

	// Check resource fields
	bucketName, exists := row[wkk.RowKeyResourceBucketName]
	assert.True(t, exists, "Should have resource_bucket_name")
	assert.Equal(t, "test-bucket", bucketName)

	fileName, exists := row[wkk.RowKeyResourceFileName]
	assert.True(t, exists, "Should have resource_file_name")
	assert.Equal(t, "./"+objectID, fileName)

	fileType, exists := row[wkk.RowKeyResourceFileType]
	assert.True(t, exists, "Should have resource_file_type")
	assert.Equal(t, "data", fileType) // GetFileType returns filename without extension, not the extension itself
}

func TestParquetLogTranslator_TranslateRow_EmptyKeyHandling(t *testing.T) {

	// Create a row with an empty key (edge case)
	row := pipeline.Row{
		wkk.NewRowKey("timestamp"): int64(1234567890123),
		wkk.NewRowKey("message"):   "test message",
		wkk.NewRowKey(""):          "value with empty key", // This should be skipped
	}

	err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
	require.NoError(t, err)

	// The empty key should be skipped during processing
	// Verify the row still has valid fields
	_, hasTimestamp := row[wkk.RowKeyCTimestamp]
	assert.True(t, hasTimestamp, "Should have timestamp")

	_, hasMessage := row[wkk.RowKeyCMessage]
	assert.True(t, hasMessage, "Should have message")
}

func TestParquetLogTranslator_TranslateRow_PreservesNonSpecialFields(t *testing.T) {

	row := pipeline.Row{
		wkk.NewRowKey("timestamp"):    int64(1234567890123),
		wkk.NewRowKey("message"):      "test message",
		wkk.NewRowKey("level"):        "error",
		wkk.NewRowKey("service_name"): "auth-service",
		wkk.NewRowKey("trace_id"):     "abc123",
		wkk.NewRowKey("span_id"):      "def456",
		wkk.NewRowKey("http_method"):  "POST",
		wkk.NewRowKey("http_status"):  int64(500),
		wkk.NewRowKey("duration_ms"):  float64(123.45),
	}

	err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
	require.NoError(t, err)

	// Check that level was used for detection and not promoted to resource.level
	_, hasResourceLevel := row[wkk.NewRowKey("resource_level")]
	assert.False(t, hasResourceLevel, "Level field used for detection should not be promoted to resource.level")

	// Check that level was correctly set in log_level
	cardinalLevel, hasCardinalLevel := row[wkk.NewRowKey("log_level")]
	assert.True(t, hasCardinalLevel, "Should have log_level")
	assert.Equal(t, "ERROR", cardinalLevel, "Should have level as uppercase")

	// Check that all other non-special fields are preserved with resource_ prefix
	assert.Equal(t, "auth-service", row[wkk.NewRowKey("resource_service_name")])
	assert.Equal(t, "abc123", row[wkk.NewRowKey("resource_trace_id")])
	assert.Equal(t, "def456", row[wkk.NewRowKey("resource_span_id")])
	assert.Equal(t, "POST", row[wkk.NewRowKey("resource_http_method")])
	assert.Equal(t, int64(500), row[wkk.NewRowKey("resource_http_status")])
	assert.Equal(t, float64(123.45), row[wkk.NewRowKey("resource_duration_ms")])
}

func TestParquetLogTranslator_TranslateRow_TimestampFieldNotPromoted(t *testing.T) {

	// Test case matching the user's example (sanitized)
	row := pipeline.Row{
		wkk.NewRowKey("timestamp"):         int64(1757812155208), // Original timestamp from syslog.parquet
		wkk.NewRowKey("application"):       "agent[1234]",
		wkk.NewRowKey("controller_ip"):     "10.0.0.1",
		wkk.NewRowKey("message"):           "2025-09-13 18:09:15 PDT | CORE | INFO | (pkg/logs/service.go:123 in handleLogRotation) | Log rotation happened to /var/log/system.log",
		wkk.NewRowKey("level"):             "INFO",
		wkk.NewRowKey("repeated_message"):  "(null)",
		wkk.NewRowKey("source"):            "192.168.1.100",
		wkk.NewRowKey("__index_level_0__"): int64(14),
	}

	err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
	require.NoError(t, err)

	// Verify that the timestamp was correctly used for chq_timestamp
	cardinalTimestamp, exists := row[wkk.RowKeyCTimestamp]
	assert.True(t, exists, "Should have chq_timestamp")
	assert.Equal(t, int64(1757812155208), cardinalTimestamp, "Should preserve original timestamp, not use current time")

	// Verify that the timestamp field was NOT promoted to resource.timestamp
	_, hasResourceTimestamp := row[wkk.NewRowKey("resource_timestamp")]
	assert.False(t, hasResourceTimestamp, "Timestamp field used for detection should not be promoted to resource.timestamp")

	// Verify other fields are promoted correctly
	assert.Equal(t, "agent[1234]", row[wkk.NewRowKey("resource_application")])
	assert.Equal(t, "10.0.0.1", row[wkk.NewRowKey("resource_controller_ip")])
	assert.Equal(t, "192.168.1.100", row[wkk.NewRowKey("resource_source")])

	// Verify message was extracted correctly
	message, hasMessage := row[wkk.RowKeyCMessage]
	assert.True(t, hasMessage, "Should have extracted message")
	expectedMessage := "2025-09-13 18:09:15 PDT | CORE | INFO | (pkg/logs/service.go:123 in handleLogRotation) | Log rotation happened to /var/log/system.log"
	assert.Equal(t, expectedMessage, message)

	// Verify that the message field was NOT promoted to resource.message (it was used for extraction)
	_, hasResourceMessage := row[wkk.NewRowKey("resource_message")]
	assert.False(t, hasResourceMessage, "Message field used for detection should not be promoted to resource.message")

	// Verify that the level field was NOT promoted to resource.level (it was used for detection)
	_, hasResourceLevel := row[wkk.NewRowKey("resource_level")]
	assert.False(t, hasResourceLevel, "Level field used for detection should not be promoted to resource.level")

	// Verify that the level was correctly set
	cardinalLevel, hasLevel := row[wkk.NewRowKey("log_level")]
	assert.True(t, hasLevel, "Should have log_level")
	assert.Equal(t, "INFO", cardinalLevel, "Should preserve original level")
}

// mockStringTimestamp simulates Arrow scalar timestamps that render as human-readable strings
type mockStringTimestamp struct {
	value string
}

func (m mockStringTimestamp) String() string { return m.value }

func TestParquetLogTranslator_TranslateRow_AvoidStringTimestampParsing(t *testing.T) {

	row := pipeline.Row{
		wkk.NewRowKey("timestamp"):   mockStringTimestamp{value: "2025-09-13 18:09:15"},
		wkk.NewRowKey("application"): "test-app",
		wkk.NewRowKey("message"):     "test message",
	}

	err := translateParquetLogRow(context.Background(), &row, "test-bucket", "test.parquet")
	require.NoError(t, err)

	// Verify that the mock string timestamp was NOT used and we fell back to current time
	cardinalTimestamp, exists := row[wkk.RowKeyCTimestamp]
	assert.True(t, exists, "Should have chq_timestamp")

	// The timestamp should be recent (current time), not 2025 milliseconds since epoch
	timestamp := cardinalTimestamp.(int64)
	now := time.Now().UnixMilli()
	assert.Greater(t, timestamp, int64(1600000000000), "Should use current time, not parse string as number")
	assert.Less(t, timestamp-now, int64(5000), "Should be within 5 seconds of current time")

	// Verify that the mock timestamp was promoted to resource attribute since it wasn't used
	mockTimestampValue, hasMockTimestamp := row[wkk.NewRowKey("resource_timestamp")]
	assert.True(t, hasMockTimestamp, "Mock timestamp should be promoted to resource attribute")
	assert.Equal(t, mockStringTimestamp{value: "2025-09-13 18:09:15"}, mockTimestampValue)
}
