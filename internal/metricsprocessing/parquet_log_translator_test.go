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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/translate"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestParquetLogTranslator_TranslateRow_NilRow(t *testing.T) {
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test.parquet",
	}

	err := translator.TranslateRow(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "row cannot be nil")
}

func TestParquetLogTranslator_TranslateRow_TimestampDetection(t *testing.T) {
	tests := []struct {
		name           string
		inputRow       filereader.Row
		expectedMs     int64
		expectedNs     int64
		shouldFindTime bool
	}{
		{
			name: "nanosecond precision timestamp",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp_ns"): int64(1234567890123456789),
				wkk.NewRowKey("message"):      "test message",
			},
			expectedMs:     1234567890123, // ns / 1000000
			expectedNs:     1234567890123456789,
			shouldFindTime: true,
		},
		{
			name: "microsecond precision timestamp",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp_us"): int64(1234567890123456),
				wkk.NewRowKey("message"):      "test message",
			},
			expectedMs:     1234567890123,       // us / 1000
			expectedNs:     1234567890123456000, // us * 1000
			shouldFindTime: true,
		},
		{
			name: "millisecond precision timestamp",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000, // ms * 1000000
			shouldFindTime: true,
		},
		{
			name: "time.Time object",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): time.Unix(1234567890, 123456789),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890123,       // UnixMilli()
			expectedNs:     1234567890123456789, // UnixNano()
			shouldFindTime: true,
		},
		{
			name: "@timestamp field",
			inputRow: filereader.Row{
				wkk.NewRowKey("@timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):    "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000,
			shouldFindTime: true,
		},
		{
			name: "no timestamp field - should use current time",
			inputRow: filereader.Row{
				wkk.NewRowKey("message"): "test message",
			},
			expectedMs:     0, // Will check that it's close to current time
			expectedNs:     0,
			shouldFindTime: false,
		},
		{
			name: "timestamp_millis field",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp_millis"): int64(1234567890123),
				wkk.NewRowKey("message"):          "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000,
			shouldFindTime: true,
		},
		{
			name: "float64 timestamp",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): float64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMs:     1234567890123,
			expectedNs:     1234567890123000000,
			shouldFindTime: true,
		},
		{
			name: "int32 timestamp",
			inputRow: filereader.Row{
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
			translator := &ParquetLogTranslator{
				OrgID:    "test-org",
				Bucket:   "test-bucket",
				ObjectID: "test.parquet",
			}

			// Create a copy of the row to avoid mutation
			row := make(filereader.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			beforeTranslate := time.Now()
			err := translator.TranslateRow(context.Background(), &row)
			afterTranslate := time.Now()
			require.NoError(t, err)

			// Check that timestamp was set correctly
			tsMs, exists := row[wkk.RowKeyCTimestamp]
			assert.True(t, exists, "Should have _cardinalhq_timestamp")

			tsNs, exists := row[wkk.NewRowKey("_cardinalhq_tsns")]
			assert.True(t, exists, "Should have _cardinalhq_tsns")

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
		inputRow        filereader.Row
		expectedMessage string
	}{
		{
			name: "message field",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
			},
			expectedMessage: "test message",
		},
		{
			name: "msg field",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("msg"):       "test msg",
			},
			expectedMessage: "test msg",
		},
		{
			name: "body field",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("body"):      "test body",
			},
			expectedMessage: "test body",
		},
		{
			name: "log field",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("log"):       "test log",
			},
			expectedMessage: "test log",
		},
		{
			name: "byte slice message",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   []byte("byte message"),
			},
			expectedMessage: "byte message",
		},
		{
			name: "no message field",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("level"):     "info",
			},
			expectedMessage: "",
		},
		{
			name: "empty message",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "",
			},
			expectedMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &ParquetLogTranslator{
				OrgID:    "test-org",
				Bucket:   "test-bucket",
				ObjectID: "test.parquet",
			}

			// Create a copy of the row
			row := make(filereader.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			err := translator.TranslateRow(context.Background(), &row)
			require.NoError(t, err)

			// Check message was set correctly
			messageKey := wkk.NewRowKey(translate.CardinalFieldMessage)
			msg, exists := row[messageKey]
			assert.True(t, exists, "Should have _cardinalhq_message")
			assert.Equal(t, tt.expectedMessage, msg)
		})
	}
}

func TestParquetLogTranslator_TranslateRow_NestedStructureFlattening(t *testing.T) {
	tests := []struct {
		name     string
		inputRow filereader.Row
		expected map[string]any
	}{
		{
			name: "nested map structure",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
				wkk.NewRowKey("metadata"): map[string]any{
					"user": "john",
					"details": map[string]any{
						"age":  30,
						"city": "NYC",
					},
				},
			},
			expected: map[string]any{
				"resource_metadata_user":         "john",
				"resource_metadata_details_age":  30,
				"resource_metadata_details_city": "NYC",
			},
		},
		{
			name: "array structure",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
				wkk.NewRowKey("tags"):      []any{"tag1", "tag2", "tag3"},
			},
			expected: map[string]any{
				"resource_tags[0]": "tag1",
				"resource_tags[1]": "tag2",
				"resource_tags[2]": "tag3",
			},
		},
		{
			name: "complex nested structure",
			inputRow: filereader.Row{
				wkk.NewRowKey("timestamp"): int64(1234567890123),
				wkk.NewRowKey("message"):   "test message",
				wkk.NewRowKey("data"): map[string]any{ // Changed from "event" to avoid message field detection
					"type": "click",
					"properties": map[string]any{
						"button": "submit",
						"coordinates": []any{
							map[string]any{"x": 100, "y": 200},
						},
					},
				},
			},
			expected: map[string]any{
				"resource_data_type":                        "click",
				"resource_data_properties_button":           "submit",
				"resource_data_properties_coordinates[0]_x": 100,
				"resource_data_properties_coordinates[0]_y": 200,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &ParquetLogTranslator{
				OrgID:    "test-org",
				Bucket:   "test-bucket",
				ObjectID: "test.parquet",
			}

			// Create a copy of the row
			row := make(filereader.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			err := translator.TranslateRow(context.Background(), &row)
			require.NoError(t, err)

			// Check that nested structures were flattened correctly
			for expectedKey, expectedValue := range tt.expected {
				key := wkk.NewRowKey(expectedKey)
				actualValue, exists := row[key]
				assert.True(t, exists, "Should have key %s", expectedKey)
				assert.Equal(t, expectedValue, actualValue, "Value mismatch for key %s", expectedKey)
			}

			// Verify original nested keys were removed
			if _, ok := tt.inputRow[wkk.NewRowKey("metadata")]; ok {
				_, exists := row[wkk.NewRowKey("metadata")]
				assert.False(t, exists, "Original nested 'metadata' key should be removed")
			}
			if _, ok := tt.inputRow[wkk.NewRowKey("tags")]; ok {
				_, exists := row[wkk.NewRowKey("tags")]
				assert.False(t, exists, "Original 'tags' array key should be removed")
			}
			if _, ok := tt.inputRow[wkk.NewRowKey("data")]; ok {
				_, exists := row[wkk.NewRowKey("data")]
				assert.False(t, exists, "Original nested 'data' key should be removed")
			}
		})
	}
}

func TestParquetLogTranslator_TranslateRow_SpecialFieldsNotDuplicated(t *testing.T) {
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test.parquet",
	}

	row := filereader.Row{
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

		// _cardinalhq fields should be skipped
		wkk.NewRowKey("_cardinalhq_test"): "skip me too",

		// resource fields should be kept as-is
		wkk.NewRowKey("resource_old"): "keep",
	}

	err := translator.TranslateRow(context.Background(), &row)
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

	// Check that level was correctly set in _cardinalhq_level
	cardinalLevel, hasCardinalLevel := row[wkk.NewRowKey("_cardinalhq_level")]
	assert.True(t, hasCardinalLevel, "Should have _cardinalhq_level")
	assert.Equal(t, "INFO", cardinalLevel, "Should have level as uppercase")

	service, hasService := row[wkk.NewRowKey("resource_service")]
	assert.True(t, hasService, "Should have resource.service attribute")
	assert.Equal(t, "api", service)
}

func TestParquetLogTranslator_TranslateRow_RequiredFields(t *testing.T) {
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "logs/2024/01/data.parquet",
	}

	row := filereader.Row{
		wkk.NewRowKey("timestamp"): int64(1234567890123),
		wkk.NewRowKey("message"):   "test message",
	}

	err := translator.TranslateRow(context.Background(), &row)
	require.NoError(t, err)

	// Check required CardinalhQ fields
	telemetryType, exists := row[wkk.RowKeyCTelemetryType]
	assert.True(t, exists, "Should have _cardinalhq_telemetry_type")
	assert.Equal(t, "logs", telemetryType)

	name, exists := row[wkk.RowKeyCName]
	assert.True(t, exists, "Should have _cardinalhq_name")
	assert.Equal(t, "log_events", name)

	// Check resource fields
	bucketName, exists := row[wkk.RowKeyResourceBucketName]
	assert.True(t, exists, "Should have resource_bucket_name")
	assert.Equal(t, "test-bucket", bucketName)

	fileName, exists := row[wkk.RowKeyResourceFileName]
	assert.True(t, exists, "Should have resource_file_name")
	assert.Equal(t, "./logs/2024/01/data.parquet", fileName)

	fileType, exists := row[wkk.RowKeyResourceFileType]
	assert.True(t, exists, "Should have resource_file_type")
	assert.Equal(t, "data", fileType) // GetFileType returns filename without extension, not the extension itself
}

func TestParquetLogTranslator_TranslateRow_EmptyKeyHandling(t *testing.T) {
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test.parquet",
	}

	// Create a row with an empty key (edge case)
	row := filereader.Row{
		wkk.NewRowKey("timestamp"): int64(1234567890123),
		wkk.NewRowKey("message"):   "test message",
		wkk.NewRowKey(""):          "value with empty key", // This should be skipped
	}

	err := translator.TranslateRow(context.Background(), &row)
	require.NoError(t, err)

	// The empty key should be skipped during processing
	// Verify the row still has valid fields
	_, hasTimestamp := row[wkk.RowKeyCTimestamp]
	assert.True(t, hasTimestamp, "Should have timestamp")

	messageKey := wkk.NewRowKey(translate.CardinalFieldMessage)
	_, hasMessage := row[messageKey]
	assert.True(t, hasMessage, "Should have message")
}

func TestParquetLogTranslator_TranslateRow_PreservesNonSpecialFields(t *testing.T) {
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test.parquet",
	}

	row := filereader.Row{
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

	err := translator.TranslateRow(context.Background(), &row)
	require.NoError(t, err)

	// Check that level was used for detection and not promoted to resource.level
	_, hasResourceLevel := row[wkk.NewRowKey("resource_level")]
	assert.False(t, hasResourceLevel, "Level field used for detection should not be promoted to resource.level")

	// Check that level was correctly set in _cardinalhq_level
	cardinalLevel, hasCardinalLevel := row[wkk.NewRowKey("_cardinalhq_level")]
	assert.True(t, hasCardinalLevel, "Should have _cardinalhq_level")
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
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "syslog.parquet",
	}

	// Test case matching the user's example (sanitized)
	row := filereader.Row{
		wkk.NewRowKey("timestamp"):         int64(1757812155208), // Original timestamp from syslog.parquet
		wkk.NewRowKey("application"):       "agent[1234]",
		wkk.NewRowKey("controller_ip"):     "10.0.0.1",
		wkk.NewRowKey("message"):           "2025-09-13 18:09:15 PDT | CORE | INFO | (pkg/logs/service.go:123 in handleLogRotation) | Log rotation happened to /var/log/system.log",
		wkk.NewRowKey("level"):             "INFO",
		wkk.NewRowKey("repeated_message"):  "(null)",
		wkk.NewRowKey("source"):            "192.168.1.100",
		wkk.NewRowKey("__index_level_0__"): int64(14),
	}

	err := translator.TranslateRow(context.Background(), &row)
	require.NoError(t, err)

	// Verify that the timestamp was correctly used for _cardinalhq_timestamp
	cardinalTimestamp, exists := row[wkk.RowKeyCTimestamp]
	assert.True(t, exists, "Should have _cardinalhq_timestamp")
	assert.Equal(t, int64(1757812155208), cardinalTimestamp, "Should preserve original timestamp, not use current time")

	// Verify that the timestamp field was NOT promoted to resource.timestamp
	_, hasResourceTimestamp := row[wkk.NewRowKey("resource_timestamp")]
	assert.False(t, hasResourceTimestamp, "Timestamp field used for detection should not be promoted to resource.timestamp")

	// Verify other fields are promoted correctly
	assert.Equal(t, "agent[1234]", row[wkk.NewRowKey("resource_application")])
	assert.Equal(t, "10.0.0.1", row[wkk.NewRowKey("resource_controller_ip")])
	assert.Equal(t, "192.168.1.100", row[wkk.NewRowKey("resource_source")])

	// Verify message was extracted correctly
	messageKey := wkk.NewRowKey(translate.CardinalFieldMessage)
	message, hasMessage := row[messageKey]
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
	cardinalLevel, hasLevel := row[wkk.NewRowKey("_cardinalhq_level")]
	assert.True(t, hasLevel, "Should have _cardinalhq_level")
	assert.Equal(t, "INFO", cardinalLevel, "Should preserve original level")
}

// mockStringTimestamp simulates Arrow scalar timestamps that render as human-readable strings
type mockStringTimestamp struct {
	value string
}

func (m mockStringTimestamp) String() string { return m.value }

func TestParquetLogTranslator_TranslateRow_AvoidStringTimestampParsing(t *testing.T) {
	translator := &ParquetLogTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test.parquet",
	}

	row := filereader.Row{
		wkk.NewRowKey("timestamp"):   mockStringTimestamp{value: "2025-09-13 18:09:15"},
		wkk.NewRowKey("application"): "test-app",
		wkk.NewRowKey("message"):     "test message",
	}

	err := translator.TranslateRow(context.Background(), &row)
	require.NoError(t, err)

	// Verify that the mock string timestamp was NOT used and we fell back to current time
	cardinalTimestamp, exists := row[wkk.RowKeyCTimestamp]
	assert.True(t, exists, "Should have _cardinalhq_timestamp")

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
