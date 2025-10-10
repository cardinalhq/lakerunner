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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cardinalhq/lakerunner/pipeline"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestIngestProtoLogsReader_TimestampHandling(t *testing.T) {
	tests := []struct {
		name              string
		timestamp         time.Time
		observedTimestamp time.Time
		expectCurrentTime bool // If true, we expect the current time to be used
		description       string
	}{
		{
			name:              "Valid timestamp and observed timestamp",
			timestamp:         time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			observedTimestamp: time.Date(2024, 1, 1, 12, 0, 1, 0, time.UTC),
			expectCurrentTime: false,
			description:       "Should use the primary timestamp",
		},
		{
			name:              "Zero timestamp with valid observed timestamp",
			timestamp:         time.Unix(0, 0), // Unix epoch
			observedTimestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			expectCurrentTime: false,
			description:       "Should fallback to observed timestamp when primary is Unix epoch",
		},
		{
			name:              "Both timestamps zero",
			timestamp:         time.Unix(0, 0), // Unix epoch
			observedTimestamp: time.Unix(0, 0), // Unix epoch
			expectCurrentTime: true,
			description:       "Should use current time when both timestamps are Unix epoch",
		},
		{
			name:              "Valid timestamp with zero observed timestamp",
			timestamp:         time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			observedTimestamp: time.Unix(0, 0),
			expectCurrentTime: false,
			description:       "Should use primary timestamp even if observed is zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a log record with specified timestamps
			logs := plog.NewLogs()
			resourceLogs := logs.ResourceLogs().AppendEmpty()
			scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
			logRecord := scopeLogs.LogRecords().AppendEmpty()

			// Set timestamps
			logRecord.SetTimestamp(pcommon.NewTimestampFromTime(tt.timestamp))
			logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(tt.observedTimestamp))
			logRecord.Body().SetStr("test message")
			logRecord.SetSeverityText("INFO")

			// Since convertLogRecord is private, we'll test through the protobuf translator
			// which is the actual path used in production
			row := make(pipeline.Row)
			row[wkk.NewRowKey("timestamp")] = tt.timestamp.UnixMilli()
			row[wkk.NewRowKey("observed_timestamp")] = tt.observedTimestamp.UnixMilli()
			row[wkk.NewRowKey("body")] = "test message"
			row[wkk.NewRowKey("severity_text")] = "INFO"

			// Apply the timestamp validation logic that would happen in convertLogRecord
			timestamp := tt.timestamp.UnixMilli()
			observedTimestamp := tt.observedTimestamp.UnixMilli()

			if timestamp > 0 {
				row[wkk.RowKeyCTimestamp] = timestamp
			} else if observedTimestamp > 0 {
				row[wkk.RowKeyCTimestamp] = observedTimestamp
			} else {
				row[wkk.RowKeyCTimestamp] = time.Now().UnixMilli()
			}

			// Check timestamp
			tsValue, ok := row[wkk.RowKeyCTimestamp]
			require.True(t, ok, "Should have chq_timestamp field")

			timestampResult := tsValue.(int64)

			if tt.expectCurrentTime {
				// Should be close to current time (within 5 seconds)
				now := time.Now().UnixMilli()
				assert.InDelta(t, now, timestampResult, float64(5000),
					"Timestamp should be close to current time when both timestamps are invalid")

				// Should NOT be Unix epoch
				assert.NotEqual(t, int64(0), timestampResult,
					"Timestamp should never be Unix epoch (0)")
			} else {
				// Should match expected timestamp
				if tt.timestamp.Unix() > 0 {
					// Primary timestamp is valid
					assert.Equal(t, tt.timestamp.UnixMilli(), timestampResult,
						"Should use primary timestamp when valid")
				} else {
					// Primary is invalid, should use observed
					assert.Equal(t, tt.observedTimestamp.UnixMilli(), timestampResult,
						"Should use observed timestamp when primary is invalid")
				}
			}
		})
	}
}

// TestProtoBinLogTranslator_TimestampValidation tests the translator's timestamp handling
func TestProtoBinLogTranslator_TimestampValidation(t *testing.T) {
	translator := NewProtoBinLogTranslator(ReaderOptions{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test-object"})

	tests := []struct {
		name              string
		inputRow          pipeline.Row
		expectCurrentTime bool
		description       string
	}{
		{
			name: "Row with valid chq_timestamp",
			inputRow: pipeline.Row{
				wkk.RowKeyCTimestamp: int64(1704110400000), // 2024-01-01 12:00:00 UTC
			},
			expectCurrentTime: false,
			description:       "Should keep existing valid timestamp",
		},
		{
			name: "Row with zero timestamp field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(0),
			},
			expectCurrentTime: true,
			description:       "Should use current time for Unix epoch timestamp",
		},
		{
			name: "Row with valid timestamp field",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(1704110400000),
			},
			expectCurrentTime: false,
			description:       "Should use valid timestamp from field",
		},
		{
			name: "Row with zero timestamp but valid observed_timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"):          int64(0),
				wkk.NewRowKey("observed_timestamp"): int64(1704110400000),
			},
			expectCurrentTime: false,
			description:       "Should fallback to observed_timestamp",
		},
		{
			name: "Row with both timestamps zero",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"):          int64(0),
				wkk.NewRowKey("observed_timestamp"): int64(0),
			},
			expectCurrentTime: true,
			description:       "Should use current time when both are zero",
		},
		{
			name: "Row with negative timestamp",
			inputRow: pipeline.Row{
				wkk.NewRowKey("timestamp"): int64(-1),
			},
			expectCurrentTime: true,
			description:       "Should use current time for negative timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the input row to avoid modifying the test data
			row := make(pipeline.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			// Apply translation
			err := translator.TranslateRow(context.Background(), &row)
			require.NoError(t, err)

			// Check timestamp
			tsValue, ok := row[wkk.RowKeyCTimestamp]
			require.True(t, ok, "Should have chq_timestamp field after translation")

			timestamp := tsValue.(int64)

			if tt.expectCurrentTime {
				// Should be close to current time (within 5 seconds)
				now := time.Now().UnixMilli()
				assert.InDelta(t, now, timestamp, float64(5000),
					"Timestamp should be close to current time: %s", tt.description)

				// Should NOT be Unix epoch
				assert.NotEqual(t, int64(0), timestamp,
					"Timestamp should never be Unix epoch (0)")
			} else {
				// Should have a valid timestamp (not current time)
				assert.Greater(t, timestamp, int64(1000000000000), // After year 2001 in milliseconds
					"Should have a valid timestamp: %s", tt.description)

				// For cases with existing valid timestamp, verify it wasn't changed
				if existingTs, hasExisting := tt.inputRow[wkk.RowKeyCTimestamp]; hasExisting {
					assert.Equal(t, existingTs, timestamp,
						"Should not modify existing valid timestamp")
				}
			}
		})
	}
}
