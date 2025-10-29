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

package queryapi

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyEvent_JSONSerialization_WithNaN(t *testing.T) {
	tests := []struct {
		name        string
		value       float64
		expectError bool
		errorMsg    string
	}{
		{
			name:        "normal value serializes successfully",
			value:       42.5,
			expectError: false,
		},
		{
			name:        "zero value serializes successfully",
			value:       0.0,
			expectError: false,
		},
		{
			name:        "negative value serializes successfully",
			value:       -123.456,
			expectError: false,
		},
		{
			name:        "NaN value fails to serialize",
			value:       math.NaN(),
			expectError: true,
			errorMsg:    "json: unsupported value: NaN",
		},
		{
			name:        "positive infinity fails to serialize",
			value:       math.Inf(1),
			expectError: true,
			errorMsg:    "json: unsupported value: +Inf",
		},
		{
			name:        "negative infinity fails to serialize",
			value:       math.Inf(-1),
			expectError: true,
			errorMsg:    "json: unsupported value: -Inf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := LegacyEvent{
				ID:   "test-query",
				Type: "timeseries",
				Message: LegacyMessage{
					Timestamp: 1234567890,
					Value:     tt.value,
					Tags: map[string]any{
						"service": "test-service",
					},
				},
			}

			data, err := json.Marshal(event)

			if tt.expectError {
				require.Error(t, err, "expected JSON marshaling to fail for %s", tt.name)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, data)
			} else {
				require.NoError(t, err, "expected JSON marshaling to succeed for %s", tt.name)
				assert.NotNil(t, data)

				// Verify we can unmarshal it back
				var decoded LegacyEvent
				err = json.Unmarshal(data, &decoded)
				require.NoError(t, err)
				assert.Equal(t, event.ID, decoded.ID)
				assert.Equal(t, event.Type, decoded.Type)
				assert.Equal(t, event.Message.Timestamp, decoded.Message.Timestamp)
				assert.Equal(t, event.Message.Value, decoded.Message.Value)
			}
		})
	}
}

func TestToLegacyTimeseriesEvent_Structure(t *testing.T) {
	denormalizer := NewLabelDenormalizer(nil)

	event := ToLegacyTimeseriesEvent(
		"query-123",
		456,
		1234567890,
		42.5,
		map[string]any{
			"service_name": "test-service",
			"status_code":  200,
		},
		denormalizer,
	)

	assert.Equal(t, "query-123", event.ID)
	assert.Equal(t, "timeseries", event.Type)
	assert.Equal(t, int64(1234567890), event.Message.Timestamp)
	assert.Equal(t, 42.5, event.Message.Value)

	// Verify the event can be marshaled to JSON
	data, err := json.Marshal(event)
	require.NoError(t, err)
	assert.NotNil(t, data)
}

func TestToLegacySSEEvent_Structure(t *testing.T) {
	denormalizer := NewLabelDenormalizer(nil)

	event := ToLegacySSEEvent(
		"query-456",
		789,
		1234567890,
		1.0,
		map[string]any{
			"service_name": "test-service",
			"message":      "test log message",
		},
		denormalizer,
	)

	assert.Equal(t, "query-456", event.ID)
	assert.Equal(t, "event", event.Type)
	assert.Equal(t, int64(1234567890), event.Message.Timestamp)
	assert.Equal(t, 1.0, event.Message.Value)

	// Verify the event can be marshaled to JSON
	data, err := json.Marshal(event)
	require.NoError(t, err)
	assert.NotNil(t, data)
}

func TestMathIsNaN_BehaviorDocumentation(t *testing.T) {
	// This test documents the behavior of math.IsNaN() which we rely on
	// to filter out NaN values before JSON serialization in both:
	// - querier.go:184 (PromQL handler)
	// - legacy_graph.go:228 (Legacy graph handler)

	assert.True(t, math.IsNaN(math.NaN()), "math.IsNaN should detect NaN")
	assert.False(t, math.IsNaN(0.0), "math.IsNaN should not detect zero")
	assert.False(t, math.IsNaN(42.5), "math.IsNaN should not detect normal values")
	assert.False(t, math.IsNaN(math.Inf(1)), "math.IsNaN should not detect positive infinity")
	assert.False(t, math.IsNaN(math.Inf(-1)), "math.IsNaN should not detect negative infinity")

	// Document that Infinity also fails JSON serialization
	// but our current fix only handles NaN
	_, err := json.Marshal(map[string]float64{"value": math.Inf(1)})
	assert.Error(t, err, "JSON marshaling should fail for +Inf")
	assert.Contains(t, err.Error(), "json: unsupported value: +Inf")
}

func TestSkippingNaN_DoesNotCauseTimestampMisalignment(t *testing.T) {
	// This test proves that skipping NaN values does NOT cause data point shifting
	// because each event carries its own timestamp - they're not array indices.
	//
	// When we skip a NaN value at timestamp T2, the next event still has its
	// correct timestamp T3, not T2.

	denormalizer := NewLabelDenormalizer(nil)

	// Simulate a timeseries with 4 data points where the 2nd one has NaN
	dataPoints := []struct {
		timestamp int64
		value     float64
	}{
		{timestamp: 1000, value: 10.0},
		{timestamp: 2000, value: math.NaN()}, // This will be skipped
		{timestamp: 3000, value: 30.0},
		{timestamp: 4000, value: 40.0},
	}

	var emittedEvents []LegacyEvent

	// Simulate the loop in legacy_graph.go that skips NaN values
	for _, dp := range dataPoints {
		if math.IsNaN(dp.value) {
			continue
		}

		event := ToLegacyTimeseriesEvent(
			"test-query",
			123,
			dp.timestamp,
			dp.value,
			map[string]any{"service": "test"},
			denormalizer,
		)
		emittedEvents = append(emittedEvents, event)
	}

	// Verify we only got 3 events (NaN was skipped)
	require.Len(t, emittedEvents, 3, "should have 3 events after skipping NaN")

	// CRITICAL: Verify that timestamps are NOT shifted
	// Each event should have its ORIGINAL timestamp, not a shifted one
	assert.Equal(t, int64(1000), emittedEvents[0].Message.Timestamp, "first event should have timestamp 1000")
	assert.Equal(t, 10.0, emittedEvents[0].Message.Value)

	// The second event should be the one from timestamp 3000, NOT 2000
	assert.Equal(t, int64(3000), emittedEvents[1].Message.Timestamp, "second event should have timestamp 3000 (not 2000)")
	assert.Equal(t, 30.0, emittedEvents[1].Message.Value)

	// The third event should still be from timestamp 4000
	assert.Equal(t, int64(4000), emittedEvents[2].Message.Timestamp, "third event should have timestamp 4000")
	assert.Equal(t, 40.0, emittedEvents[2].Message.Value)

	// Verify all events can be serialized to JSON
	for i, event := range emittedEvents {
		data, err := json.Marshal(event)
		require.NoError(t, err, "event %d should serialize to JSON", i)
		assert.NotNil(t, data)

		// Verify we can unmarshal and timestamps are preserved
		var decoded LegacyEvent
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, event.Message.Timestamp, decoded.Message.Timestamp, "timestamp should survive round-trip")
	}
}

func TestSkippingNaN_ClientPerspective(t *testing.T) {
	// This test demonstrates what the client receives when NaN values are skipped.
	// The client gets a sparse timeseries with gaps, which is correct behavior.

	denormalizer := NewLabelDenormalizer(nil)

	// Timeseries with gaps due to NaN values
	dataPoints := []struct {
		timestamp int64
		value     float64
		shouldEmit bool
	}{
		{timestamp: 1000, value: 10.0, shouldEmit: true},
		{timestamp: 2000, value: math.NaN(), shouldEmit: false},
		{timestamp: 3000, value: math.NaN(), shouldEmit: false},
		{timestamp: 4000, value: 40.0, shouldEmit: true},
		{timestamp: 5000, value: 50.0, shouldEmit: true},
	}

	var clientReceivedEvents []map[string]any

	// Simulate what gets sent to the client
	for _, dp := range dataPoints {
		if !dp.shouldEmit {
			continue
		}

		event := ToLegacyTimeseriesEvent(
			"test-query",
			123,
			dp.timestamp,
			dp.value,
			map[string]any{"service": "test"},
			denormalizer,
		)

		// Serialize to JSON (what client receives)
		jsonData, err := json.Marshal(event)
		require.NoError(t, err)

		var decoded map[string]any
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		clientReceivedEvents = append(clientReceivedEvents, decoded)
	}

	// Client receives 3 events with correct timestamps
	require.Len(t, clientReceivedEvents, 3)

	// Verify the client can see the gaps by looking at timestamps
	message0 := clientReceivedEvents[0]["message"].(map[string]any)
	message1 := clientReceivedEvents[1]["message"].(map[string]any)
	message2 := clientReceivedEvents[2]["message"].(map[string]any)

	assert.Equal(t, float64(1000), message0["timestamp"].(float64))
	assert.Equal(t, float64(4000), message1["timestamp"].(float64)) // Gap: 2000 and 3000 are missing
	assert.Equal(t, float64(5000), message2["timestamp"].(float64))

	// The client can detect the gap: timestamp jumped from 1000 to 4000
	gap := message1["timestamp"].(float64) - message0["timestamp"].(float64)
	assert.Equal(t, float64(3000), gap, "client can see there's a 3000ms gap (2 missing data points)")
}
