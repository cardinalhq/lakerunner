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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLokiSeriesResponse_Format(t *testing.T) {
	// Test that the response format matches Loki's expected format
	tests := []struct {
		name     string
		response LokiSeriesResponse
		expected string
	}{
		{
			name: "empty data",
			response: LokiSeriesResponse{
				Status: "success",
				Data:   []map[string]string{},
			},
			expected: `{"status":"success","data":[]}`,
		},
		{
			name: "single stream",
			response: LokiSeriesResponse{
				Status: "success",
				Data: []map[string]string{
					{"stream_id": "my-service"},
				},
			},
			expected: `{"status":"success","data":[{"stream_id":"my-service"}]}`,
		},
		{
			name: "multiple streams",
			response: LokiSeriesResponse{
				Status: "success",
				Data: []map[string]string{
					{"stream_id": "service-a"},
					{"stream_id": "service-b"},
					{"stream_id": "customer.domain.com"},
				},
			},
			expected: `{"status":"success","data":[{"stream_id":"service-a"},{"stream_id":"service-b"},{"stream_id":"customer.domain.com"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonBytes, err := json.Marshal(tt.response)
			require.NoError(t, err)
			assert.JSONEq(t, tt.expected, string(jsonBytes))
		})
	}
}

func TestLokiSeriesResponse_Unmarshal(t *testing.T) {
	// Test that clients can unmarshal the response correctly
	input := `{"status":"success","data":[{"stream_id":"service-a"},{"stream_id":"service-b"}]}`

	var response LokiSeriesResponse
	err := json.Unmarshal([]byte(input), &response)
	require.NoError(t, err)

	assert.Equal(t, "success", response.Status)
	require.Len(t, response.Data, 2)
	assert.Equal(t, "service-a", response.Data[0]["stream_id"])
	assert.Equal(t, "service-b", response.Data[1]["stream_id"])
}

func TestLogsSeriesPayload_Structure(t *testing.T) {
	// Verify the payload structure matches expected format
	payload := logsSeriesPayload{
		S: "2024-01-01T00:00:00Z",
		E: "2024-01-02T00:00:00Z",
	}

	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	// Should only include S and E (public fields)
	assert.JSONEq(t, `{"s":"2024-01-01T00:00:00Z","e":"2024-01-02T00:00:00Z"}`, string(jsonBytes))

	// Unmarshal back and verify
	var parsed logsSeriesPayload
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)
	assert.Equal(t, payload.S, parsed.S)
	assert.Equal(t, payload.E, parsed.E)
}
