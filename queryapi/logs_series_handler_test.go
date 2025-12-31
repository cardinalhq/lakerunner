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

	"github.com/cardinalhq/lakerunner/logql"
)

func TestLokiSeriesResponse_Format(t *testing.T) {
	// Test that the response format uses actual field names as keys
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
			name: "single stream with service_name",
			response: LokiSeriesResponse{
				Status: "success",
				Data: []map[string]string{
					{"resource_service_name": "my-service"},
				},
			},
			expected: `{"status":"success","data":[{"resource_service_name":"my-service"}]}`,
		},
		{
			name: "single stream with customer_domain",
			response: LokiSeriesResponse{
				Status: "success",
				Data: []map[string]string{
					{"resource_customer_domain": "example.com"},
				},
			},
			expected: `{"status":"success","data":[{"resource_customer_domain":"example.com"}]}`,
		},
		{
			name: "multiple streams with same field",
			response: LokiSeriesResponse{
				Status: "success",
				Data: []map[string]string{
					{"resource_service_name": "service-a"},
					{"resource_service_name": "service-b"},
					{"resource_service_name": "service-c"},
				},
			},
			expected: `{"status":"success","data":[{"resource_service_name":"service-a"},{"resource_service_name":"service-b"},{"resource_service_name":"service-c"}]}`,
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
	// Test that clients can unmarshal the response correctly with actual field names
	input := `{"status":"success","data":[{"resource_service_name":"service-a"},{"resource_service_name":"service-b"}]}`

	var response LokiSeriesResponse
	err := json.Unmarshal([]byte(input), &response)
	require.NoError(t, err)

	assert.Equal(t, "success", response.Status)
	require.Len(t, response.Data, 2)
	assert.Equal(t, "service-a", response.Data[0]["resource_service_name"])
	assert.Equal(t, "service-b", response.Data[1]["resource_service_name"])
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

func TestLogsSeriesPayload_WithQ(t *testing.T) {
	// Verify the payload structure with Q parameter
	payload := logsSeriesPayload{
		S: "2024-01-01T00:00:00Z",
		E: "2024-01-02T00:00:00Z",
		Q: `{resource_service_name="api-server"}`,
	}

	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	assert.JSONEq(t, `{"s":"2024-01-01T00:00:00Z","e":"2024-01-02T00:00:00Z","q":"{resource_service_name=\"api-server\"}"}`, string(jsonBytes))
}

func TestMatchesSeries_EqualityMatch(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		value     string
		matchers  []logql.LabelMatch
		expected  bool
	}{
		{
			name:      "exact match",
			fieldName: "resource_service_name",
			value:     "api-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchEq, Value: "api-server"},
			},
			expected: true,
		},
		{
			name:      "no match - different value",
			fieldName: "resource_service_name",
			value:     "web-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchEq, Value: "api-server"},
			},
			expected: false,
		},
		{
			name:      "no match - different field",
			fieldName: "resource_customer_domain",
			value:     "example.com",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchEq, Value: "api-server"},
			},
			expected: true, // matcher doesn't apply to this field
		},
		{
			name:      "empty matchers - matches all",
			fieldName: "resource_service_name",
			value:     "any-value",
			matchers:  []logql.LabelMatch{},
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesSeries(tt.fieldName, tt.value, tt.matchers)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchesSeries_NotEqualMatch(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		value     string
		matchers  []logql.LabelMatch
		expected  bool
	}{
		{
			name:      "not equal - matches different value",
			fieldName: "resource_service_name",
			value:     "web-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchNe, Value: "api-server"},
			},
			expected: true,
		},
		{
			name:      "not equal - rejects same value",
			fieldName: "resource_service_name",
			value:     "api-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchNe, Value: "api-server"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesSeries(tt.fieldName, tt.value, tt.matchers)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchesSeries_RegexMatch(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		value     string
		matchers  []logql.LabelMatch
		expected  bool
	}{
		{
			name:      "regex match - prefix",
			fieldName: "resource_service_name",
			value:     "api-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchRe, Value: "api-.*"},
			},
			expected: true,
		},
		{
			name:      "regex match - no match",
			fieldName: "resource_service_name",
			value:     "web-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchRe, Value: "api-.*"},
			},
			expected: false,
		},
		{
			name:      "regex not match - excludes pattern",
			fieldName: "resource_service_name",
			value:     "web-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchNre, Value: "api-.*"},
			},
			expected: true,
		},
		{
			name:      "regex not match - rejects matching pattern",
			fieldName: "resource_service_name",
			value:     "api-server",
			matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchNre, Value: "api-.*"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesSeries(tt.fieldName, tt.value, tt.matchers)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchesSeries_MultipleMatchers(t *testing.T) {
	// All matchers for the same field must match (AND logic)
	matchers := []logql.LabelMatch{
		{Label: "resource_service_name", Op: logql.MatchRe, Value: "api-.*"},
		{Label: "resource_service_name", Op: logql.MatchNe, Value: "api-test"},
	}

	// Matches: starts with "api-" AND is not "api-test"
	assert.True(t, matchesSeries("resource_service_name", "api-server", matchers))
	assert.True(t, matchesSeries("resource_service_name", "api-prod", matchers))

	// Doesn't match: is "api-test"
	assert.False(t, matchesSeries("resource_service_name", "api-test", matchers))

	// Doesn't match: doesn't start with "api-"
	assert.False(t, matchesSeries("resource_service_name", "web-server", matchers))
}
