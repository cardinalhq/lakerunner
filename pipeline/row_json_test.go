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

package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestMarshalRowJSON(t *testing.T) {
	tests := []struct {
		name     string
		row      Row
		expected string
	}{
		{
			name:     "empty row",
			row:      Row{},
			expected: `{}`,
		},
		{
			name: "simple row with string values",
			row: Row{
				wkk.NewRowKey("name"):    "test",
				wkk.NewRowKey("service"): "api",
			},
			expected: `{"name":"test","service":"api"}`,
		},
		{
			name: "row with various value types",
			row: Row{
				wkk.NewRowKey("string"): "value",
				wkk.NewRowKey("int"):    42,
				wkk.NewRowKey("float"):  3.14,
				wkk.NewRowKey("bool"):   true,
				wkk.NewRowKey("null"):   nil,
				wkk.RowKeyCName:         "metric_name",
				wkk.RowKeyCMetricType:   "gauge",
				wkk.RowKeyCTimestamp:    int64(1234567890),
			},
			expected: `{"chq_metric_type":"gauge","metric_name":"metric_name","chq_timestamp":1234567890,"bool":true,"float":3.14,"int":42,"null":null,"string":"value"}`,
		},
		{
			name: "row with nested structures",
			row: Row{
				wkk.NewRowKey("array"):  []any{"a", "b", "c"},
				wkk.NewRowKey("object"): map[string]any{"key": "value"},
			},
			expected: `{"array":["a","b","c"],"object":{"key":"value"}}`,
		},
		{
			name: "row with underscore-prefixed keys",
			row: Row{
				wkk.NewRowKey("resource_service_name"):       "test-service",
				wkk.NewRowKey("resource_k8s_cluster_name"):   "prod-cluster",
				wkk.NewRowKey("resource_k8s_namespace_name"): "default",
				wkk.NewRowKey("chq_fingerprint"):             int64(123456),
				wkk.NewRowKey("metric_label"):                "value",
			},
			expected: `{"chq_fingerprint":123456,"metric_label":"value","resource_k8s_cluster_name":"prod-cluster","resource_k8s_namespace_name":"default","resource_service_name":"test-service"}`,
		},
		{
			name: "row with special characters in keys",
			row: Row{
				wkk.NewRowKey("key with spaces"):     "value1",
				wkk.NewRowKey("key\"with\"quotes"):   "value2",
				wkk.NewRowKey("key\\with\\slashes"):  "value3",
				wkk.NewRowKey("key\nwith\nnewlines"): "value4",
				wkk.NewRowKey("key\twith\ttabs"):     "value5",
			},
			expected: `{"key with spaces":"value1","key\"with\"quotes":"value2","key\\with\\slashes":"value3","key\nwith\nnewlines":"value4","key\twith\ttabs":"value5"}`,
		},
		{
			name: "row with special characters in values",
			row: Row{
				wkk.NewRowKey("quotes"):   "value \"with\" quotes",
				wkk.NewRowKey("slashes"):  "value\\with\\slashes",
				wkk.NewRowKey("newlines"): "value\nwith\nnewlines",
				wkk.NewRowKey("tabs"):     "value\twith\ttabs",
				wkk.NewRowKey("unicode"):  "value with 🔥 emoji",
			},
			expected: `{"quotes":"value \"with\" quotes","slashes":"value\\with\\slashes","newlines":"value\nwith\nnewlines","tabs":"value\twith\ttabs","unicode":"value with 🔥 emoji"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.row.Marshal()
			require.NoError(t, err)
			assert.JSONEq(t, tt.expected, string(data))
		})
	}
}

func TestUnmarshalRowJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Row
	}{
		{
			name:     "empty object",
			input:    `{}`,
			expected: Row{},
		},
		{
			name:  "simple object with string values",
			input: `{"name":"test","service":"api"}`,
			expected: Row{
				wkk.NewRowKey("name"):    "test",
				wkk.NewRowKey("service"): "api",
			},
		},
		{
			name:  "object with various value types",
			input: `{"string":"value","int":42,"float":3.14,"bool":true,"null":null}`,
			expected: Row{
				wkk.NewRowKey("string"): "value",
				wkk.NewRowKey("int"):    float64(42), // JSON numbers unmarshal as float64
				wkk.NewRowKey("float"):  3.14,
				wkk.NewRowKey("bool"):   true,
				wkk.NewRowKey("null"):   nil,
			},
		},
		{
			name:  "object with nested structures",
			input: `{"array":["a","b","c"],"object":{"key":"value"}}`,
			expected: Row{
				wkk.NewRowKey("array"):  []any{"a", "b", "c"},
				wkk.NewRowKey("object"): map[string]any{"key": "value"},
			},
		},
		{
			name:  "object with underscore-prefixed keys",
			input: `{"resource_service_name":"test-service","chq_fingerprint":123456}`,
			expected: Row{
				wkk.NewRowKey("resource_service_name"): "test-service",
				wkk.NewRowKey("chq_fingerprint"):       float64(123456),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var row Row
			err := row.Unmarshal([]byte(tt.input))
			require.NoError(t, err)
			assert.Equal(t, len(tt.expected), len(row))
			for k, expectedValue := range tt.expected {
				actualValue, ok := row[k]
				assert.True(t, ok, "key %s not found in result", wkk.RowKeyValue(k))
				assert.Equal(t, expectedValue, actualValue, "value mismatch for key %s", wkk.RowKeyValue(k))
			}
		})
	}
}

func TestRowJSON_RoundTrip(t *testing.T) {
	original := Row{
		wkk.NewRowKey("resource_service_name"):       "test-service",
		wkk.NewRowKey("resource_k8s_cluster_name"):   "prod-cluster",
		wkk.NewRowKey("resource_k8s_namespace_name"): "default",
		wkk.RowKeyCName:                          "http_requests_total",
		wkk.RowKeyCMetricType:                    "count",
		wkk.RowKeyCTimestamp:                     int64(1234567890),
		wkk.NewRowKey("chq_fingerprint"):         int64(987654321),
		wkk.NewRowKey("metric_http_status_code"): "200",
		wkk.NewRowKey("metric_method"):           "GET",
	}

	// Marshal
	data, err := original.Marshal()
	require.NoError(t, err)

	// Unmarshal
	var row Row
	err = row.Unmarshal(data)
	require.NoError(t, err)

	// Verify all keys are present (they should be the same unique.Handle instances)
	assert.Equal(t, len(original), len(row))
	for k := range original {
		_, ok := row[k]
		assert.True(t, ok, "key %s not found after round trip", wkk.RowKeyValue(k))
	}
}

func TestRowJSON_InvalidJSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "invalid JSON syntax",
			input: `{invalid}`,
		},
		{
			name:  "not an object",
			input: `["array"]`,
		},
		{
			name:  "truncated JSON",
			input: `{"name":"test"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var row Row
			err := row.Unmarshal([]byte(tt.input))
			assert.Error(t, err)
		})
	}
}

func TestRowJSON_KeyInterning(t *testing.T) {
	// Create two rows from the same JSON
	input := `{"resource_service_name":"test","metric_name":"metric"}`

	var row1 Row
	err := row1.Unmarshal([]byte(input))
	require.NoError(t, err)

	var row2 Row
	err = row2.Unmarshal([]byte(input))
	require.NoError(t, err)

	// The keys should be the same interned instances
	for k1 := range row1 {
		found := false
		for k2 := range row2 {
			if k1 == k2 {
				found = true
				break
			}
		}
		assert.True(t, found, "key %s should be interned and equal", wkk.RowKeyValue(k1))
	}
}

func BenchmarkMarshalRowJSON(b *testing.B) {
	row := Row{
		wkk.NewRowKey("resource_service_name"):       "test-service",
		wkk.NewRowKey("resource_k8s_cluster_name"):   "prod-cluster",
		wkk.NewRowKey("resource_k8s_namespace_name"): "default",
		wkk.RowKeyCName:                          "http_requests_total",
		wkk.RowKeyCMetricType:                    "count",
		wkk.RowKeyCTimestamp:                     int64(1234567890),
		wkk.RowKeyCFingerprint:                   int64(987654321),
		wkk.NewRowKey("metric_http_status_code"): "200",
		wkk.NewRowKey("metric_method"):           "GET",
	}

	for b.Loop() {
		_, err := row.Marshal()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalRowJSON(b *testing.B) {
	input := []byte(`{"resource_service_name":"test-service","resource_k8s_cluster_name":"prod-cluster","resource_k8s_namespace_name":"default","metric_name":"http_requests_total","chq_metric_type":"count","chq_timestamp":1234567890,"chq_fingerprint":987654321,"metric_http_status_code":"200","metric_method":"GET"}`)

	for b.Loop() {
		var row Row
		err := row.Unmarshal(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestRow_DirectJSONMarshal(t *testing.T) {
	// Test that Row works directly with json.Marshal (no helper function needed)
	row := Row{
		wkk.NewRowKey("name"):    "test",
		wkk.NewRowKey("service"): "api",
	}

	data, err := json.Marshal(row)
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"test","service":"api"}`, string(data))
}

func TestRow_DirectJSONUnmarshal(t *testing.T) {
	// Test that Row works directly with json.Unmarshal (no helper function needed)
	input := []byte(`{"name":"test","service":"api"}`)
	var row Row
	err := json.Unmarshal(input, &row)
	require.NoError(t, err)

	assert.Equal(t, "test", row[wkk.NewRowKey("name")])
	assert.Equal(t, "api", row[wkk.NewRowKey("service")])
}

func TestRow_MarshalUnmarshalMethods(t *testing.T) {
	// Test Row.Marshal() and Row.Unmarshal() methods
	row := Row{
		wkk.NewRowKey("name"):    "test",
		wkk.NewRowKey("service"): "api",
	}

	// Marshal using method
	data, err := row.Marshal()
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"test","service":"api"}`, string(data))

	// Unmarshal using method
	var row2 Row
	err = row2.Unmarshal(data)
	require.NoError(t, err)
	assert.Equal(t, "test", row2[wkk.NewRowKey("name")])
	assert.Equal(t, "api", row2[wkk.NewRowKey("service")])
}
