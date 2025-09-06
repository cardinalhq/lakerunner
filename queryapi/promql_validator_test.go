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
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlePromQLValidate(t *testing.T) {
	qs := &QuerierService{}

	tests := []struct {
		name           string
		query          string
		expectedValid  bool
		expectedStatus int
	}{
		{
			name:           "Valid PromQL query - simple metric",
			query:          `up`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid PromQL query - metric with labels",
			query:          `up{job="prometheus"}`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid PromQL query - function",
			query:          `rate(http_requests_total[5m])`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid PromQL query - binary operation",
			query:          `up + up`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid PromQL query - aggregation",
			query:          `sum(rate(http_requests_total[5m])) by (job)`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid PromQL query - subquery",
			query:          `rate(http_requests_total[5m:1m])`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid PromQL query - complex expression",
			query:          `sum(rate(http_requests_total{job="api"}[5m])) / sum(rate(http_requests_total[5m]))`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Invalid PromQL - missing closing parenthesis",
			query:          `rate(http_requests_total[5m]`,
			expectedValid:  false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Invalid PromQL - syntax error",
			query:          `up{job=}`,
			expectedValid:  false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Invalid PromQL - invalid function",
			query:          `invalid_function(up)`,
			expectedValid:  false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Invalid PromQL - malformed range",
			query:          `rate(up[5x])`,
			expectedValid:  false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Empty query",
			query:          "",
			expectedValid:  false,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request body
			reqBody := map[string]string{"query": tt.query}
			jsonBody, _ := json.Marshal(reqBody)

			// Create HTTP request
			req := httptest.NewRequest("POST", "/api/v1/promql/validate", bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			w := httptest.NewRecorder()

			// Call the handler
			qs.handlePromQLValidate(w, req)

			// Check status code
			if w.Code != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			// Parse response
			var response map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("Failed to parse response JSON: %v", err)
			}

			// Check valid field
			valid, ok := response["valid"].(bool)
			if !ok {
				t.Fatalf("Response missing 'valid' field or not a boolean")
			}

			if valid != tt.expectedValid {
				t.Errorf("Expected valid=%v, got valid=%v", tt.expectedValid, valid)
			}

			// For invalid queries, check that error field is present
			if !tt.expectedValid && tt.expectedStatus == http.StatusOK {
				if _, ok := response["error"]; !ok {
					t.Error("Expected 'error' field in response for invalid query")
				}
			}
		})
	}
}

func TestHandlePromQLValidateInvalidJSON(t *testing.T) {
	qs := &QuerierService{}

	// Create HTTP request with invalid JSON
	req := httptest.NewRequest("POST", "/api/v1/promql/validate", bytes.NewReader([]byte(`{invalid json}`)))
	req.Header.Set("Content-Type", "application/json")

	// Create response recorder
	w := httptest.NewRecorder()

	// Call the handler
	qs.handlePromQLValidate(w, req)

	// Check status code
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
	}

	// Parse response
	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response JSON: %v", err)
	}

	// Check valid field
	valid, ok := response["valid"].(bool)
	if !ok {
		t.Fatalf("Response missing 'valid' field or not a boolean")
	}

	if valid != false {
		t.Errorf("Expected valid=false, got valid=%v", valid)
	}
}

func TestLineColFromOffset(t *testing.T) {
	tests := []struct {
		input        string
		pos          int
		expectedLine int
		expectedCol  int
	}{
		{
			input:        "hello world",
			pos:          5,
			expectedLine: 1,
			expectedCol:  6,
		},
		{
			input:        "hello\nworld",
			pos:          8,
			expectedLine: 2,
			expectedCol:  3,
		},
		{
			input:        "line1\nline2\nline3",
			pos:          12,
			expectedLine: 3,
			expectedCol:  1,
		},
		{
			input:        "single line",
			pos:          0,
			expectedLine: 1,
			expectedCol:  1,
		},
		{
			input:        "test",
			pos:          -1,
			expectedLine: 0,
			expectedCol:  0,
		},
		{
			input:        "test",
			pos:          10,
			expectedLine: 1,
			expectedCol:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			line, col := lineColFromOffset(tt.input, tt.pos)
			if line != tt.expectedLine {
				t.Errorf("Expected line %d, got %d", tt.expectedLine, line)
			}
			if col != tt.expectedCol {
				t.Errorf("Expected col %d, got %d", tt.expectedCol, col)
			}
		})
	}
}

func TestSliceSafe(t *testing.T) {
	tests := []struct {
		input    string
		start    int
		end      int
		expected string
	}{
		{
			input:    "hello world",
			start:    2,
			end:      7,
			expected: "llo w",
		},
		{
			input:    "hello world",
			start:    -1,
			end:      5,
			expected: "hello",
		},
		{
			input:    "hello world",
			start:    5,
			end:      20,
			expected: " world",
		},
		{
			input:    "hello world",
			start:    10,
			end:      5,
			expected: "",
		},
		{
			input:    "hello world",
			start:    0,
			end:      0,
			expected: "",
		},
		{
			input:    "hello world",
			start:    0,
			end:      11,
			expected: "hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sliceSafe(tt.input, tt.start, tt.end)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}
