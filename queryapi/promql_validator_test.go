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
	"reflect"
	"sort"
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

func TestHandlePromQLValidateWithExemplar(t *testing.T) {
	qs := &QuerierService{}

	// Sample exemplar data for testing
	validExemplar := map[string]any{
		"http_requests_total": 100,
		"job":                 "api-server",
		"instance":            "localhost:8080",
		"method":              "GET",
		"status":              "200",
		"handler":             "/api/users",
		"up":                  1,
		"cpu_usage":           45.2,
	}

	tests := []struct {
		name                    string
		query                   string
		exemplar                map[string]any
		expectedValid           bool
		expectedStatus          int
		expectedMissingFields   []string
		expectedValidationCount int
		expectedContexts        []string
	}{
		{
			name:           "Valid query with all fields in exemplar",
			query:          `sum(rate(http_requests_total[5m])) by (job, instance)`,
			exemplar:       validExemplar,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid query - metric exists",
			query:          `up`,
			exemplar:       validExemplar,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid query - labels exist",
			query:          `http_requests_total{job="api-server", method="GET"}`,
			exemplar:       validExemplar,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:                    "Invalid query - metric not in exemplar",
			query:                   `nonexistent_metric`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"nonexistent_metric"},
			expectedValidationCount: 1,
			expectedContexts:        []string{"metric_name"},
		},
		{
			name:                    "Invalid query - label matcher field missing",
			query:                   `http_requests_total{nonexistent_label="value"}`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"nonexistent_label"},
			expectedValidationCount: 1,
			expectedContexts:        []string{"label_matcher"},
		},
		{
			name:                    "Invalid query - group by field missing",
			query:                   `sum(http_requests_total) by (nonexistent_field)`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"nonexistent_field"},
			expectedValidationCount: 1,
			expectedContexts:        []string{"group_by"},
		},
		{
			name:                    "Invalid query - multiple missing fields",
			query:                   `sum(nonexistent_metric{missing_label="value"}) by (missing_group_field)`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"nonexistent_metric", "missing_label", "missing_group_field"},
			expectedValidationCount: 3,
			expectedContexts:        []string{"metric_name", "label_matcher", "group_by"},
		},
		{
			name:                    "Invalid query - group without field missing",
			query:                   `sum(http_requests_total) without (nonexistent_field)`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"nonexistent_field"},
			expectedValidationCount: 1,
			expectedContexts:        []string{"group_without"},
		},
		{
			name:           "Valid query without exemplar - should pass syntax validation",
			query:          `sum(rate(any_metric[5m])) by (any_label)`,
			exemplar:       nil,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid query with empty exemplar - should pass syntax validation",
			query:          `sum(rate(any_metric[5m])) by (any_label)`,
			exemplar:       map[string]any{},
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:                    "Complex query with mixed valid/invalid fields",
			query:                   `rate(http_requests_total{job="api", missing_label="x"}[5m]) / rate(valid_metric{instance="localhost"}[5m])`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"missing_label", "valid_metric"},
			expectedValidationCount: 2,
		},
		{
			name:           "Binary operation with valid fields",
			query:          `up + cpu_usage`,
			exemplar:       validExemplar,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		// Additional edge cases
		{
			name:           "Scalar literal query",
			query:          `123`,
			exemplar:       validExemplar,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:                    "Function with missing metric",
			query:                   `rate(missing_metric[5m])`,
			exemplar:                validExemplar,
			expectedValid:           false,
			expectedStatus:          http.StatusOK,
			expectedMissingFields:   []string{"missing_metric"},
			expectedValidationCount: 1,
			expectedContexts:        []string{"metric_name"},
		},
		{
			name:           "Complex aggregation with all fields present",
			query:          `topk(5, sum(rate(http_requests_total[5m])) by (job))`,
			exemplar:       validExemplar,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request body
			reqBody := promQLValidateRequest{
				Query:    tt.query,
				Exemplar: tt.exemplar,
			}
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

			// Parse response - handle both success and error response types
			responseBody := w.Body.Bytes()

			if tt.expectedValid {
				// For valid responses, expect promQLValidateResponse
				var response promQLValidateResponse
				if err := json.Unmarshal(responseBody, &response); err != nil {
					t.Fatalf("Failed to parse success response JSON: %v", err)
				}

				if response.Valid != tt.expectedValid {
					t.Errorf("Expected valid=%v, got valid=%v", tt.expectedValid, response.Valid)
				}
			} else {
				// For invalid responses, expect promQLValidateErrorResponse
				var response promQLValidateErrorResponse
				if err := json.Unmarshal(responseBody, &response); err != nil {
					t.Fatalf("Failed to parse error response JSON: %v", err)
				}

				if response.Valid != tt.expectedValid {
					t.Errorf("Expected valid=%v, got valid=%v", tt.expectedValid, response.Valid)
				}

				// Check missing fields
				if len(tt.expectedMissingFields) > 0 {
					sort.Strings(response.MissingFields)
					expectedSorted := make([]string, len(tt.expectedMissingFields))
					copy(expectedSorted, tt.expectedMissingFields)
					sort.Strings(expectedSorted)

					if !reflect.DeepEqual(response.MissingFields, expectedSorted) {
						t.Errorf("Expected missing fields %v, got %v", expectedSorted, response.MissingFields)
					}
				}

				// Check validation error count
				if tt.expectedValidationCount > 0 {
					if len(response.ValidationErrors) != tt.expectedValidationCount {
						t.Errorf("Expected %d validation errors, got %d", tt.expectedValidationCount, len(response.ValidationErrors))
					}
				}

				// Check contexts if specified
				if len(tt.expectedContexts) > 0 {
					contexts := make([]string, len(response.ValidationErrors))
					for i, err := range response.ValidationErrors {
						contexts[i] = err.Context
					}
					sort.Strings(contexts)
					expectedContextsSorted := make([]string, len(tt.expectedContexts))
					copy(expectedContextsSorted, tt.expectedContexts)
					sort.Strings(expectedContextsSorted)

					if !reflect.DeepEqual(contexts, expectedContextsSorted) {
						t.Errorf("Expected contexts %v, got %v", expectedContextsSorted, contexts)
					}
				}

				// Check that error message is present
				if response.Error == "" {
					t.Error("Expected error message for invalid query")
				}
			}
		})
	}
}

func TestExtractFieldsFromExemplar(t *testing.T) {
	tests := []struct {
		name           string
		exemplar       map[string]any
		expectedFields []string
	}{
		{
			name: "Basic exemplar",
			exemplar: map[string]any{
				"metric1": 100,
				"label1":  "value1",
				"label2":  "value2",
			},
			expectedFields: []string{"metric1", "label1", "label2"},
		},
		{
			name:           "Empty exemplar",
			exemplar:       map[string]any{},
			expectedFields: []string{},
		},
		{
			name:           "Nil exemplar",
			exemplar:       nil,
			expectedFields: []string{},
		},
		{
			name: "Mixed data types in exemplar",
			exemplar: map[string]any{
				"string_field": "value",
				"int_field":    42,
				"float_field":  3.14,
				"bool_field":   true,
				"array_field":  []string{"a", "b"},
				"object_field": map[string]string{"nested": "value"},
			},
			expectedFields: []string{"string_field", "int_field", "float_field", "bool_field", "array_field", "object_field"},
		},
		{
			name: "Special characters in field names",
			exemplar: map[string]any{
				"field_with_underscore": "value",
				"field-with-dash":       "value",
				"field.with.dots":       "value",
			},
			expectedFields: []string{"field_with_underscore", "field-with-dash", "field.with.dots"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := extractFieldsFromExemplar(tt.exemplar)

			// Convert map to slice for comparison
			var actualFields []string
			for field := range fields {
				actualFields = append(actualFields, field)
			}

			// Handle empty case specifically
			if len(tt.expectedFields) == 0 && len(actualFields) == 0 {
				return // Both empty, test passes
			}

			sort.Strings(actualFields)
			expectedSorted := make([]string, len(tt.expectedFields))
			copy(expectedSorted, tt.expectedFields)
			sort.Strings(expectedSorted)

			if !reflect.DeepEqual(actualFields, expectedSorted) {
				t.Errorf("Expected fields %v, got %v", expectedSorted, actualFields)
			}
		})
	}
}

func TestRemoveDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "No duplicates",
			input:    []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "With duplicates",
			input:    []string{"a", "b", "a", "c", "b"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "All duplicates",
			input:    []string{"a", "a", "a"},
			expected: []string{"a"},
		},
		{
			name:     "Empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "Single element",
			input:    []string{"a"},
			expected: []string{"a"},
		},
		{
			name:     "Nil slice",
			input:    nil,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeDuplicates(tt.input)

			// Handle nil/empty cases
			if len(tt.expected) == 0 {
				if len(result) != 0 {
					t.Errorf("Expected empty result, got %v", result)
				}
				return
			}

			// Sort both slices for comparison since order isn't guaranteed
			sort.Strings(result)
			expectedSorted := make([]string, len(tt.expected))
			copy(expectedSorted, tt.expected)
			sort.Strings(expectedSorted)

			if !reflect.DeepEqual(result, expectedSorted) {
				t.Errorf("Expected %v, got %v", expectedSorted, result)
			}
		})
	}
}
