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

func TestHandleLogQLValidate(t *testing.T) {
	qs := &QuerierService{}

	tests := []struct {
		name           string
		query          string
		expectedValid  bool
		expectedStatus int
	}{
		{
			name:           "Valid LogQL query",
			query:          `{app="api"}`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid LogQL with pipeline",
			query:          `{app="api"} | json | level="error"`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Valid LogQL with range aggregation",
			query:          `rate({app="api"}[5m])`,
			expectedValid:  true,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Invalid LogQL - missing closing brace",
			query:          `{app="api"`,
			expectedValid:  false,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Invalid LogQL - syntax error",
			query:          `{invalid`,
			expectedValid:  false,
			expectedStatus: http.StatusBadRequest,
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
			req := httptest.NewRequest("POST", "/api/v1/logql/validate", bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			w := httptest.NewRecorder()

			// Call the handler
			qs.handleLogQLValidate(w, req)

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

//func TestParseErrorPosition(t *testing.T) {
//	tests := []struct {
//		errMsg       string
//		expectedLine int
//		expectedCol  int
//	}{
//		{
//			errMsg:       "parse error at line 1, col 9: syntax error: unexpected $end, expecting = or =~ or !~ or !=",
//			expectedLine: 1,
//			expectedCol:  9,
//		},
//		{
//			errMsg:       "parse error at line 2, col 15: unexpected token",
//			expectedLine: 2,
//			expectedCol:  15,
//		},
//		{
//			errMsg:       "some other error",
//			expectedLine: 1,
//			expectedCol:  1,
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.errMsg, func(t *testing.T) {
//			line, col := parseErrorPosition(tt.errMsg)
//			if line != tt.expectedLine {
//				t.Errorf("Expected line %d, got %d", tt.expectedLine, line)
//			}
//			if col != tt.expectedCol {
//				t.Errorf("Expected col %d, got %d", tt.expectedCol, col)
//			}
//		})
//	}
//}

func TestHandleLogQLValidateInvalidJSON(t *testing.T) {
	qs := &QuerierService{}

	// Create HTTP request with invalid JSON
	req := httptest.NewRequest("POST", "/api/v1/logql/validate", bytes.NewReader([]byte(`{invalid json}`)))
	req.Header.Set("Content-Type", "application/json")

	// Create response recorder
	w := httptest.NewRecorder()

	// Call the handler
	qs.handleLogQLValidate(w, req)

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
