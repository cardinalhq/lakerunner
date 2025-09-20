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

	type okResp struct {
		Valid bool `json:"valid"`
	}
	type errResp struct {
		Status  int    `json:"status"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}

	tests := []struct {
		name           string
		query          string
		expectedStatus int
		expectValid    bool
		expectErrCode  string
	}{
		{
			name:           "Valid LogQL query",
			query:          `{app="api"}`,
			expectedStatus: http.StatusOK,
			expectValid:    true,
		},
		{
			name:           "Valid LogQL with pipeline",
			query:          `{app="api"} | json | level="error"`,
			expectedStatus: http.StatusOK,
			expectValid:    true,
		},
		{
			name:           "Valid LogQL with range aggregation",
			query:          `rate({app="api"}[5m])`,
			expectedStatus: http.StatusOK,
			expectValid:    true,
		},
		{
			name:           "Invalid LogQL - missing closing brace",
			query:          `{app="api"`,
			expectedStatus: http.StatusBadRequest,
			expectErrCode:  "INVALID_EXPR",
		},
		{
			name:           "Invalid LogQL - syntax error",
			query:          `{invalid`,
			expectedStatus: http.StatusBadRequest,
			expectErrCode:  "INVALID_EXPR",
		},
		{
			name:           "Empty query",
			query:          "",
			expectedStatus: http.StatusBadRequest,
			expectErrCode:  "INVALID_JSON", // handler returns INVALID_JSON for empty/missing query field
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody := map[string]string{"query": tt.query}
			jsonBody, _ := json.Marshal(reqBody)

			req := httptest.NewRequest("POST", "/api/v1/logql/validate", bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			qs.handleLogQLValidate(w, req)

			if w.Code != tt.expectedStatus {
				t.Fatalf("expected status %d, got %d; body=%s", tt.expectedStatus, w.Code, w.Body.String())
			}

			if tt.expectedStatus == http.StatusOK {
				var ok okResp
				if err := json.Unmarshal(w.Body.Bytes(), &ok); err != nil {
					t.Fatalf("parse 200 JSON failed: %v; body=%s", err, w.Body.String())
				}
				if ok.Valid != tt.expectValid {
					t.Fatalf("valid=%v, want %v", ok.Valid, tt.expectValid)
				}
				return
			}

			var er errResp
			if err := json.Unmarshal(w.Body.Bytes(), &er); err != nil {
				t.Fatalf("parse error JSON failed: %v; body=%s", err, w.Body.String())
			}
			if er.Code != tt.expectErrCode {
				t.Fatalf("error code=%q, want %q; body=%s", er.Code, tt.expectErrCode, w.Body.String())
			}
			if er.Message == "" {
				t.Fatalf("missing error message; body=%s", w.Body.String())
			}
		})
	}
}

func TestHandleLogQLValidateInvalidJSON(t *testing.T) {
	qs := &QuerierService{}

	req := httptest.NewRequest("POST", "/api/v1/logql/validate", bytes.NewReader([]byte(`{invalid json}`)))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	qs.handleLogQLValidate(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d; body=%s", http.StatusBadRequest, w.Code, w.Body.String())
	}

	var er struct {
		Status  int    `json:"status"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &er); err != nil {
		t.Fatalf("parse error JSON failed: %v; body=%s", err, w.Body.String())
	}
	if er.Code != "INVALID_JSON" {
		t.Fatalf("error code=%q, want INVALID_JSON; body=%s", er.Code, w.Body.String())
	}
	if er.Message == "" {
		t.Fatalf("missing error message; body=%s", w.Body.String())
	}
}
