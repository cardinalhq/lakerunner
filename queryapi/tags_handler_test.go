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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPromTagsReqParsing tests that the promTagsReq struct correctly parses
// the new Q field for scoped tag queries.
func TestPromTagsReqParsing(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantS   string
		wantE   string
		wantQ   string
		wantMet string
		wantErr bool
	}{
		{
			name:    "metric only (existing behavior)",
			json:    `{"s":"1704067200000","e":"1704153600000","metric":"cpu_usage"}`,
			wantS:   "1704067200000",
			wantE:   "1704153600000",
			wantMet: "cpu_usage",
			wantQ:   "",
		},
		{
			name:  "query expression only",
			json:  `{"s":"1704067200000","e":"1704153600000","q":"cpu_usage{host=\"web01\"}"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "cpu_usage{host=\"web01\"}",
		},
		{
			name:    "both metric and query (q takes precedence in handler)",
			json:    `{"s":"1704067200000","e":"1704153600000","metric":"cpu_usage","q":"cpu_usage{env=\"prod\"}"}`,
			wantS:   "1704067200000",
			wantE:   "1704153600000",
			wantMet: "cpu_usage",
			wantQ:   "cpu_usage{env=\"prod\"}",
		},
		{
			name:  "complex PromQL expression",
			json:  `{"s":"1704067200000","e":"1704153600000","q":"http_requests_total{method=\"GET\", status=~\"2..\"}"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "http_requests_total{method=\"GET\", status=~\"2..\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req promTagsReq
			err := json.Unmarshal([]byte(tt.json), &req)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantS, req.S)
			assert.Equal(t, tt.wantE, req.E)
			assert.Equal(t, tt.wantQ, req.Q)
			assert.Equal(t, tt.wantMet, req.Metric)
		})
	}
}

// TestSpanTagsPayloadParsing tests that the spanTagsPayload struct correctly parses
// the new Q field for scoped tag queries.
func TestSpanTagsPayloadParsing(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantS   string
		wantE   string
		wantQ   string
		wantErr bool
	}{
		{
			name:  "time range only (existing behavior)",
			json:  `{"s":"1704067200000","e":"1704153600000"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "",
		},
		{
			name:  "with query expression",
			json:  `{"s":"1704067200000","e":"1704153600000","q":"{service_name=\"api-gateway\"}"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "{service_name=\"api-gateway\"}",
		},
		{
			name:  "complex LogQL-style expression",
			json:  `{"s":"1704067200000","e":"1704153600000","q":"{service_name=\"api-gateway\", http_method=\"POST\"}"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "{service_name=\"api-gateway\", http_method=\"POST\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req spanTagsPayload
			err := json.Unmarshal([]byte(tt.json), &req)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantS, req.S)
			assert.Equal(t, tt.wantE, req.E)
			assert.Equal(t, tt.wantQ, req.Q)
		})
	}
}

// TestLogQLTagsPayloadParsing tests that the logqlTagsPayload struct correctly parses
// the Q field for scoped tag queries (this was already supported).
func TestLogQLTagsPayloadParsing(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantS   string
		wantE   string
		wantQ   string
		wantErr bool
	}{
		{
			name:  "time range only",
			json:  `{"s":"1704067200000","e":"1704153600000"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "",
		},
		{
			name:  "with query expression",
			json:  `{"s":"1704067200000","e":"1704153600000","q":"{service_name=\"api\"}"}`,
			wantS: "1704067200000",
			wantE: "1704153600000",
			wantQ: "{service_name=\"api\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req logqlTagsPayload
			err := json.Unmarshal([]byte(tt.json), &req)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantS, req.S)
			assert.Equal(t, tt.wantE, req.E)
			assert.Equal(t, tt.wantQ, req.Q)
		})
	}
}

// TestReadSpanTagsPayload tests the readSpanTagsPayload function
func TestReadSpanTagsPayload(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		contentType    string
		body           string
		expectNil      bool
		expectedStatus int
	}{
		{
			name:           "valid request",
			method:         http.MethodPost,
			contentType:    "application/json",
			body:           `{"s":"1704067200000","e":"1704153600000"}`,
			expectNil:      false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "valid request with q",
			method:         http.MethodPost,
			contentType:    "application/json",
			body:           `{"s":"1704067200000","e":"1704153600000","q":"{service=\"api\"}"}`,
			expectNil:      false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "wrong method",
			method:         http.MethodGet,
			contentType:    "application/json",
			body:           `{"s":"1704067200000","e":"1704153600000"}`,
			expectNil:      true,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "wrong content type",
			method:         http.MethodPost,
			contentType:    "text/plain",
			body:           `{"s":"1704067200000","e":"1704153600000"}`,
			expectNil:      true,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "invalid JSON",
			method:         http.MethodPost,
			contentType:    "application/json",
			body:           `{invalid json}`,
			expectNil:      true,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/v1/spans/tags", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			w := httptest.NewRecorder()

			result := readSpanTagsPayload(w, req)

			if tt.expectNil {
				assert.Nil(t, result)
				assert.Equal(t, tt.expectedStatus, w.Code)
			} else {
				// For valid requests without org context, it will still return nil
				// because GetOrgIDFromContext will fail. This is expected in unit tests.
				// The important thing is that parsing succeeded up to that point.
				if result == nil {
					// Check that we got an internal server error (missing org context)
					// rather than a bad request error
					assert.Equal(t, http.StatusInternalServerError, w.Code)
				}
			}
		})
	}
}

// TestReadLogQLTagsPayload tests the readLogQLTagsPayload function
func TestReadLogQLTagsPayload(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		contentType    string
		body           string
		expectNil      bool
		expectedStatus int
	}{
		{
			name:           "valid request",
			method:         http.MethodPost,
			contentType:    "application/json",
			body:           `{"s":"1704067200000","e":"1704153600000"}`,
			expectNil:      false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "valid request with q",
			method:         http.MethodPost,
			contentType:    "application/json",
			body:           `{"s":"1704067200000","e":"1704153600000","q":"{service=\"api\"}"}`,
			expectNil:      false,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "wrong method",
			method:         http.MethodGet,
			contentType:    "application/json",
			body:           `{"s":"1704067200000","e":"1704153600000"}`,
			expectNil:      true,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/v1/logs/tags", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			w := httptest.NewRecorder()

			result := readLogQLTagsPayload(w, req)

			if tt.expectNil {
				assert.Nil(t, result)
				assert.Equal(t, tt.expectedStatus, w.Code)
			} else {
				// For valid requests without org context, it will still return nil
				if result == nil {
					assert.Equal(t, http.StatusInternalServerError, w.Code)
				}
			}
		})
	}
}
