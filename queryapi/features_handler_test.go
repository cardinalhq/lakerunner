// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleFeatures(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		expectedStatus int
	}{
		{
			name:           "supports GET",
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "supports POST",
			method:         http.MethodPost,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "rejects unsupported method",
			method:         http.MethodPut,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QuerierService{}

			req := httptest.NewRequest(tt.method, "/api/v1/features", nil)
			rec := httptest.NewRecorder()
			q.handleFeatures(rec, req)

			resp := rec.Result()
			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus != http.StatusOK {
				return
			}

			assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			var body queryAPIFeaturesResp
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			assert.True(t, body.Features.MetricsSummarySSE)
		})
	}
}
