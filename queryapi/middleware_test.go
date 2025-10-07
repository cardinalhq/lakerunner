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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractAPIKey(t *testing.T) {
	tests := []struct {
		name           string
		setupRequest   func(*http.Request)
		expectedAPIKey string
	}{
		{
			name: "x-cardinalhq-api-key header",
			setupRequest: func(r *http.Request) {
				r.Header.Set("x-cardinalhq-api-key", "test-key-1")
			},
			expectedAPIKey: "test-key-1",
		},
		{
			name: "Api-Key header",
			setupRequest: func(r *http.Request) {
				r.Header.Set("Api-Key", "test-key-2")
			},
			expectedAPIKey: "test-key-2",
		},
		{
			name: "api_key cookie",
			setupRequest: func(r *http.Request) {
				r.AddCookie(&http.Cookie{
					Name:  "api_key",
					Value: "test-key-3",
				})
			},
			expectedAPIKey: "test-key-3",
		},
		{
			name: "x-cardinalhq-api-key takes precedence over Api-Key",
			setupRequest: func(r *http.Request) {
				r.Header.Set("x-cardinalhq-api-key", "primary-key")
				r.Header.Set("Api-Key", "secondary-key")
			},
			expectedAPIKey: "primary-key",
		},
		{
			name: "x-cardinalhq-api-key takes precedence over cookie",
			setupRequest: func(r *http.Request) {
				r.Header.Set("x-cardinalhq-api-key", "header-key")
				r.AddCookie(&http.Cookie{
					Name:  "api_key",
					Value: "cookie-key",
				})
			},
			expectedAPIKey: "header-key",
		},
		{
			name: "Api-Key takes precedence over cookie",
			setupRequest: func(r *http.Request) {
				r.Header.Set("Api-Key", "header-key")
				r.AddCookie(&http.Cookie{
					Name:  "api_key",
					Value: "cookie-key",
				})
			},
			expectedAPIKey: "header-key",
		},
		{
			name: "no API key provided",
			setupRequest: func(r *http.Request) {
				// Don't set any API key
			},
			expectedAPIKey: "",
		},
		{
			name: "empty api_key cookie ignored",
			setupRequest: func(r *http.Request) {
				r.AddCookie(&http.Cookie{
					Name:  "api_key",
					Value: "",
				})
			},
			expectedAPIKey: "",
		},
		{
			name: "all three methods provided",
			setupRequest: func(r *http.Request) {
				r.Header.Set("x-cardinalhq-api-key", "primary")
				r.Header.Set("Api-Key", "secondary")
				r.AddCookie(&http.Cookie{
					Name:  "api_key",
					Value: "tertiary",
				})
			},
			expectedAPIKey: "primary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/test", nil)
			assert.NoError(t, err)

			tt.setupRequest(req)

			apiKey := extractAPIKey(req)
			assert.Equal(t, tt.expectedAPIKey, apiKey)
		})
	}
}
