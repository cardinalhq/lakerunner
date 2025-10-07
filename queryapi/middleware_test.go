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
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestExtractOrgIDFromJWT(t *testing.T) {
	// Set up test secret key
	testSecret := "test-secret-key-for-jwt-validation"
	oldSecret := os.Getenv("TOKEN_HMAC256_KEY")
	require.NoError(t, os.Setenv("TOKEN_HMAC256_KEY", testSecret))
	defer func() {
		if oldSecret != "" {
			_ = os.Setenv("TOKEN_HMAC256_KEY", oldSecret)
		} else {
			_ = os.Unsetenv("TOKEN_HMAC256_KEY")
		}
	}()

	testOrgID := uuid.New()

	// Helper to create a valid JWT token
	createToken := func(orgID uuid.UUID, issuer string, expired bool) string {
		claims := jwt.MapClaims{
			"iss":    issuer,
			"org_id": orgID.String(),
			"exp":    time.Now().Add(1 * time.Hour).Unix(),
		}
		if expired {
			claims["exp"] = time.Now().Add(-1 * time.Hour).Unix()
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tokenString, _ := token.SignedString([]byte(testSecret))
		return tokenString
	}

	tests := []struct {
		name        string
		setupReq    func(*http.Request)
		expectError bool
		expectOrgID *uuid.UUID
	}{
		{
			name: "valid JWT token",
			setupReq: func(r *http.Request) {
				token := createToken(testOrgID, "cardinalhq.io", false)
				r.AddCookie(&http.Cookie{
					Name:  "cardinal_token",
					Value: token,
				})
			},
			expectError: false,
			expectOrgID: &testOrgID,
		},
		{
			name: "no cardinal_token cookie",
			setupReq: func(r *http.Request) {
				// Don't add cookie
			},
			expectError: true,
			expectOrgID: nil,
		},
		{
			name: "empty cardinal_token cookie",
			setupReq: func(r *http.Request) {
				r.AddCookie(&http.Cookie{
					Name:  "cardinal_token",
					Value: "",
				})
			},
			expectError: true,
			expectOrgID: nil,
		},
		{
			name: "invalid JWT format",
			setupReq: func(r *http.Request) {
				r.AddCookie(&http.Cookie{
					Name:  "cardinal_token",
					Value: "not-a-valid-jwt-token",
				})
			},
			expectError: true,
			expectOrgID: nil,
		},
		{
			name: "wrong issuer",
			setupReq: func(r *http.Request) {
				token := createToken(testOrgID, "wrong-issuer.com", false)
				r.AddCookie(&http.Cookie{
					Name:  "cardinal_token",
					Value: token,
				})
			},
			expectError: true,
			expectOrgID: nil,
		},
		{
			name: "expired token",
			setupReq: func(r *http.Request) {
				token := createToken(testOrgID, "cardinalhq.io", true)
				r.AddCookie(&http.Cookie{
					Name:  "cardinal_token",
					Value: token,
				})
			},
			expectError: true,
			expectOrgID: nil,
		},
		{
			name: "token with wrong secret",
			setupReq: func(r *http.Request) {
				// Create token with different secret
				claims := jwt.MapClaims{
					"iss":    "cardinalhq.io",
					"org_id": testOrgID.String(),
					"exp":    time.Now().Add(1 * time.Hour).Unix(),
				}
				token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
				tokenString, _ := token.SignedString([]byte("wrong-secret"))
				r.AddCookie(&http.Cookie{
					Name:  "cardinal_token",
					Value: tokenString,
				})
			},
			expectError: true,
			expectOrgID: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", "/test", nil)
			require.NoError(t, err)

			tt.setupReq(req)

			orgID, err := extractOrgIDFromJWT(req)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, orgID)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, orgID)
				assert.Equal(t, *tt.expectOrgID, *orgID)
			}
		})
	}
}

func TestExtractOrgIDFromJWT_NoEnvVar(t *testing.T) {
	// Ensure TOKEN_HMAC256_KEY is not set
	oldSecret := os.Getenv("TOKEN_HMAC256_KEY")
	require.NoError(t, os.Unsetenv("TOKEN_HMAC256_KEY"))
	defer func() {
		if oldSecret != "" {
			_ = os.Setenv("TOKEN_HMAC256_KEY", oldSecret)
		}
	}()

	req, err := http.NewRequest("GET", "/test", nil)
	require.NoError(t, err)

	req.AddCookie(&http.Cookie{
		Name:  "cardinal_token",
		Value: "dummy-token",
	})

	orgID, err := extractOrgIDFromJWT(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "JWT authentication not configured")
	assert.Nil(t, orgID)
}
