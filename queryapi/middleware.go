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
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Context key for storing organization ID
type contextKey struct{}

var orgIDKey = contextKey{}

// WithOrgID returns a new context with the organization ID stored in it
func WithOrgID(ctx context.Context, orgID uuid.UUID) context.Context {
	return context.WithValue(ctx, orgIDKey, orgID)
}

// GetOrgIDFromContext retrieves the organization ID from the context
func GetOrgIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	orgID, ok := ctx.Value(orgIDKey).(uuid.UUID)
	return orgID, ok
}

// apiKeyMiddleware validates the API key or JWT token from various sources and adds orgId to context.
// Checks in order:
// 1. x-cardinalhq-api-key header
// 2. Api-Key header (for legacy Scala compatibility)
// 3. api_key cookie (for legacy Scala compatibility)
// 4. cardinal_token cookie with JWT (for legacy Scala compatibility)
func (q *QuerierService) apiKeyMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var orgID *uuid.UUID
		var err error

		// Try API key authentication first
		apiKey := extractAPIKey(r)
		if apiKey != "" {
			orgID, err = q.apiKeyProvider.ValidateAPIKey(r.Context(), apiKey)
			if err != nil {
				slog.Error("API key validation failed", "error", err)
				http.Error(w, "invalid API key", http.StatusUnauthorized)
				return
			}

			if orgID == nil {
				http.Error(w, "invalid API key", http.StatusUnauthorized)
				return
			}
		} else {
			// Try JWT token authentication
			orgID, err = q.extractOrgIDFromJWT(r)
			if err != nil {
				slog.Error("JWT validation failed", "error", err)
				http.Error(w, "authentication required (provide API key or valid JWT token)", http.StatusUnauthorized)
				return
			}

			if orgID == nil {
				http.Error(w, "authentication required (provide API key or valid JWT token)", http.StatusUnauthorized)
				return
			}
		}

		// Add organization ID to context
		ctx := WithOrgID(r.Context(), *orgID)
		r = r.WithContext(ctx)

		// Call the next handler
		next(w, r)
	}
}

// extractAPIKey extracts the API key from various sources in the request.
// Checks in order: x-cardinalhq-api-key header, Api-Key header, api_key cookie.
func extractAPIKey(r *http.Request) string {
	// Check x-cardinalhq-api-key header (primary method)
	if apiKey := r.Header.Get("x-cardinalhq-api-key"); apiKey != "" {
		return apiKey
	}

	// Check Api-Key header (legacy Scala compatibility)
	if apiKey := r.Header.Get("Api-Key"); apiKey != "" {
		return apiKey
	}

	// Check api_key cookie (legacy Scala compatibility)
	if cookie, err := r.Cookie("api_key"); err == nil && cookie.Value != "" {
		return cookie.Value
	}

	return ""
}

// extractOrgIDFromJWT validates a JWT token from the cardinal_token cookie and extracts the org_id.
// Returns the organization ID if the token is valid, or an error if validation fails.
func (q *QuerierService) extractOrgIDFromJWT(r *http.Request) (*uuid.UUID, error) {
	// Get cardinal_token cookie
	cookie, err := r.Cookie("cardinal_token")
	if err != nil {
		return nil, fmt.Errorf("no cardinal_token cookie found")
	}

	tokenString := cookie.Value
	if tokenString == "" {
		return nil, fmt.Errorf("empty cardinal_token cookie")
	}

	// Use the cached secret key (loaded at startup)
	if q.jwtSecretKey == "" {
		// If the secret key is not configured, JWT authentication is not available
		// This is not an error - just means JWT auth isn't configured
		return nil, fmt.Errorf("JWT authentication not configured")
	}

	// Parse and validate the JWT token
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Verify the signing method is HMAC
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(q.jwtSecretKey), nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse JWT: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid JWT token")
	}

	// Extract claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("failed to extract JWT claims")
	}

	// Verify issuer
	issuer, ok := claims["iss"].(string)
	if !ok || issuer != "cardinalhq.io" {
		return nil, fmt.Errorf("invalid JWT issuer")
	}

	// Extract org_id claim
	orgIDStr, ok := claims["org_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing org_id claim in JWT")
	}

	// Parse org_id as UUID
	orgID, err := uuid.Parse(orgIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid org_id format: %w", err)
	}

	return &orgID, nil
}
