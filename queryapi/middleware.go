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
	"log/slog"
	"net/http"

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

// apiKeyMiddleware validates the API key from various sources and adds orgId to context.
// Checks in order:
// 1. x-cardinalhq-api-key header
// 2. Api-Key header (for legacy Scala compatibility)
// 3. api_key cookie (for legacy Scala compatibility)
func (q *QuerierService) apiKeyMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := extractAPIKey(r)
		if apiKey == "" {
			http.Error(w, "missing API key (provide via x-cardinalhq-api-key or Api-Key header, or api_key cookie)", http.StatusUnauthorized)
			return
		}

		// Validate API key and get organization ID
		orgID, err := q.apiKeyProvider.ValidateAPIKey(r.Context(), apiKey)
		if err != nil {
			slog.Error("API key validation failed", "error", err)
			http.Error(w, "invalid API key", http.StatusUnauthorized)
			return
		}

		if orgID == nil {
			http.Error(w, "invalid API key", http.StatusUnauthorized)
			return
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
