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

// apiKeyMiddleware validates the x-cardinalhq-api-key header and adds orgId to context
func (q *QuerierService) apiKeyMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("x-cardinalhq-api-key")
		if apiKey == "" {
			http.Error(w, "missing x-cardinalhq-api-key header", http.StatusUnauthorized)
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
