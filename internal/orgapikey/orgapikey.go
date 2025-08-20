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

package orgapikey

import (
	"context"

	"github.com/google/uuid"
)

// OrganizationAPIKey represents an API key that provides access to organization data
type OrganizationAPIKey struct {
	Name           string    `json:"name"`
	Description    string    `json:"description,omitempty"`
	OrganizationID uuid.UUID `json:"organization_id"`
}

// OrganizationAPIKeyProvider validates organization-scoped API keys and returns organization context
type OrganizationAPIKeyProvider interface {
	// ValidateAPIKey validates an API key and returns the associated organization ID
	ValidateAPIKey(ctx context.Context, apiKey string) (*uuid.UUID, error)

	// GetAPIKeyInfo returns metadata for a valid API key
	GetAPIKeyInfo(ctx context.Context, apiKey string) (*OrganizationAPIKey, error)
}

// TODO: Implement database-backed provider that uses organization API key tables
// TODO: Consider file-based provider that syncs to database via sweeper (if needed)
