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

package configdb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// CreateOrganizationAPIKeyWithMapping creates an API key and its organization mapping in a transaction
func (store *Store) CreateOrganizationAPIKeyWithMapping(ctx context.Context, params CreateOrganizationAPIKeyParams, orgID uuid.UUID) (*OrganizationApiKey, error) {
	var apiKey *OrganizationApiKey

	err := store.execTx(ctx, func(s *Store) error {
		// Create the API key
		createdKey, err := s.CreateOrganizationAPIKey(ctx, params)
		if err != nil {
			return fmt.Errorf("failed to create API key: %w", err)
		}
		apiKey = &createdKey

		// Create the organization mapping
		_, err = s.CreateOrganizationAPIKeyMapping(ctx, CreateOrganizationAPIKeyMappingParams{
			ApiKeyID:       createdKey.ID,
			OrganizationID: orgID,
		})
		if err != nil {
			return fmt.Errorf("failed to create API key mapping: %w", err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return apiKey, nil
}

// DeleteOrganizationAPIKeyWithMappings deletes an API key and its mappings in a transaction
func (store *Store) DeleteOrganizationAPIKeyWithMappings(ctx context.Context, keyID uuid.UUID) error {
	return store.execTx(ctx, func(s *Store) error {
		// Delete the mapping first (foreign key constraint)
		err := s.DeleteOrganizationAPIKeyMapping(ctx, keyID)
		if err != nil {
			return fmt.Errorf("failed to delete API key mapping: %w", err)
		}

		// Delete the API key
		err = s.DeleteOrganizationAPIKey(ctx, keyID)
		if err != nil {
			return fmt.Errorf("failed to delete API key: %w", err)
		}

		return nil
	})
}
