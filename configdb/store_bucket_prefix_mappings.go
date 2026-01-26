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

package configdb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// CreateBucketPrefixMappingForOrg creates a bucket prefix mapping in a transaction
func (store *Store) CreateBucketPrefixMappingForOrg(ctx context.Context, bucketName string, orgID uuid.UUID, pathPrefix string, signal string) (*BucketPrefixMapping, error) {
	var mapping *BucketPrefixMapping

	err := store.execTx(ctx, func(s *Store) error {
		// Get bucket configuration
		bucket, err := s.GetBucketConfigurationByName(ctx, bucketName)
		if err != nil {
			return fmt.Errorf("failed to get bucket configuration: %w", err)
		}

		// Create the prefix mapping
		createdMapping, err := s.CreateBucketPrefixMapping(ctx, CreateBucketPrefixMappingParams{
			BucketID:       bucket.ID,
			OrganizationID: orgID,
			PathPrefix:     pathPrefix,
			Signal:         signal,
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket prefix mapping: %w", err)
		}
		mapping = &createdMapping

		return nil
	})

	if err != nil {
		return nil, err
	}
	return mapping, nil
}
