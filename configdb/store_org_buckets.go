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

// AddOrganizationBucket adds a bucket to an organization in a transaction
func (store *Store) AddOrganizationBucket(ctx context.Context, orgID uuid.UUID, bucketName string, instanceNum int16, collectorName string) error {
	return store.execTx(ctx, func(s *Store) error {
		// Get bucket configuration
		bucket, err := s.GetBucketConfigurationByName(ctx, bucketName)
		if err != nil {
			return fmt.Errorf("failed to get bucket configuration: %w", err)
		}

		// Create the organization bucket
		_, err = s.CreateOrganizationBucket(ctx, CreateOrganizationBucketParams{
			OrganizationID: orgID,
			BucketID:       bucket.ID,
			InstanceNum:    instanceNum,
			CollectorName:  collectorName,
		})
		if err != nil {
			return fmt.Errorf("failed to add organization bucket: %w", err)
		}

		return nil
	})
}
