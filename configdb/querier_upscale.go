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

	"github.com/google/uuid"
)

type StoreFull interface {
	Querier
	GetStorageProfile(ctx context.Context, arg GetStorageProfileParams) (GetStorageProfileRow, error)
	GetStorageProfileByCollectorName(ctx context.Context, organizationID uuid.UUID) (GetStorageProfileByCollectorNameRow, error)
	GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]GetStorageProfilesByBucketNameRow, error)

	// High-level transactional operations
	CreateOrganizationAPIKeyWithMapping(ctx context.Context, params CreateOrganizationAPIKeyParams, orgID uuid.UUID) (*OrganizationApiKey, error)
	DeleteOrganizationAPIKeyWithMappings(ctx context.Context, keyID uuid.UUID) error
	AddOrganizationBucket(ctx context.Context, orgID uuid.UUID, bucketName string, instanceNum int16, collectorName string) error
	CreateBucketPrefixMappingForOrg(ctx context.Context, bucketName string, orgID uuid.UUID, pathPrefix string, signal string) (*BucketPrefixMapping, error)
}

// QuerierFull is deprecated, use StoreFull instead
type QuerierFull = StoreFull
