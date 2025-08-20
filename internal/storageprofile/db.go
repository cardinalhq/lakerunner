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

package storageprofile

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/configdb"
)

type databaseProvider struct {
	cdb ConfigDBStoreageProfileFetcher
}

var _ StorageProfileProvider = (*databaseProvider)(nil)

type ConfigDBStoreageProfileFetcher interface {
	GetBucketConfiguration(ctx context.Context, bucketName string) (configdb.LrconfigBucketConfiguration, error)
	GetOrganizationsByBucket(ctx context.Context, bucketName string) ([]uuid.UUID, error)
	CheckOrgBucketAccess(ctx context.Context, arg configdb.CheckOrgBucketAccessParams) (bool, error)
	GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (uuid.UUID, error)
	GetBucketByOrganization(ctx context.Context, organizationID uuid.UUID) (string, error)
}

var _ ConfigDBStoreageProfileFetcher = (*configdb.Store)(nil)

func NewDatabaseProvider(cdb ConfigDBStoreageProfileFetcher) StorageProfileProvider {
	return &databaseProvider{
		cdb: cdb,
	}
}

func (p *databaseProvider) GetStorageProfileForBucket(ctx context.Context, organizationID uuid.UUID, bucketName string) (StorageProfile, error) {
	// Verify the organization has access to this bucket
	hasAccess, err := p.cdb.CheckOrgBucketAccess(ctx, configdb.CheckOrgBucketAccessParams{
		OrgID:      organizationID,
		BucketName: bucketName,
	})
	if err != nil {
		return StorageProfile{}, fmt.Errorf("failed to check org bucket access: %w", err)
	}
	if !hasAccess {
		return StorageProfile{}, fmt.Errorf("organization %s does not have access to bucket %s", organizationID, bucketName)
	}

	// Get the bucket configuration
	bucketConfig, err := p.cdb.GetBucketConfiguration(ctx, bucketName)
	if err != nil {
		return StorageProfile{}, fmt.Errorf("failed to get bucket configuration: %w", err)
	}

	// Create storage profile from bucket configuration
	ret := StorageProfile{
		OrganizationID: organizationID,
		CloudProvider:  bucketConfig.CloudProvider,
		Region:         bucketConfig.Region,
		Bucket:         bucketConfig.BucketName,
		UsePathStyle:   bucketConfig.UsePathStyle,
		InsecureTLS:    bucketConfig.InsecureTls,
	}

	if bucketConfig.Role != nil {
		ret.Role = *bucketConfig.Role
	}
	if bucketConfig.Endpoint != nil {
		ret.Endpoint = *bucketConfig.Endpoint
	}

	return ret, nil
}

func (p *databaseProvider) GetStorageProfileForOrganization(ctx context.Context, organizationID uuid.UUID) (StorageProfile, error) {
	// Get the bucket for this organization
	bucketName, err := p.cdb.GetBucketByOrganization(ctx, organizationID)
	if err != nil {
		return StorageProfile{}, fmt.Errorf("failed to get bucket for organization %s: %w", organizationID, err)
	}

	// Get the storage profile for this organization and bucket
	return p.GetStorageProfileForBucket(ctx, organizationID, bucketName)
}

func (p *databaseProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error) {
	// Get bucket configuration
	bucketConfig, err := p.cdb.GetBucketConfiguration(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket configuration: %w", err)
	}

	// Get all organizations with access to this bucket
	orgs, err := p.cdb.GetOrganizationsByBucket(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get organizations for bucket: %w", err)
	}

	// Create storage profiles for each organization
	ret := make([]StorageProfile, len(orgs))
	for i, orgID := range orgs {
		ret[i] = StorageProfile{
			OrganizationID: orgID,
			CloudProvider:  bucketConfig.CloudProvider,
			Region:         bucketConfig.Region,
			Bucket:         bucketConfig.BucketName,
			UsePathStyle:   bucketConfig.UsePathStyle,
			InsecureTLS:    bucketConfig.InsecureTls,
		}

		if bucketConfig.Role != nil {
			ret[i].Role = *bucketConfig.Role
		}
		if bucketConfig.Endpoint != nil {
			ret[i].Endpoint = *bucketConfig.Endpoint
		}
	}

	return ret, nil
}

func (p *databaseProvider) ResolveOrganization(ctx context.Context, bucketName, objectPath string) (uuid.UUID, error) {
	pathParts := strings.Split(strings.Trim(objectPath, "/"), "/")

	// Extract signal from first path segment
	var signal string
	if len(pathParts) >= 1 {
		switch pathParts[0] {
		case "logs", "metrics", "traces":
			signal = pathParts[0]
		default:
			signal = "metrics" // Default fallback
		}
	} else {
		signal = "metrics" // Default fallback
	}

	// 1. Try to extract UUID from second path segment (signal/UUID)
	if len(pathParts) >= 2 {
		if orgID, err := uuid.Parse(pathParts[1]); err == nil {
			// Verify this org has access to the bucket
			hasAccess, err := p.cdb.CheckOrgBucketAccess(ctx, configdb.CheckOrgBucketAccessParams{
				OrgID:      orgID,
				BucketName: bucketName,
			})
			if err != nil {
				return uuid.Nil, fmt.Errorf("failed to check org bucket access: %w", err)
			}
			if hasAccess {
				return orgID, nil
			}
			// If UUID is valid but org doesn't have access, continue to prefix matching
		}
	}

	// 2. Try longest prefix match with signal
	orgID, err := p.cdb.GetLongestPrefixMatch(ctx, configdb.GetLongestPrefixMatchParams{
		BucketName: bucketName,
		Signal:     signal,
		ObjectPath: objectPath,
	})
	if err == nil {
		return orgID, nil
	}

	// 3. If single org owns the bucket, use that
	orgs, err := p.cdb.GetOrganizationsByBucket(ctx, bucketName)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get organizations for bucket: %w", err)
	}
	if len(orgs) == 1 {
		return orgs[0], nil
	}

	return uuid.Nil, fmt.Errorf("unable to resolve organization for path %s in bucket %s: %d organizations found", objectPath, bucketName, len(orgs))
}
