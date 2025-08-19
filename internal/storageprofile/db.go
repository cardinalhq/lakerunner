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

	"github.com/cardinalhq/lakerunner/internal/configdb"
)

type databaseProvider struct {
	cdb ConfigDBStoreageProfileFetcher
}

var _ StorageProfileProvider = (*databaseProvider)(nil)

type ConfigDBStoreageProfileFetcher interface {
	GetStorageProfile(ctx context.Context, params configdb.GetStorageProfileParams) (configdb.GetStorageProfileRow, error)
	GetStorageProfileByCollectorName(ctx context.Context, params configdb.GetStorageProfileByCollectorNameParams) (configdb.GetStorageProfileByCollectorNameRow, error)
	GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]configdb.GetStorageProfilesByBucketNameRow, error)
	// New methods for bucket management
	GetBucketConfiguration(ctx context.Context, bucketName string) (configdb.BucketConfiguration, error)
	GetOrganizationsByBucket(ctx context.Context, bucketName string) ([]uuid.UUID, error)
	CheckOrgBucketAccess(ctx context.Context, arg configdb.CheckOrgBucketAccessParams) (bool, error)
	GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (uuid.UUID, error)
}

var _ ConfigDBStoreageProfileFetcher = (*configdb.Store)(nil)

func NewDatabaseProvider(cdb ConfigDBStoreageProfileFetcher) StorageProfileProvider {
	return &databaseProvider{
		cdb: cdb,
	}
}

func (p *databaseProvider) Get(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (StorageProfile, error) {
	profile, err := p.cdb.GetStorageProfile(ctx, configdb.GetStorageProfileParams{
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
	})
	if err != nil {
		return StorageProfile{}, err
	}
	ret := StorageProfile{
		OrganizationID: profile.OrganizationID,
		InstanceNum:    profile.InstanceNum,
		CollectorName:  profile.ExternalID,
		CloudProvider:  profile.CloudProvider,
		Region:         profile.Region,
		Bucket:         profile.Bucket,
	}
	if profile.Role != nil {
		ret.Role = *profile.Role
	}
	return ret, nil
}

func (p *databaseProvider) GetByCollectorName(ctx context.Context, organizationID uuid.UUID, collectorName string) (StorageProfile, error) {
	profile, err := p.cdb.GetStorageProfileByCollectorName(ctx, configdb.GetStorageProfileByCollectorNameParams{
		OrganizationID: organizationID,
		CollectorName:  collectorName,
	})
	if err != nil {
		return StorageProfile{}, err
	}
	ret := StorageProfile{
		OrganizationID: profile.OrganizationID,
		InstanceNum:    profile.InstanceNum,
		CollectorName:  profile.ExternalID,
		CloudProvider:  profile.CloudProvider,
		Region:         profile.Region,
		Bucket:         profile.Bucket,
	}
	if profile.Role != nil {
		ret.Role = *profile.Role
	}
	return ret, nil
}

func (p *databaseProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error) {
	profiles, err := p.cdb.GetStorageProfilesByBucketName(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	ret := make([]StorageProfile, len(profiles))
	for i, p := range profiles {
		ret[i] = StorageProfile{
			OrganizationID: p.OrganizationID,
			InstanceNum:    p.InstanceNum,
			CollectorName:  p.ExternalID,
			CloudProvider:  p.CloudProvider,
			Region:         p.Region,
			Bucket:         p.Bucket,
		}
		if p.Role != nil {
			ret[i].Role = *p.Role
		}
	}
	return ret, nil
}

func (p *databaseProvider) ResolveOrganization(ctx context.Context, bucketName, objectPath string) (uuid.UUID, error) {
	// First check if the new bucket management tables exist by trying to get bucket config
	_, err := p.cdb.GetBucketConfiguration(ctx, bucketName)
	if err != nil {
		// If the new tables don't exist yet, fall back to old logic
		profiles, err := p.GetStorageProfilesByBucketName(ctx, bucketName)
		if err != nil {
			return uuid.Nil, err
		}
		if len(profiles) != 1 {
			return uuid.Nil, fmt.Errorf("expected exactly one storage profile for bucket %s, found %d", bucketName, len(profiles))
		}
		return profiles[0].OrganizationID, nil
	}

	// New resolution logic using the new tables
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
