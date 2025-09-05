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
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"

	"github.com/cardinalhq/lakerunner/configdb"
)

type databaseProvider struct {
	cdb                 ConfigDBStoreageProfileFetcher
	cacheBucket         *ttlcache.Cache[string, *StorageProfile]
	cacheOrg            *ttlcache.Cache[string, *StorageProfile]
	cacheBucketList     *ttlcache.Cache[string, []StorageProfile]
	cacheResolveOrg     *ttlcache.Cache[string, uuid.UUID]
	cacheOrgInstance    *ttlcache.Cache[string, *StorageProfile]
	cacheOrgCollector   *ttlcache.Cache[string, *StorageProfile]
	cacheLowestInstance *ttlcache.Cache[string, *StorageProfile]
}

var _ StorageProfileProvider = (*databaseProvider)(nil)

type ConfigDBStoreageProfileFetcher interface {
	GetBucketConfiguration(ctx context.Context, bucketName string) (configdb.BucketConfiguration, error)
	GetOrganizationsByBucket(ctx context.Context, bucketName string) ([]uuid.UUID, error)
	CheckOrgBucketAccess(ctx context.Context, arg configdb.CheckOrgBucketAccessParams) (bool, error)
	GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (uuid.UUID, error)
	GetBucketByOrganization(ctx context.Context, organizationID uuid.UUID) (string, error)
	GetOrganizationBucketByInstance(ctx context.Context, arg configdb.GetOrganizationBucketByInstanceParams) (configdb.GetOrganizationBucketByInstanceRow, error)
	GetOrganizationBucketByCollector(ctx context.Context, arg configdb.GetOrganizationBucketByCollectorParams) (configdb.GetOrganizationBucketByCollectorRow, error)
	GetDefaultOrganizationBucket(ctx context.Context, organizationID uuid.UUID) (configdb.GetDefaultOrganizationBucketRow, error)
	GetLowestInstanceOrganizationBucket(ctx context.Context, arg configdb.GetLowestInstanceOrganizationBucketParams) (configdb.GetLowestInstanceOrganizationBucketRow, error)
}

var _ ConfigDBStoreageProfileFetcher = (*configdb.Store)(nil)

func NewDatabaseProvider(cdb ConfigDBStoreageProfileFetcher) StorageProfileProvider {
	ttl := 1 * time.Minute
	return &databaseProvider{
		cdb:                 cdb,
		cacheBucket:         ttlcache.New(ttlcache.WithTTL[string, *StorageProfile](ttl)),
		cacheOrg:            ttlcache.New(ttlcache.WithTTL[string, *StorageProfile](ttl)),
		cacheBucketList:     ttlcache.New(ttlcache.WithTTL[string, []StorageProfile](ttl)),
		cacheResolveOrg:     ttlcache.New(ttlcache.WithTTL[string, uuid.UUID](ttl)),
		cacheOrgInstance:    ttlcache.New(ttlcache.WithTTL[string, *StorageProfile](ttl)),
		cacheOrgCollector:   ttlcache.New(ttlcache.WithTTL[string, *StorageProfile](ttl)),
		cacheLowestInstance: ttlcache.New(ttlcache.WithTTL[string, *StorageProfile](ttl)),
	}
}

func (p *databaseProvider) GetStorageProfileForBucket(ctx context.Context, organizationID uuid.UUID, bucketName string) (StorageProfile, error) {
	cacheKey := fmt.Sprintf("%s:%s", organizationID, bucketName)

	// Check cache first
	if item := p.cacheBucket.Get(cacheKey); item != nil {
		if item.Value() == nil {
			// Cached negative response
			return StorageProfile{}, fmt.Errorf("cached negative response for organization %s bucket %s", organizationID, bucketName)
		}
		return *item.Value(), nil
	}

	// For backward compatibility, get the default organization bucket entry (first by instance_num, collector_name)
	// This method should be deprecated in favor of the more specific methods
	result, err := p.cdb.GetDefaultOrganizationBucket(ctx, organizationID)
	if err != nil {
		// Cache negative response
		p.cacheBucket.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, fmt.Errorf("failed to get default organization bucket: %w", err)
	}

	// Verify it matches the requested bucket
	if result.BucketName != bucketName {
		// Cache negative response
		p.cacheBucket.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, fmt.Errorf("organization %s default bucket %s does not match requested bucket %s", organizationID, result.BucketName, bucketName)
	}

	profile := p.rowToStorageProfile(result.OrganizationID, result.InstanceNum, result.CollectorName,
		result.BucketName, result.CloudProvider, result.Region, result.Role, result.Endpoint,
		result.UsePathStyle, result.InsecureTls)

	// Cache positive response
	p.cacheBucket.Set(cacheKey, &profile, ttlcache.DefaultTTL)
	return profile, nil
}

func (p *databaseProvider) GetStorageProfileForOrganization(ctx context.Context, organizationID uuid.UUID) (StorageProfile, error) {
	cacheKey := organizationID.String()

	// Check cache first
	if item := p.cacheOrg.Get(cacheKey); item != nil {
		if item.Value() == nil {
			// Cached negative response
			return StorageProfile{}, fmt.Errorf("cached negative response for organization %s", organizationID)
		}
		return *item.Value(), nil
	}

	// Get the bucket for this organization
	bucketName, err := p.cdb.GetBucketByOrganization(ctx, organizationID)
	if err != nil {
		// Cache negative response
		p.cacheOrg.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, fmt.Errorf("failed to get bucket for organization %s: %w", organizationID, err)
	}

	// Get the storage profile for this organization and bucket
	profile, err := p.GetStorageProfileForBucket(ctx, organizationID, bucketName)
	if err != nil {
		// Cache negative response
		p.cacheOrg.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, err
	}

	// Cache positive response
	p.cacheOrg.Set(cacheKey, &profile, ttlcache.DefaultTTL)
	return profile, nil
}

func (p *databaseProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error) {
	cacheKey := bucketName

	// Check cache first
	if item := p.cacheBucketList.Get(cacheKey); item != nil {
		return item.Value(), nil
	}

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

	// Cache positive response
	p.cacheBucketList.Set(cacheKey, ret, ttlcache.DefaultTTL)
	return ret, nil
}

func (p *databaseProvider) ResolveOrganization(ctx context.Context, bucketName, objectPath string) (uuid.UUID, error) {
	cacheKey := fmt.Sprintf("%s:%s", bucketName, objectPath)

	// Check cache first
	if item := p.cacheResolveOrg.Get(cacheKey); item != nil {
		return item.Value(), nil
	}

	pathParts := strings.Split(strings.Trim(objectPath, "/"), "/")

	// Extract signal from first path segment
	var signal string
	if len(pathParts) >= 1 {
		switch pathParts[0] {
		case "logs", "metrics", "traces":
			signal = pathParts[0]
		default:
			signal = "logs" // Default fallback
		}
	} else {
		signal = "logs" // Default fallback
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
				// Cache positive response
				p.cacheResolveOrg.Set(cacheKey, orgID, ttlcache.DefaultTTL)
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
		// Cache positive response
		p.cacheResolveOrg.Set(cacheKey, orgID, ttlcache.DefaultTTL)
		return orgID, nil
	}

	// 3. If single org owns the bucket, use that
	orgs, err := p.cdb.GetOrganizationsByBucket(ctx, bucketName)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to get organizations for bucket: %w", err)
	}
	if len(orgs) == 1 {
		// Cache positive response
		p.cacheResolveOrg.Set(cacheKey, orgs[0], ttlcache.DefaultTTL)
		return orgs[0], nil
	}

	return uuid.Nil, fmt.Errorf("unable to resolve organization for path %s in bucket %s: %d organizations found", objectPath, bucketName, len(orgs))
}

func (p *databaseProvider) GetStorageProfileForOrganizationAndInstance(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (StorageProfile, error) {
	cacheKey := fmt.Sprintf("%s:%d", organizationID, instanceNum)

	// Check cache first
	if item := p.cacheOrgInstance.Get(cacheKey); item != nil {
		if item.Value() == nil {
			// Cached negative response
			return StorageProfile{}, fmt.Errorf("cached negative response for organization %s instance %d", organizationID, instanceNum)
		}
		return *item.Value(), nil
	}

	// Get organization bucket configuration by instance number
	result, err := p.cdb.GetOrganizationBucketByInstance(ctx, configdb.GetOrganizationBucketByInstanceParams{
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
	})
	if err != nil {
		// Cache negative response
		p.cacheOrgInstance.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, fmt.Errorf("failed to get organization bucket by instance: %w", err)
	}

	profile := p.rowToStorageProfile(result.OrganizationID, result.InstanceNum, result.CollectorName,
		result.BucketName, result.CloudProvider, result.Region, result.Role, result.Endpoint,
		result.UsePathStyle, result.InsecureTls)

	// Cache positive response
	p.cacheOrgInstance.Set(cacheKey, &profile, ttlcache.DefaultTTL)
	return profile, nil
}

func (p *databaseProvider) GetStorageProfileForOrganizationAndCollector(ctx context.Context, organizationID uuid.UUID, collectorName string) (StorageProfile, error) {
	cacheKey := fmt.Sprintf("%s:%s", organizationID, collectorName)

	// Check cache first
	if item := p.cacheOrgCollector.Get(cacheKey); item != nil {
		if item.Value() == nil {
			// Cached negative response
			return StorageProfile{}, fmt.Errorf("cached negative response for organization %s collector %s", organizationID, collectorName)
		}
		return *item.Value(), nil
	}

	// Get organization bucket configuration by collector name
	result, err := p.cdb.GetOrganizationBucketByCollector(ctx, configdb.GetOrganizationBucketByCollectorParams{
		OrganizationID: organizationID,
		CollectorName:  collectorName,
	})
	if err != nil {
		// Cache negative response
		p.cacheOrgCollector.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, fmt.Errorf("failed to get organization bucket by collector: %w", err)
	}

	profile := p.rowToStorageProfile(result.OrganizationID, result.InstanceNum, result.CollectorName,
		result.BucketName, result.CloudProvider, result.Region, result.Role, result.Endpoint,
		result.UsePathStyle, result.InsecureTls)

	// Cache positive response
	p.cacheOrgCollector.Set(cacheKey, &profile, ttlcache.DefaultTTL)
	return profile, nil
}

func (p *databaseProvider) GetLowestInstanceStorageProfile(ctx context.Context, organizationID uuid.UUID, bucketName string) (StorageProfile, error) {
	cacheKey := fmt.Sprintf("%s:%s", organizationID, bucketName)

	// Check cache first
	if item := p.cacheLowestInstance.Get(cacheKey); item != nil {
		if item.Value() == nil {
			// Cached negative response
			return StorageProfile{}, fmt.Errorf("cached negative response for organization %s bucket %s (lowest instance)", organizationID, bucketName)
		}
		return *item.Value(), nil
	}

	// Get organization bucket configuration with lowest instance number for specific bucket
	result, err := p.cdb.GetLowestInstanceOrganizationBucket(ctx, configdb.GetLowestInstanceOrganizationBucketParams{
		OrganizationID: organizationID,
		BucketName:     bucketName,
	})
	if err != nil {
		// Cache negative response
		p.cacheLowestInstance.Set(cacheKey, nil, ttlcache.DefaultTTL)
		return StorageProfile{}, fmt.Errorf("failed to get lowest instance organization bucket: %w", err)
	}

	profile := p.rowToStorageProfile(result.OrganizationID, result.InstanceNum, result.CollectorName,
		result.BucketName, result.CloudProvider, result.Region, result.Role, result.Endpoint,
		result.UsePathStyle, result.InsecureTls)

	// Cache positive response
	p.cacheLowestInstance.Set(cacheKey, &profile, ttlcache.DefaultTTL)
	return profile, nil
}

// Helper function to convert query result row to StorageProfile
func (p *databaseProvider) rowToStorageProfile(organizationID uuid.UUID, instanceNum int16, collectorName, bucketName, cloudProvider, region string, role, endpoint *string, usePathStyle, insecureTLS bool) StorageProfile {
	ret := StorageProfile{
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
		CollectorName:  collectorName,
		CloudProvider:  cloudProvider,
		Region:         region,
		Bucket:         bucketName,
		UsePathStyle:   usePathStyle,
		InsecureTLS:    insecureTLS,
	}

	if role != nil {
		ret.Role = *role
	}
	if endpoint != nil {
		ret.Endpoint = *endpoint
	}

	return ret
}
