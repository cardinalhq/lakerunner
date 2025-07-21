// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageprofile

import (
	"context"

	"github.com/cardinalhq/lakerunner/internal/configdb"
	"github.com/google/uuid"
)

type databaseProvider struct {
	cdb ConfigDBStoreageProfileFetcher
}

var _ StorageProfileProvider = (*databaseProvider)(nil)

type ConfigDBStoreageProfileFetcher interface {
	GetStorageProfile(ctx context.Context, params configdb.GetStorageProfileParams) (configdb.GetStorageProfileRow, error)
	GetStorageProfileByCollectorName(ctx context.Context, params configdb.GetStorageProfileByCollectorNameParams) (configdb.GetStorageProfileByCollectorNameRow, error)
	GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]configdb.GetStorageProfilesByBucketNameRow, error)
}

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
		Hosted:         profile.Hosted,
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
		Hosted:         profile.Hosted,
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
			Hosted:         p.Hosted,
		}
		if p.Role != nil {
			ret[i].Role = *p.Role
		}
	}
	return ret, nil
}
