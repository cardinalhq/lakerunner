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
