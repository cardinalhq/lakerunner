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
	"errors"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/configdb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// mockConfigDBStoreageProfileFetcher implements ConfigDBStoreageProfileFetcher for testing
type mockConfigDBStoreageProfileFetcher struct {
	profile configdb.GetStorageProfileRow
	err     error
}

func (m *mockConfigDBStoreageProfileFetcher) GetStorageProfile(ctx context.Context, params configdb.GetStorageProfileParams) (configdb.GetStorageProfileRow, error) {
	if m.err != nil {
		return configdb.GetStorageProfileRow{}, m.err
	}
	return m.profile, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetStorageProfileByCollectorName(ctx context.Context, params configdb.GetStorageProfileByCollectorNameParams) (configdb.GetStorageProfileByCollectorNameRow, error) {
	if m.err != nil {
		return configdb.GetStorageProfileByCollectorNameRow{}, m.err
	}
	return configdb.GetStorageProfileByCollectorNameRow{
		OrganizationID: m.profile.OrganizationID,
		ExternalID:     m.profile.ExternalID,
		CloudProvider:  m.profile.CloudProvider,
		Region:         m.profile.Region,
		Bucket:         m.profile.Bucket,
		Role:           m.profile.Role,
	}, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]configdb.GetStorageProfilesByBucketNameRow, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []configdb.GetStorageProfilesByBucketNameRow{
		{
			OrganizationID: m.profile.OrganizationID,
			ExternalID:     m.profile.ExternalID,
			CloudProvider:  m.profile.CloudProvider,
			Region:         m.profile.Region,
			Bucket:         m.profile.Bucket,
			Role:           m.profile.Role,
		},
	}, nil
}

func TestDatabaseProvider_Get_SuccessWithRole(t *testing.T) {
	orgID := uuid.New()
	role := "admin"
	mockProfile := configdb.GetStorageProfileRow{
		OrganizationID: orgID,
		InstanceNum:    1,
		ExternalID:     "ext-123",
		CloudProvider:  "aws",
		Region:         "us-west-2",
		Bucket:         "bucket-1",
		Role:           &role,
	}
	mockFetcher := &mockConfigDBStoreageProfileFetcher{profile: mockProfile}
	provider := NewDatabaseProvider(mockFetcher)

	got, err := provider.Get(context.Background(), orgID, 1)
	assert.NoError(t, err)
	assert.Equal(t, orgID, got.OrganizationID)
	assert.Equal(t, int16(1), got.InstanceNum)
	assert.Equal(t, "ext-123", got.CollectorName)
	assert.Equal(t, "aws", got.CloudProvider)
	assert.Equal(t, "us-west-2", got.Region)
	assert.Equal(t, "bucket-1", got.Bucket)
	assert.Equal(t, "admin", got.Role)
}

func TestDatabaseProvider_Get_SuccessWithoutRole(t *testing.T) {
	orgID := uuid.New()
	mockProfile := configdb.GetStorageProfileRow{
		OrganizationID: orgID,
		InstanceNum:    2,
		ExternalID:     "ext-456",
		CloudProvider:  "gcp",
		Region:         "europe-west1",
		Bucket:         "bucket-2",
		Role:           nil,
	}
	mockFetcher := &mockConfigDBStoreageProfileFetcher{profile: mockProfile}
	provider := NewDatabaseProvider(mockFetcher)

	got, err := provider.Get(context.Background(), orgID, 2)
	assert.NoError(t, err)
	assert.Equal(t, orgID, got.OrganizationID)
	assert.Equal(t, int16(2), got.InstanceNum)
	assert.Equal(t, "ext-456", got.CollectorName)
	assert.Equal(t, "gcp", got.CloudProvider)
	assert.Equal(t, "europe-west1", got.Region)
	assert.Equal(t, "bucket-2", got.Bucket)
	assert.Equal(t, "", got.Role)
}

func TestDatabaseProvider_Get_Error(t *testing.T) {
	orgID := uuid.New()
	mockFetcher := &mockConfigDBStoreageProfileFetcher{
		err: errors.New("db error"),
	}
	provider := NewDatabaseProvider(mockFetcher)

	_, err := provider.Get(context.TODO(), orgID, 3)
	assert.Error(t, err)
}
