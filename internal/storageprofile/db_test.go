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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/configdb"
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
		InstanceNum:    m.profile.InstanceNum,
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
			InstanceNum:    m.profile.InstanceNum,
			ExternalID:     m.profile.ExternalID,
			CloudProvider:  m.profile.CloudProvider,
			Region:         m.profile.Region,
			Bucket:         m.profile.Bucket,
			Role:           m.profile.Role,
		},
	}, nil
}

// New methods for bucket management (stub implementations for testing)
func (m *mockConfigDBStoreageProfileFetcher) GetBucketConfiguration(ctx context.Context, bucketName string) (configdb.BucketConfiguration, error) {
	if m.err != nil {
		return configdb.BucketConfiguration{}, m.err
	}
	// Return empty config to trigger fallback to old logic
	return configdb.BucketConfiguration{}, errors.New("bucket not found")
}

func (m *mockConfigDBStoreageProfileFetcher) GetOrganizationsByBucket(ctx context.Context, bucketName string) ([]uuid.UUID, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []uuid.UUID{m.profile.OrganizationID}, nil
}

func (m *mockConfigDBStoreageProfileFetcher) CheckOrgBucketAccess(ctx context.Context, arg configdb.CheckOrgBucketAccessParams) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	return arg.OrgID == m.profile.OrganizationID, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (uuid.UUID, error) {
	if m.err != nil {
		return uuid.Nil, m.err
	}
	return uuid.Nil, errors.New("no prefix match found")
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

func TestDatabaseProvider_GetByCollectorName(t *testing.T) {
	tests := []struct {
		name           string
		organizationID uuid.UUID
		collectorName  string
		mockProfile    configdb.GetStorageProfileRow
		mockErr        error
		wantProfile    StorageProfile
		wantErr        bool
	}{
		{
			name:           "success with role",
			organizationID: uuid.New(),
			collectorName:  "collector-123",
			mockProfile: configdb.GetStorageProfileRow{
				OrganizationID: uuid.New(),
				InstanceNum:    1,
				ExternalID:     "collector-123",
				CloudProvider:  "aws",
				Region:         "us-west-2",
				Bucket:         "test-bucket",
				Role:           stringPtr("admin-role"),
			},
			wantProfile: StorageProfile{
				OrganizationID: uuid.New(),
				InstanceNum:    1,
				CollectorName:  "collector-123",
				CloudProvider:  "aws",
				Region:         "us-west-2",
				Bucket:         "test-bucket",
				Role:           "admin-role",
			},
			wantErr: false,
		},
		{
			name:           "success without role",
			organizationID: uuid.New(),
			collectorName:  "collector-456",
			mockProfile: configdb.GetStorageProfileRow{
				OrganizationID: uuid.New(),
				InstanceNum:    2,
				ExternalID:     "collector-456",
				CloudProvider:  "gcp",
				Region:         "europe-west1",
				Bucket:         "test-bucket-2",
				Role:           nil,
			},
			wantProfile: StorageProfile{
				OrganizationID: uuid.New(),
				InstanceNum:    2,
				CollectorName:  "collector-456",
				CloudProvider:  "gcp",
				Region:         "europe-west1",
				Bucket:         "test-bucket-2",
				Role:           "",
			},
			wantErr: false,
		},
		{
			name:           "database error",
			organizationID: uuid.New(),
			collectorName:  "collector-error",
			mockErr:        errors.New("database connection failed"),
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up the expected profile organization ID to match
			if !tt.wantErr {
				tt.mockProfile.OrganizationID = tt.organizationID
				tt.wantProfile.OrganizationID = tt.organizationID
			}

			mockFetcher := &mockConfigDBStoreageProfileFetcher{
				profile: tt.mockProfile,
				err:     tt.mockErr,
			}
			provider := NewDatabaseProvider(mockFetcher)

			got, err := provider.GetByCollectorName(context.Background(), tt.organizationID, tt.collectorName)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantProfile, got)
		})
	}
}

func TestDatabaseProvider_GetStorageProfilesByBucketName(t *testing.T) {
	tests := []struct {
		name       string
		bucketName string
		mockErr    error
		wantCount  int
		wantErr    bool
	}{
		{
			name:       "success with multiple profiles",
			bucketName: "shared-bucket",
			wantCount:  1, // mock returns 1 profile
			wantErr:    false,
		},
		{
			name:       "database error",
			bucketName: "error-bucket",
			mockErr:    errors.New("database query failed"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orgID := uuid.New()
			mockProfile := configdb.GetStorageProfileRow{
				OrganizationID: orgID,
				InstanceNum:    1,
				ExternalID:     "test-collector",
				CloudProvider:  "aws",
				Region:         "us-west-2",
				Bucket:         tt.bucketName,
				Role:           stringPtr("test-role"),
			}

			mockFetcher := &mockConfigDBStoreageProfileFetcher{
				profile: mockProfile,
				err:     tt.mockErr,
			}
			provider := NewDatabaseProvider(mockFetcher)

			got, err := provider.GetStorageProfilesByBucketName(context.Background(), tt.bucketName)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
				return
			}

			assert.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
			if tt.wantCount > 0 {
				assert.Equal(t, tt.bucketName, got[0].Bucket)
				assert.Equal(t, orgID, got[0].OrganizationID)
				assert.Equal(t, "test-collector", got[0].CollectorName)
				assert.Equal(t, "test-role", got[0].Role)
			}
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
