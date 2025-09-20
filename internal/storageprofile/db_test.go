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

	"github.com/cardinalhq/lakerunner/configdb"
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

func (m *mockConfigDBStoreageProfileFetcher) GetStorageProfileByCollectorName(ctx context.Context, organizationID uuid.UUID) (configdb.GetStorageProfileByCollectorNameRow, error) {
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
	// Return a valid bucket configuration for testing
	config := configdb.BucketConfiguration{
		ID:            uuid.New(),
		BucketName:    m.profile.Bucket,
		CloudProvider: m.profile.CloudProvider,
		Region:        m.profile.Region,
		UsePathStyle:  true,  // Default for testing
		InsecureTls:   false, // Default for testing
	}
	if m.profile.Role != nil {
		config.Role = m.profile.Role
	}
	return config, nil
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

func (m *mockConfigDBStoreageProfileFetcher) GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (configdb.GetLongestPrefixMatchRow, error) {
	if m.err != nil {
		return configdb.GetLongestPrefixMatchRow{}, m.err
	}
	return configdb.GetLongestPrefixMatchRow{}, errors.New("no prefix match found")
}

func (m *mockConfigDBStoreageProfileFetcher) GetBucketByOrganization(ctx context.Context, organizationID uuid.UUID) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.profile.Bucket, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetOrganizationBucketByInstance(ctx context.Context, arg configdb.GetOrganizationBucketByInstanceParams) (configdb.GetOrganizationBucketByInstanceRow, error) {
	if m.err != nil {
		return configdb.GetOrganizationBucketByInstanceRow{}, m.err
	}
	return configdb.GetOrganizationBucketByInstanceRow{
		OrganizationID: arg.OrganizationID,
		InstanceNum:    arg.InstanceNum,
		CollectorName:  "default",
		BucketName:     m.profile.Bucket,
		CloudProvider:  m.profile.CloudProvider,
		Region:         m.profile.Region,
		Role:           m.profile.Role,
		Endpoint:       nil,
		UsePathStyle:   false,
		InsecureTls:    false,
	}, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetOrganizationBucketByCollector(ctx context.Context, arg configdb.GetOrganizationBucketByCollectorParams) (configdb.GetOrganizationBucketByCollectorRow, error) {
	if m.err != nil {
		return configdb.GetOrganizationBucketByCollectorRow{}, m.err
	}
	return configdb.GetOrganizationBucketByCollectorRow{
		OrganizationID: arg.OrganizationID,
		InstanceNum:    1,
		CollectorName:  arg.CollectorName,
		BucketName:     m.profile.Bucket,
		CloudProvider:  m.profile.CloudProvider,
		Region:         m.profile.Region,
		Role:           m.profile.Role,
		Endpoint:       nil,
		UsePathStyle:   false,
		InsecureTls:    false,
	}, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetDefaultOrganizationBucket(ctx context.Context, organizationID uuid.UUID) (configdb.GetDefaultOrganizationBucketRow, error) {
	if m.err != nil {
		return configdb.GetDefaultOrganizationBucketRow{}, m.err
	}
	return configdb.GetDefaultOrganizationBucketRow{
		OrganizationID: organizationID,
		InstanceNum:    1,
		CollectorName:  "default",
		BucketName:     m.profile.Bucket,
		CloudProvider:  m.profile.CloudProvider,
		Region:         m.profile.Region,
		Role:           m.profile.Role,
		Endpoint:       nil,
		UsePathStyle:   false,
		InsecureTls:    false,
	}, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetLowestInstanceOrganizationBucket(ctx context.Context, arg configdb.GetLowestInstanceOrganizationBucketParams) (configdb.GetLowestInstanceOrganizationBucketRow, error) {
	if m.err != nil {
		return configdb.GetLowestInstanceOrganizationBucketRow{}, m.err
	}
	return configdb.GetLowestInstanceOrganizationBucketRow{
		OrganizationID: arg.OrganizationID,
		InstanceNum:    1,
		CollectorName:  "default",
		BucketName:     arg.BucketName,
		CloudProvider:  m.profile.CloudProvider,
		Region:         m.profile.Region,
		Role:           m.profile.Role,
		Endpoint:       nil,
		UsePathStyle:   false,
		InsecureTls:    false,
	}, nil
}

func (m *mockConfigDBStoreageProfileFetcher) GetBucketPrefixMappings(ctx context.Context, bucketName string) ([]configdb.GetBucketPrefixMappingsRow, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []configdb.GetBucketPrefixMappingsRow{}, nil
}

func TestDatabaseProvider_Get_SuccessWithRole(t *testing.T) {
	orgID := uuid.New()
	role := "admin"
	mockProfile := configdb.GetStorageProfileRow{
		OrganizationID: orgID,
		InstanceNum:    9999,
		ExternalID:     "ext-123",
		CloudProvider:  "aws",
		Region:         "us-west-2",
		Bucket:         "bucket-1",
		Role:           &role,
	}
	mockFetcher := &mockConfigDBStoreageProfileFetcher{profile: mockProfile}
	provider := NewDatabaseProvider(mockFetcher)

	// Test new GetStorageProfileForOrganization method instead
	got, err := provider.GetStorageProfileForOrganization(context.Background(), orgID)
	assert.NoError(t, err)
	assert.Equal(t, orgID, got.OrganizationID)
	assert.Equal(t, "aws", got.CloudProvider)
	assert.Equal(t, "us-west-2", got.Region)
	assert.Equal(t, "bucket-1", got.Bucket)
	assert.Equal(t, "admin", got.Role)
}

func TestDatabaseProvider_Get_SuccessWithoutRole(t *testing.T) {
	orgID := uuid.New()
	mockProfile := configdb.GetStorageProfileRow{
		OrganizationID: orgID,
		InstanceNum:    9999,
		ExternalID:     "ext-456",
		CloudProvider:  "gcp",
		Region:         "europe-west1",
		Bucket:         "bucket-2",
		Role:           nil,
	}
	mockFetcher := &mockConfigDBStoreageProfileFetcher{profile: mockProfile}
	provider := NewDatabaseProvider(mockFetcher)

	// Test new GetStorageProfileForOrganization method instead
	got, err := provider.GetStorageProfileForOrganization(context.Background(), orgID)
	assert.NoError(t, err)
	assert.Equal(t, orgID, got.OrganizationID)
	assert.Equal(t, "gcp", got.CloudProvider)
	assert.Equal(t, "europe-west1", got.Region)
	assert.Equal(t, "bucket-2", got.Bucket)
	assert.Equal(t, "", got.Role)
}

func TestDatabaseProvider_GetStorageProfileForOrganization_Error(t *testing.T) {
	orgID := uuid.New()
	mockFetcher := &mockConfigDBStoreageProfileFetcher{
		err: errors.New("db error"),
	}
	provider := NewDatabaseProvider(mockFetcher)

	_, err := provider.GetStorageProfileForOrganization(context.TODO(), orgID)
	assert.Error(t, err)
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
			role := "test-role"
			mockProfile := configdb.GetStorageProfileRow{
				OrganizationID: orgID,
				InstanceNum:    9999,
				ExternalID:     "test-collector",
				CloudProvider:  "aws",
				Region:         "us-west-2",
				Bucket:         tt.bucketName,
				Role:           &role,
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
				assert.Equal(t, "test-role", got[0].Role)
			}
		})
	}
}

// multiOrgMockFetcher is a mock that returns multiple organizations for testing

func TestDatabaseProvider_rowToStorageProfile(t *testing.T) {
	tests := []struct {
		name           string
		organizationID uuid.UUID
		instanceNum    int16
		collectorName  string
		bucketName     string
		cloudProvider  string
		region         string
		role           *string
		endpoint       *string
		usePathStyle   bool
		insecureTLS    bool
		want           StorageProfile
	}{
		{
			name:           "complete profile with role and endpoint",
			organizationID: uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			instanceNum:    42,
			collectorName:  "test-collector",
			bucketName:     "my-bucket",
			cloudProvider:  "aws",
			region:         "us-east-1",
			role:           stringPtr("arn:aws:iam::123456789012:role/MyRole"),
			endpoint:       stringPtr("https://s3.amazonaws.com"),
			usePathStyle:   true,
			insecureTLS:    false,
			want: StorageProfile{
				OrganizationID: uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
				InstanceNum:    42,
				CollectorName:  "test-collector",
				CloudProvider:  "aws",
				Region:         "us-east-1",
				Bucket:         "my-bucket",
				Role:           "arn:aws:iam::123456789012:role/MyRole",
				Endpoint:       "https://s3.amazonaws.com",
				UsePathStyle:   true,
				InsecureTLS:    false,
			},
		},
		{
			name:           "minimal profile without role and endpoint",
			organizationID: uuid.MustParse("987fcdeb-51a2-43d7-b890-123456789abc"),
			instanceNum:    1,
			collectorName:  "minimal-collector",
			bucketName:     "simple-bucket",
			cloudProvider:  "gcp",
			region:         "us-central1",
			role:           nil,
			endpoint:       nil,
			usePathStyle:   false,
			insecureTLS:    true,
			want: StorageProfile{
				OrganizationID: uuid.MustParse("987fcdeb-51a2-43d7-b890-123456789abc"),
				InstanceNum:    1,
				CollectorName:  "minimal-collector",
				CloudProvider:  "gcp",
				Region:         "us-central1",
				Bucket:         "simple-bucket",
				Role:           "",
				Endpoint:       "",
				UsePathStyle:   false,
				InsecureTLS:    true,
			},
		},
		{
			name:           "profile with role but no endpoint",
			organizationID: uuid.MustParse("456789ab-cdef-1234-5678-90abcdef1234"),
			instanceNum:    0,
			collectorName:  "partial-collector",
			bucketName:     "role-only-bucket",
			cloudProvider:  "azure",
			region:         "eastus",
			role:           stringPtr("storage-admin"),
			endpoint:       nil,
			usePathStyle:   true,
			insecureTLS:    false,
			want: StorageProfile{
				OrganizationID: uuid.MustParse("456789ab-cdef-1234-5678-90abcdef1234"),
				InstanceNum:    0,
				CollectorName:  "partial-collector",
				CloudProvider:  "azure",
				Region:         "eastus",
				Bucket:         "role-only-bucket",
				Role:           "storage-admin",
				Endpoint:       "",
				UsePathStyle:   true,
				InsecureTLS:    false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &databaseProvider{}
			got := provider.rowToStorageProfile(
				tt.organizationID,
				tt.instanceNum,
				tt.collectorName,
				tt.bucketName,
				tt.cloudProvider,
				tt.region,
				tt.role,
				tt.endpoint,
				tt.usePathStyle,
				tt.insecureTLS,
			)

			assert.Equal(t, tt.want, got)
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

func TestFindLongestPrefixMatch(t *testing.T) {
	testOrgID := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
	otherOrgID := uuid.MustParse("987fcdeb-51a2-43d7-b890-123456789abc")

	tests := []struct {
		name       string
		objectPath string
		mappings   []PrefixMapping
		want       *PrefixMapping
	}{
		{
			name:       "exact match",
			objectPath: "logs/app1/file.json",
			mappings: []PrefixMapping{
				{OrganizationID: testOrgID, PathPrefix: "logs/app1/", Signal: "logs"},
				{OrganizationID: otherOrgID, PathPrefix: "logs/", Signal: "logs"},
			},
			want: &PrefixMapping{OrganizationID: testOrgID, PathPrefix: "logs/app1/", Signal: "logs"},
		},
		{
			name:       "longest prefix wins",
			objectPath: "metrics/service1/cpu.parquet",
			mappings: []PrefixMapping{
				{OrganizationID: testOrgID, PathPrefix: "metrics/service1/", Signal: "metrics"},
				{OrganizationID: otherOrgID, PathPrefix: "metrics/", Signal: "metrics"},
				{OrganizationID: otherOrgID, PathPrefix: "m", Signal: "metrics"},
			},
			want: &PrefixMapping{OrganizationID: testOrgID, PathPrefix: "metrics/service1/", Signal: "metrics"},
		},
		{
			name:       "no match",
			objectPath: "traces/span1/data.json",
			mappings: []PrefixMapping{
				{OrganizationID: testOrgID, PathPrefix: "logs/", Signal: "logs"},
				{OrganizationID: otherOrgID, PathPrefix: "metrics/", Signal: "metrics"},
			},
			want: nil,
		},
		{
			name:       "empty mappings",
			objectPath: "any/path/file.json",
			mappings:   []PrefixMapping{},
			want:       nil,
		},
		{
			name:       "single character prefix",
			objectPath: "logs/file.json",
			mappings: []PrefixMapping{
				{OrganizationID: testOrgID, PathPrefix: "l", Signal: "logs"},
			},
			want: &PrefixMapping{OrganizationID: testOrgID, PathPrefix: "l", Signal: "logs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findLongestPrefixMatch(tt.objectPath, tt.mappings)
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tt.want.OrganizationID, got.OrganizationID)
				assert.Equal(t, tt.want.PathPrefix, got.PathPrefix)
				assert.Equal(t, tt.want.Signal, got.Signal)
			}
		})
	}
}
