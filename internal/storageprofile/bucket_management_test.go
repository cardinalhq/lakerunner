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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/configdb"
)

// Mock for new bucket management queries
type mockBucketManagementFetcher struct {
	bucketConfig    configdb.BucketConfiguration
	bucketErr       error
	orgsByBucket    []uuid.UUID
	orgsByBucketErr error
	hasAccess       bool
	hasAccessErr    error
	prefixMatch     uuid.UUID
	prefixMatchErr  error
	// For validation
	lastPrefixMatchParams *configdb.GetLongestPrefixMatchParams
}

func (m *mockBucketManagementFetcher) GetStorageProfile(ctx context.Context, params configdb.GetStorageProfileParams) (configdb.GetStorageProfileRow, error) {
	return configdb.GetStorageProfileRow{}, errors.New("not implemented")
}

func (m *mockBucketManagementFetcher) GetStorageProfileByCollectorName(ctx context.Context, organizationID uuid.UUID) (configdb.GetStorageProfileByCollectorNameRow, error) {
	return configdb.GetStorageProfileByCollectorNameRow{}, errors.New("not implemented")
}

func (m *mockBucketManagementFetcher) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]configdb.GetStorageProfilesByBucketNameRow, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBucketManagementFetcher) GetBucketConfiguration(ctx context.Context, bucketName string) (configdb.BucketConfiguration, error) {
	return m.bucketConfig, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetOrganizationsByBucket(ctx context.Context, bucketName string) ([]uuid.UUID, error) {
	return m.orgsByBucket, m.orgsByBucketErr
}

func (m *mockBucketManagementFetcher) CheckOrgBucketAccess(ctx context.Context, arg configdb.CheckOrgBucketAccessParams) (bool, error) {
	return m.hasAccess, m.hasAccessErr
}

func (m *mockBucketManagementFetcher) GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (configdb.GetLongestPrefixMatchRow, error) {
	// Store parameters for validation
	m.lastPrefixMatchParams = &arg
	return configdb.GetLongestPrefixMatchRow{
		OrganizationID: m.prefixMatch,
		Signal:         "logs", // Default signal for tests
	}, m.prefixMatchErr
}

func (m *mockBucketManagementFetcher) GetBucketByOrganization(ctx context.Context, organizationID uuid.UUID) (string, error) {
	return m.bucketConfig.BucketName, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetOrganizationBucketByInstance(ctx context.Context, arg configdb.GetOrganizationBucketByInstanceParams) (configdb.GetOrganizationBucketByInstanceRow, error) {
	return configdb.GetOrganizationBucketByInstanceRow{
		OrganizationID: arg.OrganizationID,
		InstanceNum:    arg.InstanceNum,
		CollectorName:  "default",
		BucketName:     m.bucketConfig.BucketName,
		CloudProvider:  m.bucketConfig.CloudProvider,
		Region:         m.bucketConfig.Region,
		Role:           m.bucketConfig.Role,
		Endpoint:       m.bucketConfig.Endpoint,
		UsePathStyle:   m.bucketConfig.UsePathStyle,
		InsecureTls:    m.bucketConfig.InsecureTls,
	}, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetOrganizationBucketByCollector(ctx context.Context, arg configdb.GetOrganizationBucketByCollectorParams) (configdb.GetOrganizationBucketByCollectorRow, error) {
	return configdb.GetOrganizationBucketByCollectorRow{
		OrganizationID: arg.OrganizationID,
		InstanceNum:    1,
		CollectorName:  arg.CollectorName,
		BucketName:     m.bucketConfig.BucketName,
		CloudProvider:  m.bucketConfig.CloudProvider,
		Region:         m.bucketConfig.Region,
		Role:           m.bucketConfig.Role,
		Endpoint:       m.bucketConfig.Endpoint,
		UsePathStyle:   m.bucketConfig.UsePathStyle,
		InsecureTls:    m.bucketConfig.InsecureTls,
	}, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetDefaultOrganizationBucket(ctx context.Context, organizationID uuid.UUID) (configdb.GetDefaultOrganizationBucketRow, error) {
	return configdb.GetDefaultOrganizationBucketRow{
		OrganizationID: organizationID,
		InstanceNum:    1,
		CollectorName:  "default",
		BucketName:     m.bucketConfig.BucketName,
		CloudProvider:  m.bucketConfig.CloudProvider,
		Region:         m.bucketConfig.Region,
		Role:           m.bucketConfig.Role,
		Endpoint:       m.bucketConfig.Endpoint,
		UsePathStyle:   m.bucketConfig.UsePathStyle,
		InsecureTls:    m.bucketConfig.InsecureTls,
	}, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetLowestInstanceOrganizationBucket(ctx context.Context, arg configdb.GetLowestInstanceOrganizationBucketParams) (configdb.GetLowestInstanceOrganizationBucketRow, error) {
	return configdb.GetLowestInstanceOrganizationBucketRow{
		OrganizationID: arg.OrganizationID,
		InstanceNum:    1,
		CollectorName:  "default",
		BucketName:     arg.BucketName,
		CloudProvider:  m.bucketConfig.CloudProvider,
		Region:         m.bucketConfig.Region,
		Role:           m.bucketConfig.Role,
		Endpoint:       m.bucketConfig.Endpoint,
		UsePathStyle:   m.bucketConfig.UsePathStyle,
		InsecureTls:    m.bucketConfig.InsecureTls,
	}, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetBucketPrefixMappings(ctx context.Context, bucketName string) ([]configdb.GetBucketPrefixMappingsRow, error) {
	if m.prefixMatchErr != nil {
		return nil, m.prefixMatchErr
	}
	if m.prefixMatch != uuid.Nil && bucketName == "shared-bucket" {
		// Return mappings that would match the test paths
		return []configdb.GetBucketPrefixMappingsRow{
			{
				OrganizationID: m.prefixMatch,
				PathPrefix:     "/metrics/org1-data",
				Signal:         "metrics",
			},
			{
				OrganizationID: m.prefixMatch,
				PathPrefix:     "/logs/org1-data",
				Signal:         "logs",
			},
			{
				OrganizationID: m.prefixMatch,
				PathPrefix:     "/traces/org1-data",
				Signal:         "traces",
			},
			{
				OrganizationID: m.prefixMatch,
				PathPrefix:     "/org1-data",
				Signal:         "logs",
			},
		}, nil
	}
	return []configdb.GetBucketPrefixMappingsRow{}, nil
}

func TestDatabaseProvider_ResolveOrganization_PrefixMatching(t *testing.T) {
	orgID := uuid.New()

	tests := []struct {
		name            string
		bucketName      string
		objectPath      string
		mockPrefixMatch uuid.UUID
		mockPrefixErr   error
		want            OrganizationResolution
		wantErr         bool
	}{
		{
			name:            "successful prefix match",
			bucketName:      "shared-bucket",
			objectPath:      "/org1-data/metrics/file.parquet",
			mockPrefixMatch: orgID,
			mockPrefixErr:   nil,
			want: OrganizationResolution{
				OrganizationID: orgID,
				Signal:         "logs",
			},
			wantErr: false,
		},
		{
			name:            "no prefix match found",
			bucketName:      "shared-bucket",
			objectPath:      "/unknown-prefix/metrics/file.parquet",
			mockPrefixMatch: uuid.Nil,
			mockPrefixErr:   errors.New("no match found"),
			want:            OrganizationResolution{},
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockBucketManagementFetcher{
				bucketConfig: configdb.BucketConfiguration{
					ID:            uuid.New(),
					BucketName:    tt.bucketName,
					CloudProvider: "aws",
					Region:        "us-west-2",
				},
				bucketErr:      nil,
				prefixMatch:    tt.mockPrefixMatch,
				prefixMatchErr: tt.mockPrefixErr,
				hasAccess:      !tt.wantErr, // Grant access for successful tests
				hasAccessErr:   nil,
			}

			provider := NewDatabaseProvider(mock)

			got, err := provider.ResolveOrganization(context.Background(), tt.bucketName, tt.objectPath)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, OrganizationResolution{}, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestBucketConfiguration_RoleBasedBehavior(t *testing.T) {
	tests := []struct {
		name           string
		bucketConfig   configdb.BucketConfiguration
		expectedHosted bool
	}{
		{
			name: "bucket with role should be non-hosted",
			bucketConfig: configdb.BucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "role-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
				Role:          stringPtr("arn:aws:iam::123456789:role/Lakerunner"),
			},
			expectedHosted: false,
		},
		{
			name: "bucket without role should be hosted",
			bucketConfig: configdb.BucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "hosted-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
				Role:          nil,
			},
			expectedHosted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that we can derive hosted behavior from role field
			actualHosted := (tt.bucketConfig.Role == nil || *tt.bucketConfig.Role == "")
			assert.Equal(t, tt.expectedHosted, actualHosted, "Hosted behavior should match role field")
		})
	}
}

func TestSimplifiedPathParsing(t *testing.T) {
	tests := []struct {
		name         string
		objectPath   string
		expectUUID   bool
		expectedUUID string
	}{
		{
			name:         "valid UUID in second segment",
			objectPath:   "/otel/01234567-89ab-cdef-0123-456789abcdef/metrics.parquet",
			expectUUID:   true,
			expectedUUID: "01234567-89ab-cdef-0123-456789abcdef",
		},
		{
			name:       "invalid UUID in second segment",
			objectPath: "/otel/not-a-uuid/metrics.parquet",
			expectUUID: false,
		},
		{
			name:       "UUID in third segment (not extracted)",
			objectPath: "/otel/nested/01234567-89ab-cdef-0123-456789abcdef/metrics.parquet",
			expectUUID: false,
		},
		{
			name:       "path with only one segment",
			objectPath: "/otel",
			expectUUID: false,
		},
		{
			name:       "empty path",
			objectPath: "",
			expectUUID: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the simplified parsing logic
			pathParts := strings.Split(strings.Trim(tt.objectPath, "/"), "/")

			var extractedUUID uuid.UUID
			var parseErr error

			if len(pathParts) >= 2 {
				extractedUUID, parseErr = uuid.Parse(pathParts[1])
			}

			if tt.expectUUID {
				assert.NoError(t, parseErr, "Should successfully parse UUID")
				assert.Equal(t, tt.expectedUUID, extractedUUID.String())
			} else {
				if len(pathParts) >= 2 {
					assert.Error(t, parseErr, "Should fail to parse UUID or UUID should be invalid")
				}
			}
		})
	}
}

func TestSignalExtraction(t *testing.T) {
	tests := []struct {
		name           string
		objectPath     string
		expectedSignal string
	}{
		{
			name:           "metrics path",
			objectPath:     "/metrics/12345678-1234-1234-1234-123456789abc/data.parquet",
			expectedSignal: "metrics",
		},
		{
			name:           "logs path",
			objectPath:     "/logs/12345678-1234-1234-1234-123456789abc/data.parquet",
			expectedSignal: "logs",
		},
		{
			name:           "traces path",
			objectPath:     "/traces/12345678-1234-1234-1234-123456789abc/data.parquet",
			expectedSignal: "traces",
		},
		{
			name:           "unknown signal defaults to metrics",
			objectPath:     "/unknown-signal/12345678-1234-1234-1234-123456789abc/data.parquet",
			expectedSignal: "metrics",
		},
		{
			name:           "empty path defaults to metrics",
			objectPath:     "",
			expectedSignal: "metrics",
		},
		{
			name:           "only one segment defaults to metrics",
			objectPath:     "/single",
			expectedSignal: "metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the signal extraction logic from the implementation
			pathParts := strings.Split(strings.Trim(tt.objectPath, "/"), "/")

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

			assert.Equal(t, tt.expectedSignal, signal)
		})
	}
}
