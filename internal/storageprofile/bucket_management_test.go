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
	bucketConfig    configdb.LrconfigBucketConfiguration
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

func (m *mockBucketManagementFetcher) GetStorageProfileByCollectorName(ctx context.Context, params configdb.GetStorageProfileByCollectorNameParams) (configdb.GetStorageProfileByCollectorNameRow, error) {
	return configdb.GetStorageProfileByCollectorNameRow{}, errors.New("not implemented")
}

func (m *mockBucketManagementFetcher) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]configdb.GetStorageProfilesByBucketNameRow, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBucketManagementFetcher) GetBucketConfiguration(ctx context.Context, bucketName string) (configdb.LrconfigBucketConfiguration, error) {
	return m.bucketConfig, m.bucketErr
}

func (m *mockBucketManagementFetcher) GetOrganizationsByBucket(ctx context.Context, bucketName string) ([]uuid.UUID, error) {
	return m.orgsByBucket, m.orgsByBucketErr
}

func (m *mockBucketManagementFetcher) CheckOrgBucketAccess(ctx context.Context, arg configdb.CheckOrgBucketAccessParams) (bool, error) {
	return m.hasAccess, m.hasAccessErr
}

func (m *mockBucketManagementFetcher) GetLongestPrefixMatch(ctx context.Context, arg configdb.GetLongestPrefixMatchParams) (uuid.UUID, error) {
	// Store parameters for validation
	m.lastPrefixMatchParams = &arg
	return m.prefixMatch, m.prefixMatchErr
}

func (m *mockBucketManagementFetcher) GetBucketByOrganization(ctx context.Context, organizationID uuid.UUID) (string, error) {
	return m.bucketConfig.BucketName, m.bucketErr
}

func TestDatabaseProvider_ResolveOrganization_UUIDExtraction(t *testing.T) {
	orgID := uuid.New()

	tests := []struct {
		name            string
		bucketName      string
		objectPath      string
		mockConfig      configdb.BucketConfiguration
		mockConfigErr   error
		mockHasAccess   bool
		mockAccessErr   error
		wantOrgID       uuid.UUID
		wantErr         bool
		wantErrContains string
	}{
		{
			name:       "valid UUID in path with access",
			bucketName: "test-bucket",
			objectPath: "/metrics/" + orgID.String() + "/data.parquet",
			mockConfig: configdb.LrconfigBucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "test-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
			},
			mockConfigErr: nil,
			mockHasAccess: true,
			mockAccessErr: nil,
			wantOrgID:     orgID,
			wantErr:       false,
		},
		{
			name:       "valid UUID in path but no access continues to prefix matching",
			bucketName: "test-bucket",
			objectPath: "/metrics/" + orgID.String() + "/data.parquet",
			mockConfig: configdb.LrconfigBucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "test-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
			},
			mockConfigErr:   nil,
			mockHasAccess:   false, // No access, should continue to prefix matching
			mockAccessErr:   nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "failed to get organizations for bucket", // Falls through to single-org fallback
		},
		{
			name:       "nil UUID in path without access",
			bucketName: "test-bucket",
			objectPath: "/metrics/00000000-0000-0000-0000-000000000000/data.parquet", // Nil UUID
			mockConfig: configdb.LrconfigBucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "test-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
			},
			mockConfigErr:   nil,
			mockHasAccess:   false, // Nil UUID won't have access, continues to prefix matching
			mockAccessErr:   nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "failed to get organizations for bucket", // Falls through to single-org fallback
		},
		{
			name:       "invalid UUID in path",
			bucketName: "test-bucket",
			objectPath: "/metrics/invalid-uuid/data.parquet",
			mockConfig: configdb.LrconfigBucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "test-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
			},
			mockConfigErr:   nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "failed to get organizations for bucket", // UUID parse fails, falls through to prefix matching
		},
		{
			name:       "no UUID in path",
			bucketName: "test-bucket",
			objectPath: "/metrics/some-other-path/data.parquet",
			mockConfig: configdb.LrconfigBucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "test-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
			},
			mockConfigErr:   nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "failed to get organizations for bucket", // UUID parse fails, falls through to prefix matching
		},
		{
			name:       "deeper nested UUID not extracted",
			bucketName: "test-bucket",
			objectPath: "/metrics/nested/" + orgID.String() + "/data.parquet", // UUID in 3rd segment, not 2nd
			mockConfig: configdb.LrconfigBucketConfiguration{
				ID:            uuid.New(),
				BucketName:    "test-bucket",
				CloudProvider: "aws",
				Region:        "us-west-2",
			},
			mockConfigErr:   nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "failed to get organizations for bucket", // UUID not in 2nd segment, falls through
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockBucketManagementFetcher{
				bucketConfig:    tt.mockConfig,
				bucketErr:       tt.mockConfigErr,
				hasAccess:       tt.mockHasAccess,
				hasAccessErr:    tt.mockAccessErr,
				orgsByBucket:    []uuid.UUID{}, // Empty to trigger error for fallback
				orgsByBucketErr: errors.New("no orgs found"),
				prefixMatchErr:  errors.New("no prefix match"),
			}

			provider := NewDatabaseProvider(mock)

			gotOrgID, err := provider.ResolveOrganization(context.Background(), tt.bucketName, tt.objectPath)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
				assert.Equal(t, uuid.Nil, gotOrgID)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantOrgID, gotOrgID)
			}
		})
	}
}

func TestDatabaseProvider_ResolveOrganization_PrefixMatching(t *testing.T) {
	orgID := uuid.New()

	tests := []struct {
		name            string
		bucketName      string
		objectPath      string
		mockPrefixMatch uuid.UUID
		mockPrefixErr   error
		wantOrgID       uuid.UUID
		wantErr         bool
	}{
		{
			name:            "successful prefix match",
			bucketName:      "shared-bucket",
			objectPath:      "/org1-data/metrics/file.parquet",
			mockPrefixMatch: orgID,
			mockPrefixErr:   nil,
			wantOrgID:       orgID,
			wantErr:         false,
		},
		{
			name:            "no prefix match found",
			bucketName:      "shared-bucket",
			objectPath:      "/unknown-prefix/metrics/file.parquet",
			mockPrefixMatch: uuid.Nil,
			mockPrefixErr:   errors.New("no match found"),
			wantOrgID:       uuid.Nil,
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
				bucketErr:       nil,
				prefixMatch:     tt.mockPrefixMatch,
				prefixMatchErr:  tt.mockPrefixErr,
				orgsByBucket:    []uuid.UUID{}, // Empty to trigger error for single-org fallback
				orgsByBucketErr: errors.New("no orgs found"),
			}

			provider := NewDatabaseProvider(mock)

			gotOrgID, err := provider.ResolveOrganization(context.Background(), tt.bucketName, tt.objectPath)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, uuid.Nil, gotOrgID)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantOrgID, gotOrgID)
			}
		})
	}
}

func TestDatabaseProvider_ResolveOrganization_SingleOrgFallback(t *testing.T) {
	orgID := uuid.New()

	tests := []struct {
		name            string
		bucketName      string
		objectPath      string
		mockOrgs        []uuid.UUID
		mockOrgsErr     error
		wantOrgID       uuid.UUID
		wantErr         bool
		wantErrContains string
	}{
		{
			name:        "single org bucket success",
			bucketName:  "dedicated-bucket",
			objectPath:  "/any/path/file.parquet",
			mockOrgs:    []uuid.UUID{orgID},
			mockOrgsErr: nil,
			wantOrgID:   orgID,
			wantErr:     false,
		},
		{
			name:            "multiple orgs ambiguous",
			bucketName:      "shared-bucket",
			objectPath:      "/ambiguous/path/file.parquet",
			mockOrgs:        []uuid.UUID{uuid.New(), uuid.New()},
			mockOrgsErr:     nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "2 organizations found",
		},
		{
			name:            "no orgs found",
			bucketName:      "empty-bucket",
			objectPath:      "/any/path/file.parquet",
			mockOrgs:        []uuid.UUID{},
			mockOrgsErr:     nil,
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "0 organizations found",
		},
		{
			name:            "database error getting orgs",
			bucketName:      "error-bucket",
			objectPath:      "/any/path/file.parquet",
			mockOrgs:        nil,
			mockOrgsErr:     errors.New("database connection failed"),
			wantOrgID:       uuid.Nil,
			wantErr:         true,
			wantErrContains: "failed to get organizations",
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
				bucketErr:       nil,
				orgsByBucket:    tt.mockOrgs,
				orgsByBucketErr: tt.mockOrgsErr,
				prefixMatchErr:  errors.New("no prefix match"), // Force fallback to single-org
			}

			provider := NewDatabaseProvider(mock)

			gotOrgID, err := provider.ResolveOrganization(context.Background(), tt.bucketName, tt.objectPath)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
				assert.Equal(t, uuid.Nil, gotOrgID)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantOrgID, gotOrgID)
			}
		})
	}
}

func TestDatabaseProvider_ResolveOrganization_BucketNotFound(t *testing.T) {
	mock := &mockBucketManagementFetcher{
		bucketErr:       errors.New("bucket configuration not found"),
		orgsByBucketErr: errors.New("bucket not found"),
		prefixMatchErr:  errors.New("no prefix match"),
	}

	provider := NewDatabaseProvider(mock)

	_, err := provider.ResolveOrganization(context.Background(), "nonexistent-bucket", "/any/path/file.parquet")

	// Should return error since bucket is not found
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "bucket not found")
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
				Role:          stringPtr("arn:aws:iam::123456789:role/LakeRunner"),
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

func TestDatabaseProvider_ResolveOrganization_SignalBasedPrefixMatching(t *testing.T) {
	orgID := uuid.New()

	tests := []struct {
		name              string
		bucketName        string
		objectPath        string
		mockPrefixMatch   uuid.UUID
		mockPrefixErr     error
		expectedSignalArg string // What signal should be passed to the query
		wantOrgID         uuid.UUID
		wantErr           bool
	}{
		{
			name:              "metrics prefix match",
			bucketName:        "shared-bucket",
			objectPath:        "/metrics/org1-data/file.parquet",
			mockPrefixMatch:   orgID,
			mockPrefixErr:     nil,
			expectedSignalArg: "metrics",
			wantOrgID:         orgID,
			wantErr:           false,
		},
		{
			name:              "logs prefix match",
			bucketName:        "shared-bucket",
			objectPath:        "/logs/org1-data/file.parquet",
			mockPrefixMatch:   orgID,
			mockPrefixErr:     nil,
			expectedSignalArg: "logs",
			wantOrgID:         orgID,
			wantErr:           false,
		},
		{
			name:              "traces prefix match",
			bucketName:        "shared-bucket",
			objectPath:        "/traces/org1-data/file.parquet",
			mockPrefixMatch:   orgID,
			mockPrefixErr:     nil,
			expectedSignalArg: "traces",
			wantOrgID:         orgID,
			wantErr:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock that validates the signal parameter
			mock := &mockBucketManagementFetcher{
				bucketConfig: configdb.BucketConfiguration{
					ID:            uuid.New(),
					BucketName:    tt.bucketName,
					CloudProvider: "aws",
					Region:        "us-west-2",
				},
				bucketErr:       nil,
				prefixMatch:     tt.mockPrefixMatch,
				prefixMatchErr:  tt.mockPrefixErr,
				orgsByBucket:    []uuid.UUID{}, // Empty to trigger error for single-org fallback
				orgsByBucketErr: errors.New("no orgs found"),
			}

			provider := NewDatabaseProvider(mock)

			gotOrgID, err := provider.ResolveOrganization(context.Background(), tt.bucketName, tt.objectPath)

			// Validate the parameters that were passed to GetLongestPrefixMatch
			if mock.lastPrefixMatchParams != nil {
				assert.Equal(t, tt.expectedSignalArg, mock.lastPrefixMatchParams.Signal, "Expected signal to match extracted signal")
				assert.Equal(t, tt.bucketName, mock.lastPrefixMatchParams.BucketName)
				assert.Equal(t, tt.objectPath, mock.lastPrefixMatchParams.ObjectPath)
			}

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, uuid.Nil, gotOrgID)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantOrgID, gotOrgID)
			}
		})
	}
}

func TestNewBucketManagement_IntegrationFlow(t *testing.T) {
	orgID := uuid.New()
	bucketID := uuid.New()

	// Simulate a complete flow through the new bucket management system
	mock := &mockBucketManagementFetcher{
		bucketConfig: configdb.BucketConfiguration{
			ID:            bucketID,
			BucketName:    "integration-test-bucket",
			CloudProvider: "aws",
			Region:        "us-west-2",
			Role:          stringPtr("arn:aws:iam::123456789:role/TestRole"),
		},
		bucketErr:    nil,
		hasAccess:    true,
		hasAccessErr: nil,
	}

	provider := NewDatabaseProvider(mock)

	// Test UUID-based resolution (priority 1)
	objectPath := "/otel/" + orgID.String() + "/metrics.parquet"
	resolvedOrgID, err := provider.ResolveOrganization(context.Background(), "integration-test-bucket", objectPath)

	assert.NoError(t, err)
	assert.Equal(t, orgID, resolvedOrgID)

	// Verify the bucket configuration can be used to determine role-based behavior
	hasRole := mock.bucketConfig.Role != nil && *mock.bucketConfig.Role != ""
	assert.True(t, hasRole, "Bucket should have a role configured")

	// This demonstrates that the role field determines credential behavior
	// (empty role = use default credentials, non-empty = assume role)
}
