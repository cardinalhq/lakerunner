// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package admin

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/adminproto"
)

func TestGenerateAPIKey(t *testing.T) {
	key, err := generateAPIKey()
	require.NoError(t, err)
	assert.Len(t, key, 64) // 32 bytes hex encoded = 64 chars
	assert.NotEmpty(t, key)

	// Keys should be unique
	key2, err := generateAPIKey()
	require.NoError(t, err)
	assert.NotEqual(t, key, key2)
}

func TestHashAPIKey(t *testing.T) {
	key := "test-api-key"
	hash := hashAPIKey(key)
	assert.Len(t, hash, 64) // SHA256 hex encoded = 64 chars

	// Same key should produce same hash
	hash2 := hashAPIKey(key)
	assert.Equal(t, hash, hash2)

	// Different key should produce different hash
	hash3 := hashAPIKey("different-key")
	assert.NotEqual(t, hash, hash3)
}

func TestListOrganizationAPIKeys_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.ListOrganizationAPIKeysRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.ListOrganizationAPIKeysRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.ListOrganizationAPIKeysRequest{
				OrganizationId: "not-a-uuid",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.ListOrganizationAPIKeys(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestCreateOrganizationAPIKey_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.CreateOrganizationAPIKeyRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.CreateOrganizationAPIKeyRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "missing name",
			req: &adminproto.CreateOrganizationAPIKeyRequest{
				OrganizationId: uuid.New().String(),
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "name is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.CreateOrganizationAPIKeyRequest{
				OrganizationId: "not-a-uuid",
				Name:           "test-key",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.CreateOrganizationAPIKey(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestDeleteOrganizationAPIKey_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.DeleteOrganizationAPIKeyRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing id",
			req:           &adminproto.DeleteOrganizationAPIKeyRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "id is required",
		},
		{
			name: "invalid id",
			req: &adminproto.DeleteOrganizationAPIKeyRequest{
				Id: "not-a-uuid",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.DeleteOrganizationAPIKey(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestListOrganizationBuckets_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.ListOrganizationBucketsRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.ListOrganizationBucketsRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.ListOrganizationBucketsRequest{
				OrganizationId: "not-a-uuid",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.ListOrganizationBuckets(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestAddOrganizationBucket_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.AddOrganizationBucketRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.AddOrganizationBucketRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "missing bucket name",
			req: &adminproto.AddOrganizationBucketRequest{
				OrganizationId: uuid.New().String(),
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "bucket_name is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.AddOrganizationBucketRequest{
				OrganizationId: "not-a-uuid",
				BucketName:     "test-bucket",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.AddOrganizationBucket(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestDeleteOrganizationBucket_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.DeleteOrganizationBucketRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.DeleteOrganizationBucketRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "missing bucket name",
			req: &adminproto.DeleteOrganizationBucketRequest{
				OrganizationId: uuid.New().String(),
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "bucket_name is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.DeleteOrganizationBucketRequest{
				OrganizationId: "not-a-uuid",
				BucketName:     "test-bucket",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.DeleteOrganizationBucket(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestCreateBucketConfiguration_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.CreateBucketConfigurationRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing bucket name",
			req:           &adminproto.CreateBucketConfigurationRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "bucket_name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.CreateBucketConfiguration(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestDeleteBucketConfiguration_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.DeleteBucketConfigurationRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing bucket name",
			req:           &adminproto.DeleteBucketConfigurationRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "bucket_name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.DeleteBucketConfiguration(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestCreateBucketPrefixMapping_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	orgID := uuid.New().String()

	tests := []struct {
		name          string
		req           *adminproto.CreateBucketPrefixMappingRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing bucket name",
			req:           &adminproto.CreateBucketPrefixMappingRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "bucket_name is required",
		},
		{
			name: "missing organization id",
			req: &adminproto.CreateBucketPrefixMappingRequest{
				BucketName: "test-bucket",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "missing path prefix",
			req: &adminproto.CreateBucketPrefixMappingRequest{
				BucketName:     "test-bucket",
				OrganizationId: orgID,
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "path_prefix is required",
		},
		{
			name: "missing signal",
			req: &adminproto.CreateBucketPrefixMappingRequest{
				BucketName:     "test-bucket",
				OrganizationId: orgID,
				PathPrefix:     "/test/",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "signal is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.CreateBucketPrefixMappingRequest{
				BucketName:     "test-bucket",
				OrganizationId: "not-a-uuid",
				PathPrefix:     "/test/",
				Signal:         "logs",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.CreateBucketPrefixMapping(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestDeleteBucketPrefixMapping_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.DeleteBucketPrefixMappingRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing id",
			req:           &adminproto.DeleteBucketPrefixMappingRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "id is required",
		},
		{
			name: "invalid id",
			req: &adminproto.DeleteBucketPrefixMappingRequest{
				Id: "not-a-uuid",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.DeleteBucketPrefixMapping(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestAPIKeyPreview(t *testing.T) {
	// Test that API key preview shows first 8 chars of hash
	key := "test-key"
	hash := hashAPIKey(key)
	preview := hash[:8]

	assert.Len(t, preview, 8)
	assert.True(t, strings.HasPrefix(hash, preview))
}
