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
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	orgID       = uuid.New()
	yamlContent = fmt.Sprintf(`
- organization_id: %s
  cloud_provider: "aws"
  region: "us-west-2"
  role: "role-arn"
  bucket: "my-bucket"
`, orgID.String())
)

func Test_newFileProviderFromContents_Success(t *testing.T) {
	provider, err := newFileProviderFromContents("test.yaml", []byte(yamlContent))
	require.NoError(t, err)
	require.NotNil(t, provider)

	fp, ok := provider.(*fileProvider)
	require.True(t, ok)
	require.Len(t, fp.profiles, 1)
	profile := fp.profiles[0]
	require.Equal(t, orgID, profile.OrganizationID)
	// Note: InstanceNum and CollectorName no longer exist in StorageProfile
	require.Equal(t, "aws", profile.CloudProvider)
	require.Equal(t, "us-west-2", profile.Region)
	require.Equal(t, "role-arn", profile.Role)
	require.Equal(t, "my-bucket", profile.Bucket)

	// Note: Get method was removed, using GetStorageProfileForOrganization instead
	item, err := provider.GetStorageProfileForOrganization(context.TODO(), orgID)
	require.NoError(t, err)
	require.Equal(t, profile.OrganizationID, item.OrganizationID)
}

func Test_newFileProviderFromContents_UnmarshalError(t *testing.T) {
	invalidYAML := []byte("not: [valid: yaml")
	provider, err := newFileProviderFromContents("bad.yaml", invalidYAML)
	require.Error(t, err)
	require.Nil(t, provider)
	require.Contains(t, err.Error(), "failed to unmarshal v1 storage profiles from file bad.yaml")
}

func Test_newFileProviderFromContents_V2YAML(t *testing.T) {
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	v2YAMLContent := fmt.Sprintf(`version: 2
buckets:
  - name: "shared-bucket"
    cloud_provider: "aws"
    region: "us-west-2"
    role: "arn:aws:iam::123456789012:role/LakeRunner"
    organizations:
      - "%s"
      - "%s"
    prefix_mappings:
      - organization_id: "%s"
        prefix: "/metrics/org1-data/"
        signal: "metrics"
      - organization_id: "%s"
        prefix: "/metrics/org2-data/"
        signal: "metrics"
  - name: "dedicated-bucket"
    cloud_provider: "gcp"
    region: "us-central1"
    organizations:
      - "%s"
`, orgID1.String(), orgID2.String(), orgID1.String(), orgID2.String(), orgID1.String())

	provider, err := newFileProviderFromContents("test-v2.yaml", []byte(v2YAMLContent))
	require.NoError(t, err)
	require.NotNil(t, provider)

	// Test ResolveOrganization with UUID in path
	orgID, err := provider.ResolveOrganization(context.Background(), "shared-bucket", "/logs-raw/"+orgID1.String()+"/some/file.json")
	require.NoError(t, err)
	require.Equal(t, orgID1, orgID)

	// Test ResolveOrganization with prefix matching (now requires signal)
	orgID, err = provider.ResolveOrganization(context.Background(), "shared-bucket", "/metrics/org1-data/some/file.json")
	require.NoError(t, err)
	require.Equal(t, orgID1, orgID)

	// Test single org bucket
	orgID, err = provider.ResolveOrganization(context.Background(), "dedicated-bucket", "any/path/file.json")
	require.NoError(t, err)
	require.Equal(t, orgID1, orgID)

	// Test GetStorageProfileForOrganization method with v2
	profile, err := provider.GetStorageProfileForOrganization(context.Background(), orgID1)
	require.NoError(t, err)
	require.Equal(t, orgID1, profile.OrganizationID)
	require.Equal(t, "shared-bucket", profile.Bucket) // Returns first bucket
}

func Test_NewFileProvider_env(t *testing.T) {
	os.Setenv("TEST_STORAGE_PROFILES", yamlContent)
	provider, err := NewFileProvider("env:TEST_STORAGE_PROFILES")
	require.NoError(t, err)
	require.NotNil(t, provider)
}

// Test removed - GetByCollectorName method was removed

func TestFileProvider_GetStorageProfilesByBucketName(t *testing.T) {
	multiYamlContent := fmt.Sprintf(`
- organization_id: %s
  cloud_provider: "aws"
  region: "us-west-2"
  role: "role-arn"
  bucket: "shared-bucket"
- organization_id: %s
  cloud_provider: "gcp"
  region: "europe-west1"
  bucket: "unique-bucket"
- organization_id: %s
  cloud_provider: "aws"
  region: "us-east-1"
  bucket: "shared-bucket"
`, orgID.String(), orgID.String(), uuid.New().String())

	provider, err := newFileProviderFromContents("test.yaml", []byte(multiYamlContent))
	require.NoError(t, err)

	tests := []struct {
		name       string
		bucketName string
		wantCount  int
		wantBucket string
	}{
		{
			name:       "shared bucket with 2 profiles",
			bucketName: "shared-bucket",
			wantCount:  2,
			wantBucket: "shared-bucket",
		},
		{
			name:       "unique bucket with 1 profile",
			bucketName: "unique-bucket",
			wantCount:  1,
			wantBucket: "unique-bucket",
		},
		{
			name:       "nonexistent bucket",
			bucketName: "nonexistent",
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := provider.GetStorageProfilesByBucketName(context.Background(), tt.bucketName)
			require.NoError(t, err)
			require.Len(t, got, tt.wantCount)

			for _, profile := range got {
				require.Equal(t, tt.wantBucket, profile.Bucket)
			}
		})
	}
}

func TestNewFileProvider_ErrorCases(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		setup    func()
		wantErr  string
	}{
		{
			name:     "file not found",
			filename: "/nonexistent/file.yaml",
			wantErr:  "failed to read storage profiles from file",
		},
		{
			name:     "env variable not set",
			filename: "env:UNSET_VAR",
			wantErr:  "environment variable UNSET_VAR is not set",
		},
		{
			name:     "env variable empty",
			filename: "env:EMPTY_VAR",
			setup: func() {
				os.Setenv("EMPTY_VAR", "")
			},
			wantErr: "environment variable EMPTY_VAR is not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			provider, err := NewFileProvider(tt.filename)
			require.Error(t, err)
			require.Nil(t, provider)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
