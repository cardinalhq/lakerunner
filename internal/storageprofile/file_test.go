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
  instance_num: 1
  collector_name: "ext-123"
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
	require.Equal(t, int16(1), profile.InstanceNum)
	require.Equal(t, "ext-123", profile.CollectorName)
	require.Equal(t, "aws", profile.CloudProvider)
	require.Equal(t, "us-west-2", profile.Region)
	require.Equal(t, "role-arn", profile.Role)
	require.Equal(t, "my-bucket", profile.Bucket)

	item, err := provider.Get(context.TODO(), orgID, 1)
	require.NoError(t, err)
	require.Equal(t, profile, item)

	_, err = provider.Get(context.TODO(), orgID, 2)
	require.Error(t, err)
}

func Test_newFileProviderFromContents_UnmarshalError(t *testing.T) {
	invalidYAML := []byte("not: [valid: yaml")
	provider, err := newFileProviderFromContents("bad.yaml", invalidYAML)
	require.Error(t, err)
	require.Nil(t, provider)
	require.Contains(t, err.Error(), "failed to unmarshal storage profiles from file bad.yaml")
}

func Test_NewFileProvider_env(t *testing.T) {
	os.Setenv("TEST_STORAGE_PROFILES", yamlContent)
	provider, err := NewFileProvider("env:TEST_STORAGE_PROFILES")
	require.NoError(t, err)
	require.NotNil(t, provider)
}

func TestFileProvider_GetByCollectorName(t *testing.T) {
	multiYamlContent := fmt.Sprintf(`
- organization_id: %s
  instance_num: 1
  collector_name: "collector-1"
  cloud_provider: "aws"
  region: "us-west-2"
  role: "role-arn"
  bucket: "bucket-1"
- organization_id: %s
  instance_num: 2
  collector_name: "collector-2"
  cloud_provider: "gcp"
  region: "europe-west1"
  bucket: "bucket-2"
- organization_id: %s
  instance_num: 3
  collector_name: "collector-3"
  cloud_provider: "aws"
  region: "us-east-1"
  bucket: "bucket-3"
`, orgID.String(), orgID.String(), uuid.New().String())

	provider, err := newFileProviderFromContents("test.yaml", []byte(multiYamlContent))
	require.NoError(t, err)

	tests := []struct {
		name           string
		organizationID uuid.UUID
		collectorName  string
		want           StorageProfile
		wantErr        bool
	}{
		{
			name:           "found collector-1",
			organizationID: orgID,
			collectorName:  "collector-1",
			want: StorageProfile{
				OrganizationID: orgID,
				InstanceNum:    1,
				CollectorName:  "collector-1",
				CloudProvider:  "aws",
				Region:         "us-west-2",
				Role:           "role-arn",
				Bucket:         "bucket-1",
			},
			wantErr: false,
		},
		{
			name:           "found collector-2",
			organizationID: orgID,
			collectorName:  "collector-2",
			want: StorageProfile{
				OrganizationID: orgID,
				InstanceNum:    2,
				CollectorName:  "collector-2",
				CloudProvider:  "gcp",
				Region:         "europe-west1",
				Bucket:         "bucket-2",
				Hosted:         true, // no role, so hosted=true
			},
			wantErr: false,
		},
		{
			name:           "collector not found",
			organizationID: orgID,
			collectorName:  "nonexistent",
			want:           StorageProfile{},
			wantErr:        true,
		},
		{
			name:           "wrong organization",
			organizationID: uuid.New(),
			collectorName:  "collector-1",
			want:           StorageProfile{},
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := provider.GetByCollectorName(context.Background(), tt.organizationID, tt.collectorName)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "storage profile not found")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFileProvider_GetStorageProfilesByBucketName(t *testing.T) {
	multiYamlContent := fmt.Sprintf(`
- organization_id: %s
  instance_num: 1
  collector_name: "collector-1"
  cloud_provider: "aws"
  region: "us-west-2"
  role: "role-arn"
  bucket: "shared-bucket"
- organization_id: %s
  instance_num: 2
  collector_name: "collector-2"
  cloud_provider: "gcp"
  region: "europe-west1"
  bucket: "unique-bucket"
- organization_id: %s
  instance_num: 3
  collector_name: "collector-3"
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

func TestFileProvider_HostedFlag(t *testing.T) {
	yamlContentNoRole := fmt.Sprintf(`
- organization_id: %s
  instance_num: 1
  collector_name: "collector-no-role"
  cloud_provider: "aws"
  region: "us-west-2"
  bucket: "bucket-1"
- organization_id: %s
  instance_num: 2
  collector_name: "collector-with-role"
  cloud_provider: "aws"
  region: "us-west-2"
  role: "some-role"
  bucket: "bucket-2"
`, orgID.String(), orgID.String())

	provider, err := newFileProviderFromContents("test.yaml", []byte(yamlContentNoRole))
	require.NoError(t, err)

	// Profile without role should have hosted=true
	profile1, err := provider.Get(context.Background(), orgID, 1)
	require.NoError(t, err)
	require.True(t, profile1.Hosted)
	require.Empty(t, profile1.Role)

	// Profile with role should have hosted=false
	profile2, err := provider.Get(context.Background(), orgID, 2)
	require.NoError(t, err)
	require.False(t, profile2.Hosted)
	require.Equal(t, "some-role", profile2.Role)
}
