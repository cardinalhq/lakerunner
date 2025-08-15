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
