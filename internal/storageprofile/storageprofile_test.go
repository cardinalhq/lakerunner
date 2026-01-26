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

package storageprofile

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/configdb"
)

func TestNewStorageProfileProvider(t *testing.T) {
	orgID := uuid.New()
	role := "test-role"
	mockProfile := configdb.GetStorageProfileRow{
		OrganizationID: orgID,
		InstanceNum:    9999,
		ExternalID:     "test-collector",
		CloudProvider:  "aws",
		Region:         "us-west-2",
		Bucket:         "test-bucket",
		Role:           &role,
	}

	// Create mock database provider
	mockFetcher := &mockConfigDBStoreageProfileFetcher{profile: mockProfile}

	// Test the dependency injection function
	provider := NewStorageProfileProvider(mockFetcher)
	assert.NotNil(t, provider)

	// Verify it returns a database provider
	providerType := fmt.Sprintf("%T", provider)
	assert.Equal(t, "*storageprofile.databaseProvider", providerType)

	// Test that it works correctly
	got, err := provider.GetStorageProfileForOrganization(context.Background(), orgID)
	assert.NoError(t, err)
	assert.Equal(t, orgID, got.OrganizationID)
	assert.Equal(t, "aws", got.CloudProvider)
	assert.Equal(t, "us-west-2", got.Region)
	assert.Equal(t, "test-bucket", got.Bucket)
	assert.Equal(t, "test-role", got.Role)
}
