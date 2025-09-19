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

//go:build integration
// +build integration

package configdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrganizationAPIKeyOperations(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := ConnectToConfigDB(ctx)
	require.NoError(t, err)
	defer pool.Close()

	store := NewStore(pool)
	orgID := uuid.New()

	t.Run("CreateOrganizationAPIKeyWithMapping", func(t *testing.T) {
		// Create an API key
		params := CreateOrganizationAPIKeyParams{
			KeyHash:     "test-hash-" + uuid.New().String(),
			Name:        "Test API Key",
			Description: stringPtr("Test description"),
		}

		apiKey, err := store.CreateOrganizationAPIKeyWithMapping(ctx, params, orgID)
		require.NoError(t, err)
		assert.NotNil(t, apiKey)
		assert.Equal(t, params.Name, apiKey.Name)
		assert.Equal(t, params.KeyHash, apiKey.KeyHash)

		// Verify the mapping was created
		fetchedKey, err := store.GetOrganizationAPIKeyByID(ctx, apiKey.ID)
		require.NoError(t, err)
		assert.Equal(t, orgID, fetchedKey.OrganizationID)

		// Clean up
		err = store.DeleteOrganizationAPIKeyWithMappings(ctx, apiKey.ID)
		require.NoError(t, err)
	})

	t.Run("DeleteOrganizationAPIKeyWithMappings", func(t *testing.T) {
		// Create an API key first
		params := CreateOrganizationAPIKeyParams{
			KeyHash:     "test-hash-delete-" + uuid.New().String(),
			Name:        "API Key to Delete",
			Description: stringPtr("Will be deleted"),
		}

		apiKey, err := store.CreateOrganizationAPIKeyWithMapping(ctx, params, orgID)
		require.NoError(t, err)

		// Delete it
		err = store.DeleteOrganizationAPIKeyWithMappings(ctx, apiKey.ID)
		require.NoError(t, err)

		// Verify it's gone
		_, err = store.GetOrganizationAPIKeyByID(ctx, apiKey.ID)
		assert.Error(t, err)
	})
}

func TestOrganizationBucketOperations(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := ConnectToConfigDB(ctx)
	require.NoError(t, err)
	defer pool.Close()

	store := NewStore(pool)
	orgID := uuid.New()
	bucketName := "test-bucket-" + uuid.New().String()

	t.Run("AddOrganizationBucket", func(t *testing.T) {
		// First create a bucket configuration
		_, err := store.CreateBucketConfiguration(ctx, CreateBucketConfigurationParams{
			BucketName:    bucketName,
			CloudProvider: "aws",
			Region:        "us-west-2",
		})
		require.NoError(t, err)

		// Add the bucket to the organization
		err = store.AddOrganizationBucket(ctx, orgID, bucketName, 1, "test-collector")
		require.NoError(t, err)

		// Verify the association
		buckets, err := store.ListOrganizationBucketsByOrg(ctx, orgID)
		require.NoError(t, err)

		found := false
		for _, bucket := range buckets {
			if bucket.BucketName == bucketName {
				found = true
				assert.Equal(t, int16(1), bucket.InstanceNum)
				assert.Equal(t, "test-collector", bucket.CollectorName)
				break
			}
		}
		assert.True(t, found, "Bucket should be associated with organization")

		// Clean up
		err = store.DeleteOrganizationBucket(ctx, DeleteOrganizationBucketParams{
			OrganizationID: orgID,
			BucketName:     bucketName,
			InstanceNum:    1,
			CollectorName:  "test-collector",
		})
		require.NoError(t, err)

		err = store.DeleteBucketConfiguration(ctx, bucketName)
		require.NoError(t, err)
	})
}

func TestBucketPrefixMappingOperations(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := ConnectToConfigDB(ctx)
	require.NoError(t, err)
	defer pool.Close()

	store := NewStore(pool)
	orgID := uuid.New()
	bucketName := "test-prefix-bucket-" + uuid.New().String()

	t.Run("CreateBucketPrefixMappingForOrg", func(t *testing.T) {
		// First create a bucket configuration
		_, err := store.CreateBucketConfiguration(ctx, CreateBucketConfigurationParams{
			BucketName:    bucketName,
			CloudProvider: "gcp",
			Region:        "us-central1",
		})
		require.NoError(t, err)

		// Create a prefix mapping
		mapping, err := store.CreateBucketPrefixMappingForOrg(ctx, bucketName, orgID, "/foo", "logs")
		require.NoError(t, err)
		assert.NotNil(t, mapping)
		assert.Equal(t, "/foo", mapping.PathPrefix)
		assert.Equal(t, "logs", mapping.Signal)

		// Verify the mapping
		mappings, err := store.ListBucketPrefixMappings(ctx)
		require.NoError(t, err)

		found := false
		for _, m := range mappings {
			if m.ID == mapping.ID {
				found = true
				assert.Equal(t, bucketName, m.BucketName)
				assert.Equal(t, orgID, m.OrganizationID)
				assert.Equal(t, "/foo", m.PathPrefix)
				assert.Equal(t, "logs", m.Signal)
				break
			}
		}
		assert.True(t, found, "Prefix mapping should exist")

		// Clean up
		err = store.DeleteBucketPrefixMapping(ctx, mapping.ID)
		require.NoError(t, err)

		err = store.DeleteBucketConfiguration(ctx, bucketName)
		require.NoError(t, err)
	})
}

func TestTransactionRollback(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := ConnectToConfigDB(ctx)
	require.NoError(t, err)
	defer pool.Close()

	store := NewStore(pool)

	t.Run("RollbackOnError", func(t *testing.T) {
		// Try to add a bucket to a non-existent bucket configuration
		// This should fail and rollback
		orgID := uuid.New()
		err := store.AddOrganizationBucket(ctx, orgID, "non-existent-bucket", 1, "test")
		assert.Error(t, err)

		// The transaction should have rolled back, so no partial state
		buckets, err := store.ListOrganizationBucketsByOrg(ctx, orgID)
		require.NoError(t, err)
		assert.Empty(t, buckets)
	})
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}
