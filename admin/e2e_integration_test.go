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

package admin

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/adminconfig"
)

func TestEndToEndOrganizationAPIKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start admin server
	service, client, cleanup := setupE2ETestServer(t, ctx)
	defer cleanup()

	// Create a test organization first
	orgResp, err := client.CreateOrganization(ctx, &adminproto.CreateOrganizationRequest{
		Name:    "Test Org for API Keys",
		Enabled: true,
	})
	require.NoError(t, err)
	orgID := orgResp.Organization.Id

	t.Run("CreateAPIKey", func(t *testing.T) {
		// Create API key
		resp, err := client.CreateOrganizationAPIKey(ctx, &adminproto.CreateOrganizationAPIKeyRequest{
			OrganizationId: orgID,
			Name:           "Test API Key",
			Description:    "Test description",
		})
		require.NoError(t, err)
		assert.NotNil(t, resp.ApiKey)
		assert.NotEmpty(t, resp.FullKey)
		assert.Equal(t, "Test API Key", resp.ApiKey.Name)
		assert.Equal(t, "Test description", resp.ApiKey.Description)

		// List API keys to verify creation
		listResp, err := client.ListOrganizationAPIKeys(ctx, &adminproto.ListOrganizationAPIKeysRequest{
			OrganizationId: orgID,
		})
		require.NoError(t, err)
		assert.Len(t, listResp.ApiKeys, 1)
		assert.Equal(t, resp.ApiKey.Id, listResp.ApiKeys[0].Id)

		// Delete API key
		_, err = client.DeleteOrganizationAPIKey(ctx, &adminproto.DeleteOrganizationAPIKeyRequest{
			Id: resp.ApiKey.Id,
		})
		require.NoError(t, err)

		// Verify deletion
		listResp, err = client.ListOrganizationAPIKeys(ctx, &adminproto.ListOrganizationAPIKeysRequest{
			OrganizationId: orgID,
		})
		require.NoError(t, err)
		assert.Empty(t, listResp.ApiKeys)
	})

	// Clean up organization
	cleanupTestOrg(t, service, orgID)
}

func TestEndToEndOrganizationBuckets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start admin server
	service, client, cleanup := setupE2ETestServer(t, ctx)
	defer cleanup()

	// Create a test organization
	orgResp, err := client.CreateOrganization(ctx, &adminproto.CreateOrganizationRequest{
		Name:    "Test Org for Buckets",
		Enabled: true,
	})
	require.NoError(t, err)
	orgID := orgResp.Organization.Id

	bucketName := "test-bucket-" + uuid.New().String()

	t.Run("BucketOperations", func(t *testing.T) {
		// Create bucket configuration
		bucketResp, err := client.CreateBucketConfiguration(ctx, &adminproto.CreateBucketConfigurationRequest{
			BucketName:    bucketName,
			CloudProvider: "aws",
			Region:        "us-west-2",
			UsePathStyle:  false,
			InsecureTls:   false,
		})
		require.NoError(t, err)
		assert.Equal(t, bucketName, bucketResp.Configuration.BucketName)

		// List bucket configurations
		listConfigResp, err := client.ListBucketConfigurations(ctx, &adminproto.ListBucketConfigurationsRequest{})
		require.NoError(t, err)

		found := false
		for _, cfg := range listConfigResp.Configurations {
			if cfg.BucketName == bucketName {
				found = true
				break
			}
		}
		assert.True(t, found, "Created bucket configuration should be in list")

		// Add bucket to organization
		addResp, err := client.AddOrganizationBucket(ctx, &adminproto.AddOrganizationBucketRequest{
			OrganizationId: orgID,
			BucketName:     bucketName,
			InstanceNum:    1,
			CollectorName:  "test-collector",
		})
		require.NoError(t, err)
		assert.Equal(t, bucketName, addResp.Bucket.BucketName)
		assert.Equal(t, int32(1), addResp.Bucket.InstanceNum)

		// List organization buckets
		listResp, err := client.ListOrganizationBuckets(ctx, &adminproto.ListOrganizationBucketsRequest{
			OrganizationId: orgID,
		})
		require.NoError(t, err)
		assert.Len(t, listResp.Buckets, 1)
		assert.Equal(t, bucketName, listResp.Buckets[0].BucketName)

		// Delete bucket from organization
		_, err = client.DeleteOrganizationBucket(ctx, &adminproto.DeleteOrganizationBucketRequest{
			OrganizationId: orgID,
			BucketName:     bucketName,
			InstanceNum:    1,
			CollectorName:  "test-collector",
		})
		require.NoError(t, err)

		// Verify deletion
		listResp, err = client.ListOrganizationBuckets(ctx, &adminproto.ListOrganizationBucketsRequest{
			OrganizationId: orgID,
		})
		require.NoError(t, err)
		assert.Empty(t, listResp.Buckets)

		// Delete bucket configuration
		_, err = client.DeleteBucketConfiguration(ctx, &adminproto.DeleteBucketConfigurationRequest{
			BucketName: bucketName,
		})
		require.NoError(t, err)
	})

	// Clean up organization
	cleanupTestOrg(t, service, orgID)
}

func TestEndToEndPrefixMappings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start admin server
	service, client, cleanup := setupE2ETestServer(t, ctx)
	defer cleanup()

	// Create a test organization
	orgResp, err := client.CreateOrganization(ctx, &adminproto.CreateOrganizationRequest{
		Name:    "Test Org for Prefix Mappings",
		Enabled: true,
	})
	require.NoError(t, err)
	orgID := orgResp.Organization.Id

	bucketName := "test-prefix-bucket-" + uuid.New().String()

	t.Run("PrefixMappingOperations", func(t *testing.T) {
		// Create bucket configuration first
		_, err := client.CreateBucketConfiguration(ctx, &adminproto.CreateBucketConfigurationRequest{
			BucketName:    bucketName,
			CloudProvider: "gcp",
			Region:        "us-central1",
		})
		require.NoError(t, err)

		// Create prefix mapping
		createResp, err := client.CreateBucketPrefixMapping(ctx, &adminproto.CreateBucketPrefixMappingRequest{
			BucketName:     bucketName,
			OrganizationId: orgID,
			PathPrefix:     "/foo",
			Signal:         "logs",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, createResp.Mapping.Id)
		assert.Equal(t, "/foo", createResp.Mapping.PathPrefix)
		assert.Equal(t, "logs", createResp.Mapping.Signal)

		mappingID := createResp.Mapping.Id

		// List prefix mappings
		listResp, err := client.ListBucketPrefixMappings(ctx, &adminproto.ListBucketPrefixMappingsRequest{
			OrganizationId: orgID,
		})
		require.NoError(t, err)

		found := false
		for _, mapping := range listResp.Mappings {
			if mapping.Id == mappingID {
				found = true
				assert.Equal(t, bucketName, mapping.BucketName)
				assert.Equal(t, orgID, mapping.OrganizationId)
				assert.Equal(t, "/foo", mapping.PathPrefix)
				assert.Equal(t, "logs", mapping.Signal)
				break
			}
		}
		assert.True(t, found, "Created prefix mapping should be in list")

		// Delete prefix mapping
		_, err = client.DeleteBucketPrefixMapping(ctx, &adminproto.DeleteBucketPrefixMappingRequest{
			Id: mappingID,
		})
		require.NoError(t, err)

		// Verify deletion
		listResp, err = client.ListBucketPrefixMappings(ctx, &adminproto.ListBucketPrefixMappingsRequest{
			OrganizationId: orgID,
		})
		require.NoError(t, err)

		found = false
		for _, mapping := range listResp.Mappings {
			if mapping.Id == mappingID {
				found = true
				break
			}
		}
		assert.False(t, found, "Deleted prefix mapping should not be in list")

		// Clean up bucket configuration
		_, err = client.DeleteBucketConfiguration(ctx, &adminproto.DeleteBucketConfigurationRequest{
			BucketName: bucketName,
		})
		require.NoError(t, err)
	})

	// Clean up organization
	cleanupTestOrg(t, service, orgID)
}

// Helper functions

func setupE2ETestServer(t *testing.T, ctx context.Context) (*Service, adminproto.AdminServiceClient, func()) {
	// Set up admin configuration for testing
	t.Setenv("ADMIN_API_KEY", "test-key")
	_, err := adminconfig.SetupAdminConfig()
	require.NoError(t, err)

	// Create service with a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	service := &Service{
		server:   grpc.NewServer(),
		listener: listener,
		addr:     addr,
		serverID: "test-server",
	}
	adminproto.RegisterAdminServiceServer(service.server, service)

	// Start server in background
	go func() {
		if err := service.server.Serve(listener); err != nil {
			// Server stopped, which is expected during cleanup
			return
		}
	}()

	// Create client
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := adminproto.NewAdminServiceClient(conn)

	cleanup := func() {
		conn.Close()
		service.server.GracefulStop()
		listener.Close()
	}

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	return service, client, cleanup
}

func cleanupTestOrg(t *testing.T, service *Service, orgID string) {
	ctx := context.Background()

	// Connect to database
	pool, err := connectToTestConfigDB(ctx)
	require.NoError(t, err)
	defer pool.Close()

	store := configdb.NewStore(pool)

	// Parse org ID
	orgUUID, err := uuid.Parse(orgID)
	require.NoError(t, err)

	// Delete organization
	err = store.DeleteOrganization(ctx, orgUUID)
	if err != nil {
		// Best effort cleanup
		fmt.Printf("Warning: failed to clean up organization %s: %v\n", orgID, err)
	}
}

func connectToTestConfigDB(ctx context.Context) (*pgxpool.Pool, error) {
	// Use environment variables to configure the test database connection
	host := os.Getenv("CONFIGDB_HOST")
	if host == "" {
		host = "localhost"
	}
	dbName := os.Getenv("CONFIGDB_DBNAME")
	if dbName == "" {
		dbName = "testing_configdb"
	}

	connStr := "postgres://" + host + "/" + dbName + "?sslmode=disable"
	return pgxpool.New(ctx, connStr)
}
