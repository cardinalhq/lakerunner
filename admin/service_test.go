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
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/adminconfig"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const testAPIKey = "ak_test123456789"

const bufSize = 1024 * 1024

var lis *bufconn.Listener

// mockConfigDBStore is a minimal mock for configdb.StoreFull for testing
type mockConfigDBStore struct {
	configdb.Querier
	listOrgsErr error
	orgs        []configdb.Organization
}

func (m *mockConfigDBStore) Close() {}

func (m *mockConfigDBStore) GetStorageProfile(ctx context.Context, arg configdb.GetStorageProfileParams) (configdb.GetStorageProfileRow, error) {
	return configdb.GetStorageProfileRow{}, errors.New("not implemented")
}

func (m *mockConfigDBStore) GetStorageProfileByCollectorName(ctx context.Context, organizationID uuid.UUID) (configdb.GetStorageProfileByCollectorNameRow, error) {
	return configdb.GetStorageProfileByCollectorNameRow{}, errors.New("not implemented")
}

func (m *mockConfigDBStore) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]configdb.GetStorageProfilesByBucketNameRow, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConfigDBStore) CreateOrganizationAPIKeyWithMapping(ctx context.Context, params configdb.CreateOrganizationAPIKeyParams, orgID uuid.UUID) (*configdb.OrganizationApiKey, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConfigDBStore) DeleteOrganizationAPIKeyWithMappings(ctx context.Context, keyID uuid.UUID) error {
	return errors.New("not implemented")
}

func (m *mockConfigDBStore) AddOrganizationBucket(ctx context.Context, orgID uuid.UUID, bucketName string, instanceNum int16, collectorName string) error {
	return errors.New("not implemented")
}

func (m *mockConfigDBStore) CreateBucketPrefixMappingForOrg(ctx context.Context, bucketName string, orgID uuid.UUID, pathPrefix string, signal string) (*configdb.BucketPrefixMapping, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConfigDBStore) ListOrganizations(ctx context.Context) ([]configdb.Organization, error) {
	if m.listOrgsErr != nil {
		return nil, m.listOrgsErr
	}
	return m.orgs, nil
}

func (m *mockConfigDBStore) UpsertOrganization(ctx context.Context, arg configdb.UpsertOrganizationParams) (configdb.Organization, error) {
	return configdb.Organization{}, errors.New("not implemented")
}

func (m *mockConfigDBStore) GetOrganization(ctx context.Context, id uuid.UUID) (configdb.Organization, error) {
	return configdb.Organization{}, errors.New("not implemented")
}

func (m *mockConfigDBStore) GetOrgConfig(ctx context.Context, arg configdb.GetOrgConfigParams) (json.RawMessage, error) {
	return nil, sql.ErrNoRows
}

func (m *mockConfigDBStore) UpsertOrgConfig(ctx context.Context, arg configdb.UpsertOrgConfigParams) error {
	return errors.New("not implemented")
}

func (m *mockConfigDBStore) DeleteOrgConfig(ctx context.Context, arg configdb.DeleteOrgConfigParams) error {
	return errors.New("not implemented")
}

func (m *mockConfigDBStore) ListOrgConfigs(ctx context.Context, organizationID uuid.UUID) ([]configdb.ListOrgConfigsRow, error) {
	return nil, errors.New("not implemented")
}

// mockLRDBStore is a minimal mock for LRDBStore interface for testing
type mockLRDBStore struct {
	workQueueStatusErr error
	workQueueStatus    []lrdb.WorkQueueStatusRow
}

func (m *mockLRDBStore) WorkQueueStatus(ctx context.Context) ([]lrdb.WorkQueueStatusRow, error) {
	if m.workQueueStatusErr != nil {
		return nil, m.workQueueStatusErr
	}
	return m.workQueueStatus, nil
}

func (m *mockLRDBStore) WorkQueueAdd(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
	return lrdb.WorkQueue{}, errors.New("not implemented")
}

func (m *mockLRDBStore) WorkQueueDepth(ctx context.Context, taskName string) (int64, error) {
	return 0, errors.New("not implemented")
}

func (m *mockLRDBStore) WorkQueueCleanup(ctx context.Context, heartbeatTimeout time.Duration) error {
	return errors.New("not implemented")
}

func (m *mockLRDBStore) ListLogSegsForRecompact(ctx context.Context, arg lrdb.ListLogSegsForRecompactParams) ([]lrdb.LogSeg, error) {
	return nil, errors.New("not implemented")
}

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}

func authContext() context.Context {
	md := metadata.Pairs("authorization", "Bearer "+testAPIKey)
	return metadata.NewOutgoingContext(context.Background(), md)
}

type testServerOpts struct {
	configDB configdb.StoreFull
	lrDB     LRDBStore
}

func setupTestServer(t *testing.T, opts ...testServerOpts) (adminproto.AdminServiceClient, func()) {
	lis = bufconn.Listen(bufSize)

	// Create mock config provider with a valid test key
	configProvider := &mockAdminConfigProvider{
		validKeys: map[string]*adminconfig.AdminAPIKey{
			testAPIKey: {Name: "test-key"},
		},
	}

	// Create auth interceptor
	authInterceptor := newAuthInterceptor(configProvider)

	server := grpc.NewServer(
		grpc.UnaryInterceptor(authInterceptor.unaryInterceptor),
	)

	// Use provided stores or default mocks
	var configDB configdb.StoreFull = &mockConfigDBStore{}
	var lrDB LRDBStore = &mockLRDBStore{}
	if len(opts) > 0 {
		if opts[0].configDB != nil {
			configDB = opts[0].configDB
		}
		if opts[0].lrDB != nil {
			lrDB = opts[0].lrDB
		}
	}

	// Create our admin service with mock stores
	adminService := &Service{
		serverID: "test-server",
		configDB: configDB,
		lrDB:     lrDB,
	}

	adminproto.RegisterAdminServiceServer(server, adminService)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Create client
	conn, err := grpc.DialContext(context.Background(), "bufnet", //nolint:staticcheck // Required for bufconn testing
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := adminproto.NewAdminServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	}

	return client, cleanup
}

func TestPing(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := authContext()

	// Test empty message
	resp, err := client.Ping(ctx, &adminproto.PingRequest{})
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	if resp.Message != "pong" {
		t.Errorf("Expected 'pong', got %q", resp.Message)
	}

	if resp.ServerId != "test-server" {
		t.Errorf("Expected 'test-server', got %q", resp.ServerId)
	}

	if resp.Timestamp == 0 {
		t.Error("Expected non-zero timestamp")
	}

	// Test with message
	resp, err = client.Ping(ctx, &adminproto.PingRequest{Message: "hello"})
	if err != nil {
		t.Fatalf("Ping with message failed: %v", err)
	}

	if resp.Message != "pong: hello" {
		t.Errorf("Expected 'pong: hello', got %q", resp.Message)
	}
}

func TestInQueueStatus(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := authContext()

	// Should return empty response (InQueueStatus returns empty for backward compatibility)
	resp, err := client.InQueueStatus(ctx, &adminproto.InQueueStatusRequest{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Should return empty items
	if resp == nil || len(resp.Items) != 0 {
		t.Error("Expected empty items in response")
	}
}

func TestListOrganizationsWithDBError(t *testing.T) {
	// Create mock that returns an error
	mockStore := &mockConfigDBStore{
		listOrgsErr: errors.New("database error"),
	}

	client, cleanup := setupTestServer(t, testServerOpts{configDB: mockStore})
	defer cleanup()

	ctx := authContext()

	_, err := client.ListOrganizations(ctx, &adminproto.ListOrganizationsRequest{})
	if err == nil {
		t.Error("Expected error due to database error")
	}
}

type stubAdminClient struct {
	lags []fly.ConsumerGroupInfo
}

func (s *stubAdminClient) GetMultipleConsumerGroupLag(ctx context.Context, topicGroups map[string]string) ([]fly.ConsumerGroupInfo, error) {
	return s.lags, nil
}

func TestGetConsumerLag(t *testing.T) {
	originalLoadConfig := loadConfig
	originalNewAdminClient := newAdminClient
	defer func() {
		loadConfig = originalLoadConfig
		newAdminClient = originalNewAdminClient
	}()

	loadConfig = func() (*config.Config, error) {
		cfg := &config.Config{Kafka: config.DefaultKafkaConfig(), TopicRegistry: config.NewTopicRegistry("test")}
		return cfg, nil
	}

	stub := &stubAdminClient{lags: []fly.ConsumerGroupInfo{{
		Topic:           "test.topic",
		Partition:       0,
		CommittedOffset: 5,
		HighWaterMark:   10,
		Lag:             5,
		GroupID:         "test-group",
	}}}

	newAdminClient = func(conf *config.KafkaConfig) (fly.AdminClientInterface, error) {
		return stub, nil
	}

	svc := &Service{serverID: "test"}

	resp, err := svc.GetConsumerLag(context.Background(), &adminproto.GetConsumerLagRequest{})
	if err != nil {
		t.Fatalf("GetConsumerLag failed: %v", err)
	}
	if len(resp.Lags) != 1 {
		t.Fatalf("expected 1 lag entry, got %d", len(resp.Lags))
	}
	lag := resp.Lags[0]
	if lag.Topic != "test.topic" || lag.Partition != 0 || lag.ConsumerGroup != "test-group" || lag.Lag != 5 {
		t.Fatalf("unexpected lag response: %+v", lag)
	}
}

func TestGetWorkQueueStatusWithDBError(t *testing.T) {
	// Create mock that returns an error
	mockLRDB := &mockLRDBStore{
		workQueueStatusErr: errors.New("database error"),
	}

	client, cleanup := setupTestServer(t, testServerOpts{lrDB: mockLRDB})
	defer cleanup()

	ctx := authContext()

	_, err := client.GetWorkQueueStatus(ctx, &adminproto.GetWorkQueueStatusRequest{})
	if err == nil {
		t.Error("Expected error due to database error")
	}
}

func TestCreateOrganizationWithDBError(t *testing.T) {
	// Uses the default mock which returns "not implemented" for UpsertOrganization
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := authContext()

	_, err := client.CreateOrganization(ctx, &adminproto.CreateOrganizationRequest{Name: "test", Enabled: true})
	if err == nil {
		t.Error("Expected error due to missing database connection")
	}

	if err != nil && err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}
