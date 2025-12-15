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

package admin

import (
	"context"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/adminconfig"
	"github.com/cardinalhq/lakerunner/internal/fly"
)

const testAPIKey = "ak_test123456789"

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}

func authContext() context.Context {
	md := metadata.Pairs("authorization", "Bearer "+testAPIKey)
	return metadata.NewOutgoingContext(context.Background(), md)
}

func setupTestServer(t *testing.T) (adminproto.AdminServiceClient, func()) {
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

	// Create our admin service
	adminService := &Service{
		serverID: "test-server",
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

func TestInQueueStatusWithoutDB(t *testing.T) {
	// Save original database environment variables
	originalLRDBVars := map[string]string{
		"LRDB_HOST":     os.Getenv("LRDB_HOST"),
		"LRDB_USER":     os.Getenv("LRDB_USER"),
		"LRDB_PASSWORD": os.Getenv("LRDB_PASSWORD"),
		"LRDB_DBNAME":   os.Getenv("LRDB_DBNAME"),
		"LRDB_URL":      os.Getenv("LRDB_URL"),
	}

	// Temporarily unset database environment variables to force database connection failure
	_ = os.Unsetenv("LRDB_HOST")
	_ = os.Unsetenv("LRDB_USER")
	_ = os.Unsetenv("LRDB_PASSWORD")
	_ = os.Unsetenv("LRDB_DBNAME")
	_ = os.Unsetenv("LRDB_URL")

	// Restore environment variables when test completes
	defer func() {
		for key, value := range originalLRDBVars {
			if value != "" {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := authContext()

	// Should return empty response
	resp, err := client.InQueueStatus(ctx, &adminproto.InQueueStatusRequest{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Should return empty items
	if resp == nil || len(resp.Items) != 0 {
		t.Error("Expected empty items in response")
	}
}

func TestListOrganizationsWithoutDB(t *testing.T) {
	originalConfigDBVars := map[string]string{
		"CONFIGDB_HOST":     os.Getenv("CONFIGDB_HOST"),
		"CONFIGDB_USER":     os.Getenv("CONFIGDB_USER"),
		"CONFIGDB_PASSWORD": os.Getenv("CONFIGDB_PASSWORD"),
		"CONFIGDB_DBNAME":   os.Getenv("CONFIGDB_DBNAME"),
		"CONFIGDB_URL":      os.Getenv("CONFIGDB_URL"),
	}

	_ = os.Unsetenv("CONFIGDB_HOST")
	_ = os.Unsetenv("CONFIGDB_USER")
	_ = os.Unsetenv("CONFIGDB_PASSWORD")
	_ = os.Unsetenv("CONFIGDB_DBNAME")
	_ = os.Unsetenv("CONFIGDB_URL")

	defer func() {
		for key, value := range originalConfigDBVars {
			if value != "" {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := authContext()

	_, err := client.ListOrganizations(ctx, &adminproto.ListOrganizationsRequest{})
	if err == nil {
		t.Error("Expected error due to missing database connection")
	}

	if err != nil && err.Error() == "" {
		t.Error("Expected non-empty error message")
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

func TestGetWorkQueueStatusWithoutDB(t *testing.T) {
	originalLRDBVars := map[string]string{
		"LRDB_HOST":     os.Getenv("LRDB_HOST"),
		"LRDB_USER":     os.Getenv("LRDB_USER"),
		"LRDB_PASSWORD": os.Getenv("LRDB_PASSWORD"),
		"LRDB_DBNAME":   os.Getenv("LRDB_DBNAME"),
		"LRDB_URL":      os.Getenv("LRDB_URL"),
	}

	_ = os.Unsetenv("LRDB_HOST")
	_ = os.Unsetenv("LRDB_USER")
	_ = os.Unsetenv("LRDB_PASSWORD")
	_ = os.Unsetenv("LRDB_DBNAME")
	_ = os.Unsetenv("LRDB_URL")

	defer func() {
		for key, value := range originalLRDBVars {
			if value != "" {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}()

	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := authContext()

	_, err := client.GetWorkQueueStatus(ctx, &adminproto.GetWorkQueueStatusRequest{})
	if err == nil {
		t.Error("Expected error due to missing database connection")
	}

	if err != nil && err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestCreateOrganizationWithoutDB(t *testing.T) {
	originalConfigDBVars := map[string]string{
		"CONFIGDB_HOST":     os.Getenv("CONFIGDB_HOST"),
		"CONFIGDB_USER":     os.Getenv("CONFIGDB_USER"),
		"CONFIGDB_PASSWORD": os.Getenv("CONFIGDB_PASSWORD"),
		"CONFIGDB_DBNAME":   os.Getenv("CONFIGDB_DBNAME"),
		"CONFIGDB_URL":      os.Getenv("CONFIGDB_URL"),
	}

	_ = os.Unsetenv("CONFIGDB_HOST")
	_ = os.Unsetenv("CONFIGDB_USER")
	_ = os.Unsetenv("CONFIGDB_PASSWORD")
	_ = os.Unsetenv("CONFIGDB_DBNAME")
	_ = os.Unsetenv("CONFIGDB_URL")

	defer func() {
		for key, value := range originalConfigDBVars {
			if value != "" {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}()

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
