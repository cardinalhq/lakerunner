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
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/internal/adminconfig"
)

const authBufSize = 1024 * 1024

var authLis *bufconn.Listener

func authBufDialer(ctx context.Context, address string) (net.Conn, error) {
	return authLis.Dial()
}

// Mock admin config provider for testing
type mockAdminConfigProvider struct {
	validKeys map[string]*adminconfig.AdminAPIKey
}

func (m *mockAdminConfigProvider) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	if apiKey == "" {
		return false, nil
	}
	_, exists := m.validKeys[apiKey]
	return exists, nil
}

func (m *mockAdminConfigProvider) GetAPIKeyInfo(ctx context.Context, apiKey string) (*adminconfig.AdminAPIKey, error) {
	if key, exists := m.validKeys[apiKey]; exists {
		return &adminconfig.AdminAPIKey{
			Name:        key.Name,
			Description: key.Description,
		}, nil
	}
	return nil, status.Error(codes.NotFound, "API key not found")
}

func setupAuthTestServer(t *testing.T, configProvider adminconfig.AdminConfigProvider) (adminproto.AdminServiceClient, func()) {
	authLis = bufconn.Listen(authBufSize)

	// Create auth interceptor
	authInterceptor := newAuthInterceptor(configProvider)

	// Create GRPC server with auth interceptor
	server := grpc.NewServer(
		grpc.UnaryInterceptor(authInterceptor.unaryInterceptor),
	)

	// Create our admin service
	adminService := &Service{
		serverID: "test-server",
	}

	adminproto.RegisterAdminServiceServer(server, adminService)

	go func() {
		if err := server.Serve(authLis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Create client
	conn, err := grpc.DialContext(context.Background(), "bufnet", //nolint:staticcheck // Required for bufconn testing
		grpc.WithContextDialer(authBufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := adminproto.NewAdminServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = authLis.Close()
	}

	return client, cleanup
}

func TestAuthentication_ValidAPIKey(t *testing.T) {
	configProvider := &mockAdminConfigProvider{
		validKeys: map[string]*adminconfig.AdminAPIKey{
			"ak_valid123": {
				Name:        "test-key",
				Description: "Test key",
			},
		},
	}

	client, cleanup := setupAuthTestServer(t, configProvider)
	defer cleanup()

	// Create context with valid API key
	md := metadata.Pairs("authorization", "Bearer ak_valid123")
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	// Test ping with valid auth
	resp, err := client.Ping(ctx, &adminproto.PingRequest{Message: "test"})
	if err != nil {
		t.Fatalf("Ping with valid auth failed: %v", err)
	}

	if resp.Message != "pong: test" {
		t.Errorf("Expected 'pong: test', got %q", resp.Message)
	}
}

func TestAuthentication_InvalidAPIKey(t *testing.T) {
	configProvider := &mockAdminConfigProvider{
		validKeys: map[string]*adminconfig.AdminAPIKey{
			"ak_valid123": {
				Name: "test-key",
			},
		},
	}

	client, cleanup := setupAuthTestServer(t, configProvider)
	defer cleanup()

	// Create context with invalid API key
	md := metadata.Pairs("authorization", "Bearer ak_invalid")
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	// Test ping with invalid auth
	_, err := client.Ping(ctx, &adminproto.PingRequest{Message: "test"})
	if err == nil {
		t.Fatal("Expected authentication error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("Expected GRPC status error")
	}

	if st.Code() != codes.Unauthenticated {
		t.Errorf("Expected Unauthenticated error, got %v", st.Code())
	}
}

func TestAuthentication_MissingAuthHeader(t *testing.T) {
	configProvider := &mockAdminConfigProvider{
		validKeys: map[string]*adminconfig.AdminAPIKey{
			"ak_valid123": {
				Name: "test-key",
			},
		},
	}

	client, cleanup := setupAuthTestServer(t, configProvider)
	defer cleanup()

	// Test ping without auth header
	_, err := client.Ping(context.Background(), &adminproto.PingRequest{Message: "test"})
	if err == nil {
		t.Fatal("Expected authentication error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("Expected GRPC status error")
	}

	if st.Code() != codes.Unauthenticated {
		t.Errorf("Expected Unauthenticated error, got %v", st.Code())
	}
}

func TestAuthentication_InvalidAuthFormat(t *testing.T) {
	configProvider := &mockAdminConfigProvider{
		validKeys: map[string]*adminconfig.AdminAPIKey{
			"ak_valid123": {Name: "test-key"},
		},
	}

	client, cleanup := setupAuthTestServer(t, configProvider)
	defer cleanup()

	tests := []struct {
		name       string
		authHeader string
	}{
		{
			name:       "missing Bearer prefix",
			authHeader: "ak_valid123",
		},
		{
			name:       "wrong prefix",
			authHeader: "Basic ak_valid123",
		},
		{
			name:       "empty key",
			authHeader: "Bearer ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := metadata.Pairs("authorization", tt.authHeader)
			ctx := metadata.NewOutgoingContext(context.Background(), md)

			_, err := client.Ping(ctx, &adminproto.PingRequest{Message: "test"})
			if err == nil {
				t.Fatal("Expected authentication error")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatal("Expected GRPC status error")
			}

			if st.Code() != codes.Unauthenticated {
				t.Errorf("Expected Unauthenticated error, got %v", st.Code())
			}
		})
	}
}

func TestAuthentication_NoKeysConfigured(t *testing.T) {
	// Empty config provider should reject all requests
	configProvider := &mockAdminConfigProvider{
		validKeys: map[string]*adminconfig.AdminAPIKey{},
	}

	client, cleanup := setupAuthTestServer(t, configProvider)
	defer cleanup()

	// Test ping without auth header should fail with empty config
	_, err := client.Ping(context.Background(), &adminproto.PingRequest{Message: "test"})
	if err == nil {
		t.Fatal("Expected authentication error with no keys configured")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatal("Expected GRPC status error")
	}

	if st.Code() != codes.Unauthenticated {
		t.Errorf("Expected Unauthenticated error, got %v", st.Code())
	}
}
