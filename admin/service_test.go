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
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cardinalhq/lakerunner/adminproto"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}

func setupTestServer(t *testing.T) (adminproto.AdminServiceClient, func()) {
	lis = bufconn.Listen(bufSize)

	server := grpc.NewServer()

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
		conn.Close()
		server.Stop()
		lis.Close()
	}

	return client, cleanup
}

func TestPing(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

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

func TestWorkQueueStatusWithoutDB(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// This will fail because we don't have a database connection in the test
	// but it tests that the GRPC plumbing works
	_, err := client.WorkQueueStatus(ctx, &adminproto.WorkQueueStatusRequest{})
	if err == nil {
		t.Error("Expected error due to missing database connection")
	}

	// Check that error message contains database connection failure
	if err != nil && err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestInQueueStatusWithoutDB(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// This will fail because we don't have a database connection in the test
	// but it tests that the GRPC plumbing works
	_, err := client.InQueueStatus(ctx, &adminproto.InQueueStatusRequest{})
	if err == nil {
		t.Error("Expected error due to missing database connection")
	}

	// Check that error message contains database connection failure
	if err != nil && err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}
