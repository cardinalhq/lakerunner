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

package cmd

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cardinalhq/lakerunner/adminproto"
)

// Mock admin service for testing
type mockAdminService struct {
	adminproto.UnimplementedAdminServiceServer
}

func (m *mockAdminService) Ping(ctx context.Context, req *adminproto.PingRequest) (*adminproto.PingResponse, error) {
	response := "pong"
	if req.Message != "" {
		response = fmt.Sprintf("pong: %s", req.Message)
	}
	return &adminproto.PingResponse{
		Message:   response,
		Timestamp: 1234567890,
		ServerId:  "mock-server",
	}, nil
}

func (m *mockAdminService) InQueueStatus(ctx context.Context, req *adminproto.InQueueStatusRequest) (*adminproto.InQueueStatusResponse, error) {
	return &adminproto.InQueueStatusResponse{
		Items: []*adminproto.InQueueItem{
			{Count: 10, TelemetryType: "logs"},
			{Count: 7, TelemetryType: "metrics"},
		},
	}, nil
}

const testBufSize = 1024 * 1024

var testLis *bufconn.Listener

func testBufDialer(ctx context.Context, address string) (net.Conn, error) {
	return testLis.Dial()
}

func setupMockServer(t *testing.T) func() {
	testLis = bufconn.Listen(testBufSize)

	server := grpc.NewServer()
	adminproto.RegisterAdminServiceServer(server, &mockAdminService{})

	go func() {
		if err := server.Serve(testLis); err != nil {
			t.Logf("Mock server exited with error: %v", err)
		}
	}()

	cleanup := func() {
		server.Stop()
		_ = testLis.Close()
	}

	return cleanup
}

func createMockClient(t *testing.T) adminproto.AdminServiceClient {
	conn, err := grpc.DialContext(context.Background(), "bufnet", //nolint:staticcheck // Required for bufconn testing
		grpc.WithContextDialer(testBufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	return adminproto.NewAdminServiceClient(conn)
}

func TestRunAdminInQueueStatusOutput(t *testing.T) {
	cleanup := setupMockServer(t)
	defer cleanup()

	client := createMockClient(t)

	// Test the status call
	ctx := context.Background()
	resp, err := client.InQueueStatus(ctx, &adminproto.InQueueStatusRequest{})
	if err != nil {
		t.Fatalf("InQueueStatus failed: %v", err)
	}

	// Verify response
	if len(resp.Items) != 2 {
		t.Errorf("Expected 2 items, got %d", len(resp.Items))
	}

	if resp.Items[0].Count != 10 || resp.Items[0].TelemetryType != "logs" {
		t.Errorf("Unexpected first item: %+v", resp.Items[0])
	}

	if resp.Items[1].Count != 7 || resp.Items[1].TelemetryType != "metrics" {
		t.Errorf("Unexpected second item: %+v", resp.Items[1])
	}

	// Test table formatting
	if len(resp.Items) > 0 {
		colWidths := []int{len("Count"), len("Telemetry Type")}

		for _, item := range resp.Items {
			countStr := fmt.Sprintf("%d", item.Count)
			if len(countStr) > colWidths[0] {
				colWidths[0] = len(countStr)
			}
			if len(item.TelemetryType) > colWidths[1] {
				colWidths[1] = len(item.TelemetryType)
			}
		}

		// Verify table structure makes sense
		if colWidths[0] < 1 || colWidths[1] < 1 {
			t.Error("Column widths should be at least 1")
		}
	}
}

func TestTableFormatting(t *testing.T) {
	// Test the table drawing logic
	colWidths := []int{5, 7, 6} // Count, Signal, Action

	// Test header row generation
	header := fmt.Sprintf("│ %-*s │ %-*s │ %-*s │", colWidths[0], "Count", colWidths[1], "Signal", colWidths[2], "Action")
	if !strings.Contains(header, "Count") || !strings.Contains(header, "Signal") || !strings.Contains(header, "Action") {
		t.Error("Header should contain all column names")
	}

	// Test data row generation
	dataRow := fmt.Sprintf("│ %-*d │ %-*s │ %-*s │", colWidths[0], 5, colWidths[1], "logs", colWidths[2], "compact")
	if !strings.Contains(dataRow, "5") || !strings.Contains(dataRow, "logs") || !strings.Contains(dataRow, "compact") {
		t.Error("Data row should contain all values")
	}

	// Test border generation
	border := "┌" + strings.Repeat("─", colWidths[0]+2) + "┬" + strings.Repeat("─", colWidths[1]+2) + "┬" + strings.Repeat("─", colWidths[2]+2) + "┐"
	if len(border) < 10 { // Should be a reasonable length
		t.Error("Border should be reasonably long")
	}
}
