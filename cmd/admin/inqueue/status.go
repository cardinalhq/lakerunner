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

package inqueue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	adminAddr string
	apiKey    string
	useLocal  bool
)

func GetStatusCmd() *cobra.Command {
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show status of unclaimed inqueue items by telemetry type",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runInQueueStatus()
		},
	}
	statusCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	statusCmd.Flags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	return statusCmd
}

func SetAPIKey(key string) {
	apiKey = key
}

func runInQueueStatus() error {
	if useLocal {
		return runLocalInQueueStatus()
	}
	return runRemoteInQueueStatus()
}

func runLocalInQueueStatus() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.InqueueSummary(ctx)
	if err != nil {
		return fmt.Errorf("failed to query inqueue summary: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No unclaimed inqueue items found")
		return nil
	}

	colWidths := []int{len("Count"), len("Telemetry Type")}

	for _, result := range results {
		countStr := fmt.Sprintf("%d", result.Count)
		telemetryTypeStr := result.TelemetryType

		if len(countStr) > colWidths[0] {
			colWidths[0] = len(countStr)
		}
		if len(telemetryTypeStr) > colWidths[1] {
			colWidths[1] = len(telemetryTypeStr)
		}
	}

	printInQueueTable(results, colWidths)
	return nil
}

func runRemoteInQueueStatus() error {
	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = createAuthContext(ctx)

	resp, err := client.InQueueStatus(ctx, &adminproto.InQueueStatusRequest{})
	if err != nil {
		return fmt.Errorf("inqueue status failed: %w", err)
	}

	if len(resp.Items) == 0 {
		fmt.Println("No unclaimed inqueue items found")
		return nil
	}

	colWidths := []int{len("Count"), len("Telemetry Type")}

	for _, item := range resp.Items {
		countStr := fmt.Sprintf("%d", item.Count)
		telemetryTypeStr := item.TelemetryType

		if len(countStr) > colWidths[0] {
			colWidths[0] = len(countStr)
		}
		if len(telemetryTypeStr) > colWidths[1] {
			colWidths[1] = len(telemetryTypeStr)
		}
	}

	printInQueueTableFromProto(resp.Items, colWidths)
	return nil
}

func createAdminClient() (adminproto.AdminServiceClient, func(), error) {
	conn, err := grpc.NewClient(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to admin service: %w", err)
	}

	cleanup := func() {
		conn.Close()
	}

	client := adminproto.NewAdminServiceClient(conn)
	return client, cleanup, nil
}

func createAuthContext(ctx context.Context) context.Context {
	if apiKey != "" {
		md := metadata.Pairs("authorization", "Bearer "+apiKey)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func printInQueueTable(results []lrdb.InqueueSummaryRow, colWidths []int) {
	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │\n", colWidths[0], "Count", colWidths[1], "Telemetry Type")

	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	for _, result := range results {
		fmt.Printf("│ %-*d │ %-*s │\n",
			colWidths[0], result.Count,
			colWidths[1], result.TelemetryType)
	}

	fmt.Print("└")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┘")
}

func printInQueueTableFromProto(items []*adminproto.InQueueItem, colWidths []int) {
	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │\n", colWidths[0], "Count", colWidths[1], "Telemetry Type")

	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	for _, item := range items {
		fmt.Printf("│ %-*d │ %-*s │\n",
			colWidths[0], item.Count,
			colWidths[1], item.TelemetryType)
	}

	fmt.Print("└")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┘")
}
