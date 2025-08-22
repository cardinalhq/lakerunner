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

package workqueue

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
		Short: "Show status of runnable work queue items by signal and action",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runWorkQueueStatus()
		},
	}
	statusCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	statusCmd.Flags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	return statusCmd
}

func SetAPIKey(key string) {
	apiKey = key
}

func runWorkQueueStatus() error {
	if useLocal {
		return runLocalWorkQueueStatus()
	}
	return runRemoteWorkQueueStatus()
}

func runLocalWorkQueueStatus() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.WorkQueueSummary(ctx)
	if err != nil {
		return fmt.Errorf("failed to query work queue summary: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No runnable work queue items found")
		return nil
	}

	colWidths := []int{len("Count"), len("Signal"), len("Action")}

	for _, result := range results {
		countStr := fmt.Sprintf("%d", result.Count)
		signalStr := string(result.Signal)
		actionStr := string(result.Action)

		if len(countStr) > colWidths[0] {
			colWidths[0] = len(countStr)
		}
		if len(signalStr) > colWidths[1] {
			colWidths[1] = len(signalStr)
		}
		if len(actionStr) > colWidths[2] {
			colWidths[2] = len(actionStr)
		}
	}

	printWorkQueueTable(results, colWidths)
	return nil
}

func runRemoteWorkQueueStatus() error {
	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = createAuthContext(ctx)

	resp, err := client.WorkQueueStatus(ctx, &adminproto.WorkQueueStatusRequest{})
	if err != nil {
		return fmt.Errorf("workqueue status failed: %w", err)
	}

	if len(resp.Items) == 0 {
		fmt.Println("No runnable work queue items found")
		return nil
	}

	colWidths := []int{len("Count"), len("Signal"), len("Action")}

	for _, item := range resp.Items {
		countStr := fmt.Sprintf("%d", item.Count)
		signalStr := item.Signal
		actionStr := item.Action

		if len(countStr) > colWidths[0] {
			colWidths[0] = len(countStr)
		}
		if len(signalStr) > colWidths[1] {
			colWidths[1] = len(signalStr)
		}
		if len(actionStr) > colWidths[2] {
			colWidths[2] = len(actionStr)
		}
	}

	printWorkQueueTableFromProto(resp.Items, colWidths)
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

func printWorkQueueTable(results []lrdb.WorkQueueSummaryRow, colWidths []int) {
	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │ %-*s │\n", colWidths[0], "Count", colWidths[1], "Signal", colWidths[2], "Action")

	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	for _, result := range results {
		fmt.Printf("│ %-*d │ %-*s │ %-*s │\n",
			colWidths[0], result.Count,
			colWidths[1], string(result.Signal),
			colWidths[2], string(result.Action))
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

func printWorkQueueTableFromProto(items []*adminproto.WorkQueueItem, colWidths []int) {
	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │ %-*s │\n", colWidths[0], "Count", colWidths[1], "Signal", colWidths[2], "Action")

	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	for _, item := range items {
		fmt.Printf("│ %-*d │ %-*s │ %-*s │\n",
			colWidths[0], item.Count,
			colWidths[1], item.Signal,
			colWidths[2], item.Action)
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
