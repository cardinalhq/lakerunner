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
	"github.com/cardinalhq/lakerunner/internal/helpers"
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
		Short: "Show detailed status of runnable work queue items by signal and action (timestamps in UTC)",
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

	// Use admin-friendly connection that warns on migration mismatches instead of failing
	// Alternative syntax: dbopen.ConnectTolrdb(ctx, dbopen.WarnOnMigrationMismatch())
	store, err := dbopen.LRDBStoreForAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.WorkQueueExtendedStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to query work queue extended status: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No runnable work queue items found")
		return nil
	}

	printExtendedWorkQueueTable(results)
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

func formatDuration(seconds *float64) string {
	if seconds == nil || *seconds == 0 {
		return ""
	}
	duration := time.Duration(*seconds) * time.Second
	return duration.String()
}

func printExtendedWorkQueueTable(results []lrdb.WorkQueueExtendedStatusRow) {
	// Column headers and widths
	headers := []string{"Signal", "Action", "Time Range", "Claimed At", "Age", "Worker ID"}
	colWidths := make([]int, len(headers))

	// Initialize with header lengths
	for i, header := range headers {
		colWidths[i] = len(header)
	}

	// Calculate column widths by examining all data
	for _, result := range results {
		signalStr := string(result.Signal)
		actionStr := string(result.Action)

		if len(signalStr) > colWidths[0] {
			colWidths[0] = len(signalStr)
		}
		if len(actionStr) > colWidths[1] {
			colWidths[1] = len(actionStr)
		}

		if result.RowType == "unclaimed" {
			// For unclaimed, show count in Time Range column
			countStr := fmt.Sprintf("%d items pending", result.CountOrClaimedBy)
			if len(countStr) > colWidths[2] {
				colWidths[2] = len(countStr)
			}
		} else {
			// For claimed, show actual data
			timeRange := helpers.FormatTSRange(result.TsRange)
			if len(timeRange) > colWidths[2] {
				colWidths[2] = len(timeRange)
			}

			if result.ClaimedAtUtc != nil {
				claimedStr := result.ClaimedAtUtc.Format("15:04:05")
				if len(claimedStr) > colWidths[3] {
					colWidths[3] = len(claimedStr)
				}
			}

			ageStr := formatDuration(result.AgeSeconds)
			if result.IsStale {
				ageStr += " (STALE)"
			}
			if len(ageStr) > colWidths[4] {
				colWidths[4] = len(ageStr)
			}

			workerStr := fmt.Sprintf("%d", result.CountOrClaimedBy)
			if len(workerStr) > colWidths[5] {
				colWidths[5] = len(workerStr)
			}
		}
	}

	// Print table header
	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	// Print header row
	fmt.Print("│")
	for i, header := range headers {
		fmt.Printf(" %-*s │", colWidths[i], header)
	}
	fmt.Println()

	// Print separator
	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	// Print data rows
	for _, result := range results {
		fmt.Print("│")

		// Signal
		fmt.Printf(" %-*s │", colWidths[0], string(result.Signal))

		// Action
		fmt.Printf(" %-*s │", colWidths[1], string(result.Action))

		if result.RowType == "unclaimed" {
			// Time Range shows count
			countStr := fmt.Sprintf("%d items pending", result.CountOrClaimedBy)
			fmt.Printf(" %-*s │", colWidths[2], countStr)

			// Empty columns for unclaimed
			fmt.Printf(" %-*s │", colWidths[3], "-")
			fmt.Printf(" %-*s │", colWidths[4], "-")
			fmt.Printf(" %-*s │", colWidths[5], "-")
		} else {
			// Time Range shows actual range
			timeRange := helpers.FormatTSRange(result.TsRange)
			fmt.Printf(" %-*s │", colWidths[2], timeRange)

			// Claimed At
			claimedStr := "-"
			if result.ClaimedAtUtc != nil {
				claimedStr = result.ClaimedAtUtc.Format("15:04:05")
			}
			fmt.Printf(" %-*s │", colWidths[3], claimedStr)

			// Age
			ageStr := formatDuration(result.AgeSeconds)
			if result.IsStale {
				ageStr += " (STALE)"
			}
			if ageStr == "" {
				ageStr = "-"
			}
			fmt.Printf(" %-*s │", colWidths[4], ageStr)

			// Worker ID
			fmt.Printf(" %-*d │", colWidths[5], result.CountOrClaimedBy)
		}

		fmt.Println()
	}

	// Print table footer
	fmt.Print("└")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┘")
}
