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
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/admin"
	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/bootstrap"
)

var (
	adminAddr   string
	adminAPIKey string
)

func init() {
	adminCmd := &cobra.Command{
		Use:   "admin",
		Short: "Administrative commands for LakeRunner",
	}

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the admin GRPC service",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			service, err := admin.NewService(adminAddr)
			if err != nil {
				return fmt.Errorf("failed to create admin service: %w", err)
			}

			return service.Run(ctx)
		},
	}
	serveCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address to bind the admin service")

	pingCmd := &cobra.Command{
		Use:   "ping",
		Short: "Ping the admin service",
		RunE: func(_ *cobra.Command, args []string) error {
			client, cleanup, err := createAdminClient()
			if err != nil {
				return err
			}
			defer cleanup()

			message := ""
			if len(args) > 0 {
				message = args[0]
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctx = createAuthContext(ctx)

			resp, err := client.Ping(ctx, &adminproto.PingRequest{Message: message})
			if err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}

			slog.Info("Ping successful",
				slog.String("message", resp.Message),
				slog.Int64("timestamp", resp.Timestamp),
				slog.String("server_id", resp.ServerId))

			return nil
		},
	}
	pingCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")

	workqueueCmd := &cobra.Command{
		Use:   "workqueue",
		Short: "Workqueue administrative commands",
	}

	workqueueStatusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show status of runnable work queue items by signal and action",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAdminWorkQueueStatus()
		},
	}
	workqueueStatusCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")

	inqueueCmdAdmin := &cobra.Command{
		Use:   "inqueue",
		Short: "Inqueue administrative commands",
	}

	inqueueStatusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show status of unclaimed inqueue items by telemetry type",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAdminInQueueStatus()
		},
	}
	inqueueStatusCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")

	bootstrapCmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap configuration management",
	}

	var bootstrapFile string
	bootstrapImportCmd := &cobra.Command{
		Use:   "import",
		Short: "Import configuration from YAML file (one-time bootstrap)",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Connect to configdb
			configDBPool, err := dbopen.ConnectToConfigDB(ctx)
			if err != nil {
				return fmt.Errorf("failed to connect to configdb: %w", err)
			}
			defer configDBPool.Close()

			// Run import
			return bootstrap.ImportFromYAML(ctx, bootstrapFile, configDBPool, slog.Default())
		},
	}
	bootstrapImportCmd.Flags().StringVar(&bootstrapFile, "file", "", "YAML file to import (required)")
	if err := bootstrapImportCmd.MarkFlagRequired("file"); err != nil {
		panic(fmt.Sprintf("failed to mark flag as required: %v", err))
	}

	bootstrapCmd.AddCommand(bootstrapImportCmd)
	workqueueCmd.AddCommand(workqueueStatusCmd)
	inqueueCmdAdmin.AddCommand(inqueueStatusCmd)

	adminCmd.AddCommand(serveCmd)
	adminCmd.AddCommand(pingCmd)
	adminCmd.AddCommand(workqueueCmd)
	adminCmd.AddCommand(inqueueCmdAdmin)
	adminCmd.AddCommand(bootstrapCmd)
	// Set API key from environment variable if not provided via flag
	adminCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if adminAPIKey == "" {
			adminAPIKey = os.Getenv("ADMIN_API_KEY")
		}
	}

	// Add API key flag to admin command and all subcommands
	adminCmd.PersistentFlags().StringVar(&adminAPIKey, "api-key", "", "Admin API key (can also be set via ADMIN_API_KEY environment variable)")

	rootCmd.AddCommand(adminCmd)
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
	if adminAPIKey != "" {
		md := metadata.Pairs("authorization", "Bearer "+adminAPIKey)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func runAdminWorkQueueStatus() error {
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

	for _, item := range resp.Items {
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

	return nil
}

func runAdminInQueueStatus() error {
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

	for _, item := range resp.Items {
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

	return nil
}
