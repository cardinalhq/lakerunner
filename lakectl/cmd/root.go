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
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/cmd/admin/kafka"
	"github.com/cardinalhq/lakerunner/cmd/admin/organizations"
)

var (
	adminAPIKey string
)

var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "Administrative commands for Lakerunner",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&adminAPIKey, "api-key", "", "Admin API key (or set LAKERUNNER_ADMIN_API_KEY)")

	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if adminAPIKey == "" {
			adminAPIKey = os.Getenv("LAKERUNNER_ADMIN_API_KEY")
		}
		organizations.SetAPIKey(adminAPIKey)
		kafka.SetAPIKey(adminAPIKey)
	}

	rootCmd.AddCommand(getPingCmd())
	rootCmd.AddCommand(organizations.GetOrganizationsCmd())
	rootCmd.AddCommand(kafka.GetKafkaCmd())
}

func getPingCmd() *cobra.Command {
	var adminAddr string
	pingCmd := &cobra.Command{
		Use:   "ping",
		Short: "Ping the admin service",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := grpc.NewClient(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("failed to connect to admin service: %w", err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					// Log the error if needed, but don't fail the command
					_ = err
				}
			}()

			client := adminproto.NewAdminServiceClient(conn)
			ctx = attachAPIKey(ctx)
			resp, err := client.Ping(ctx, &adminproto.PingRequest{Message: "ping"})
			if err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}
			fmt.Println(resp.Message)
			return nil
		},
	}
	pingCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	return pingCmd
}

func attachAPIKey(ctx context.Context) context.Context {
	if adminAPIKey != "" {
		md := metadata.New(map[string]string{"authorization": "Bearer " + adminAPIKey})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
