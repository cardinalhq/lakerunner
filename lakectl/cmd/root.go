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

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/adminclient"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/kafka"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/logs"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/organizations"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/workqueue"
)

var (
	adminAPIKey   string
	endpoint      string
	insecureMode  bool
	tlsSkipVerify bool
	tlsCACert     string
)

var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "Administrative commands for Lakerunner",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&adminAPIKey, "api-key", "", "Admin API key (or set LAKERUNNER_ADMIN_API_KEY)")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "", "Admin service endpoint (or set LAKERUNNER_ADMIN_API_ENDPOINT)")
	rootCmd.PersistentFlags().BoolVar(&insecureMode, "insecure", false, "Disable TLS")
	rootCmd.PersistentFlags().BoolVar(&tlsSkipVerify, "tls-skip-verify", false, "Skip TLS certificate verification")
	rootCmd.PersistentFlags().StringVar(&tlsCACert, "tls-ca-cert", "", "Path to CA certificate for TLS verification")

	rootCmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		if adminAPIKey == "" {
			adminAPIKey = os.Getenv("LAKERUNNER_ADMIN_API_KEY")
		}
		if endpoint == "" {
			endpoint = os.Getenv("LAKERUNNER_ADMIN_API_ENDPOINT")
		}
		if endpoint == "" {
			return fmt.Errorf("endpoint required: set --endpoint flag or LAKERUNNER_ADMIN_API_ENDPOINT environment variable")
		}
		adminclient.SetAPIKey(adminAPIKey)
		adminclient.SetConnectionConfig(endpoint, insecureMode, tlsSkipVerify, tlsCACert)
		return nil
	}

	rootCmd.AddCommand(getPingCmd())
	rootCmd.AddCommand(organizations.GetOrganizationsCmd())
	rootCmd.AddCommand(kafka.GetKafkaCmd())
	rootCmd.AddCommand(workqueue.GetWorkQueueCmd())
	rootCmd.AddCommand(logs.GetLogsCmd())
}

func getPingCmd() *cobra.Command {
	pingCmd := &cobra.Command{
		Use:   "ping",
		Short: "Ping the admin service",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client, cleanup, err := adminclient.CreateClient()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx = adminclient.AttachAPIKey(ctx)
			resp, err := client.Ping(ctx, &adminproto.PingRequest{Message: "ping"})
			if err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}
			fmt.Println(resp.Message)
			return nil
		},
	}
	return pingCmd
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
