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

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/admin"
	"github.com/cardinalhq/lakerunner/cmd/admin/adminapikeys"
	"github.com/cardinalhq/lakerunner/cmd/admin/bootstrap"
	"github.com/cardinalhq/lakerunner/cmd/admin/bucketprefixes"
	"github.com/cardinalhq/lakerunner/cmd/admin/logs"
	"github.com/cardinalhq/lakerunner/cmd/admin/metrics"
	"github.com/cardinalhq/lakerunner/cmd/admin/metrics/estimates"
	"github.com/cardinalhq/lakerunner/cmd/admin/objcleanup"
	"github.com/cardinalhq/lakerunner/cmd/admin/organizations"
	"github.com/cardinalhq/lakerunner/cmd/admin/orgapikeys"
	"github.com/cardinalhq/lakerunner/cmd/admin/storageprofiles"
	"github.com/cardinalhq/lakerunner/cmd/admin/traces"
)

var (
	adminAddr   string
	adminAPIKey string
)

func init() {
	adminCmd := &cobra.Command{
		Use:   "admin",
		Short: "Administrative commands for Lakerunner",
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
			// TODO: Implement ping functionality if needed
			return fmt.Errorf("ping command not yet implemented")
		},
	}
	pingCmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")

	// Get subcommands from their respective packages
	objcleanupCmd := getObjCleanupCmd()
	bootstrapCmd := bootstrap.GetBootstrapCmd()
	orgCmd := organizations.GetOrganizationsCmd()
	orgAPIKeysCmd := orgapikeys.GetOrgAPIKeysCmd()
	adminAPIKeysCmd := adminapikeys.GetAdminAPIKeysCmd()
	storageProfilesCmd := storageprofiles.GetStorageProfilesCmd()
	bucketPrefixesCmd := bucketprefixes.GetBucketPrefixesCmd()
	logsCmd := getLogsCmd()
	tracesCmd := getTracesCmd()
	metricsCmd := getMetricsCmd()

	// Add subcommands to admin command
	adminCmd.AddCommand(serveCmd)
	adminCmd.AddCommand(pingCmd)
	adminCmd.AddCommand(objcleanupCmd)
	adminCmd.AddCommand(bootstrapCmd)
	adminCmd.AddCommand(orgCmd)
	adminCmd.AddCommand(orgAPIKeysCmd)
	adminCmd.AddCommand(adminAPIKeysCmd)
	adminCmd.AddCommand(storageProfilesCmd)
	adminCmd.AddCommand(bucketPrefixesCmd)
	adminCmd.AddCommand(logsCmd)
	adminCmd.AddCommand(tracesCmd)
	adminCmd.AddCommand(metricsCmd)

	// Set API key from environment variable if not provided via flag
	adminCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if adminAPIKey == "" {
			adminAPIKey = os.Getenv("ADMIN_API_KEY")
		}
		// Pass the API key to subcommands
		objcleanup.SetAPIKey(adminAPIKey)
		organizations.SetAPIKey(adminAPIKey)
		orgapikeys.SetAPIKey(adminAPIKey)
		adminapikeys.SetAPIKey(adminAPIKey)
		storageprofiles.SetAPIKey(adminAPIKey)
		bucketprefixes.SetAPIKey(adminAPIKey)
		logs.SetAPIKey(adminAPIKey)
		traces.SetAPIKey(adminAPIKey)
		estimates.SetAPIKey(adminAPIKey)
	}

	// Add API key flag to admin command and all subcommands
	adminCmd.PersistentFlags().StringVar(&adminAPIKey, "api-key", "", "Admin API key (can also be set via ADMIN_API_KEY environment variable)")

	rootCmd.AddCommand(adminCmd)
}

func getObjCleanupCmd() *cobra.Command {
	objCleanupCmd := &cobra.Command{
		Use:   "objcleanup",
		Short: "Object cleanup administrative commands",
	}

	objCleanupCmd.AddCommand(objcleanup.GetStatusCmd())
	return objCleanupCmd
}

func getLogsCmd() *cobra.Command {
	logsCmd := &cobra.Command{
		Use:   "logs",
		Short: "Log administrative commands",
	}

	logsCmd.AddCommand(logs.GetCompactCmd())
	return logsCmd
}

func getTracesCmd() *cobra.Command {
	tracesCmd := &cobra.Command{
		Use:   "traces",
		Short: "Trace administrative commands",
	}

	tracesCmd.AddCommand(traces.GetCompactCmd())
	return tracesCmd
}

func getMetricsCmd() *cobra.Command {
	metricsCmd := &cobra.Command{
		Use:   "metrics",
		Short: "Metric administrative commands",
	}

	metricsCmd.AddCommand(metrics.GetEstimatesCmd())
	return metricsCmd
}
