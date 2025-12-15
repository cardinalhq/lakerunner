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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/cmd/admin/kafka"
	"github.com/cardinalhq/lakerunner/cmd/admin/organizations"
	"github.com/cardinalhq/lakerunner/cmd/admin/workqueue"
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
	rootCmd.PersistentFlags().BoolVar(&insecureMode, "insecure", false, "Disable TLS (not recommended)")
	rootCmd.PersistentFlags().BoolVar(&tlsSkipVerify, "tls-skip-verify", false, "Skip TLS certificate verification")
	rootCmd.PersistentFlags().StringVar(&tlsCACert, "tls-ca-cert", "", "Path to CA certificate for TLS verification")

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if adminAPIKey == "" {
			adminAPIKey = os.Getenv("LAKERUNNER_ADMIN_API_KEY")
		}
		if endpoint == "" {
			endpoint = os.Getenv("LAKERUNNER_ADMIN_API_ENDPOINT")
		}
		if endpoint == "" {
			return fmt.Errorf("endpoint required: set --endpoint flag or LAKERUNNER_ADMIN_API_ENDPOINT environment variable")
		}
		organizations.SetAPIKey(adminAPIKey)
		organizations.SetConnectionConfig(endpoint, insecureMode, tlsSkipVerify, tlsCACert)
		kafka.SetAPIKey(adminAPIKey)
		kafka.SetConnectionConfig(endpoint, insecureMode, tlsSkipVerify, tlsCACert)
		workqueue.SetAPIKey(adminAPIKey)
		workqueue.SetConnectionConfig(endpoint, insecureMode, tlsSkipVerify, tlsCACert)
		return nil
	}

	rootCmd.AddCommand(getPingCmd())
	rootCmd.AddCommand(organizations.GetOrganizationsCmd())
	rootCmd.AddCommand(kafka.GetKafkaCmd())
	rootCmd.AddCommand(workqueue.GetWorkQueueCmd())
}

func getPingCmd() *cobra.Command {
	pingCmd := &cobra.Command{
		Use:   "ping",
		Short: "Ping the admin service",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			client, cleanup, err := CreateAdminClient()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx = AttachAPIKey(ctx)
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

// AttachAPIKey adds the API key to the context for gRPC calls.
func AttachAPIKey(ctx context.Context) context.Context {
	if adminAPIKey != "" {
		md := metadata.New(map[string]string{"authorization": "Bearer " + adminAPIKey})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// CreateAdminClient creates a gRPC client for the admin service with TLS support.
func CreateAdminClient() (adminproto.AdminServiceClient, func(), error) {
	var opts []grpc.DialOption

	if insecureMode {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if tlsSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}

		if tlsCACert != "" {
			caCert, err := os.ReadFile(tlsCACert)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read CA certificate: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to admin service: %w", err)
	}

	cleanup := func() { _ = conn.Close() }
	client := adminproto.NewAdminServiceClient(conn)
	return client, cleanup, nil
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
