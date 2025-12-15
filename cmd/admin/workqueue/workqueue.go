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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
)

var (
	apiKey        string
	endpoint      string
	insecureMode  bool
	tlsSkipVerify bool
	tlsCACert     string
)

// SetAPIKey configures the API key used for auth with the admin service.
func SetAPIKey(key string) {
	apiKey = key
}

// SetConnectionConfig configures the endpoint and TLS settings.
func SetConnectionConfig(ep string, insec, skipVerify bool, caCert string) {
	endpoint = ep
	insecureMode = insec
	tlsSkipVerify = skipVerify
	tlsCACert = caCert
}

// GetWorkQueueCmd provides work queue administrative commands.
func GetWorkQueueCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workqueue",
		Short: "Work queue administrative commands",
	}

	cmd.AddCommand(getStatusCmd())
	return cmd
}

func getStatusCmd() *cobra.Command {
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show work queue status by task type",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			client, cleanup, err := createAdminClient()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx = createAuthContext(ctx)

			resp, err := client.GetWorkQueueStatus(ctx, &adminproto.GetWorkQueueStatusRequest{})
			if err != nil {
				return fmt.Errorf("failed to get work queue status: %w", err)
			}

			if len(resp.Tasks) == 0 {
				fmt.Println("Work queue is empty")
				return nil
			}

			if jsonOutput {
				return printStatusJSON(resp.Tasks)
			}
			return printStatusTable(resp.Tasks)
		},
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	return cmd
}

func createAdminClient() (adminproto.AdminServiceClient, func(), error) {
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

func createAuthContext(ctx context.Context) context.Context {
	if apiKey != "" {
		md := metadata.New(map[string]string{"authorization": "Bearer " + apiKey})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func printStatusTable(tasks []*adminproto.WorkQueueTaskStatus) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "TASK\tPENDING\tIN_PROGRESS\tFAILED\tWORKERS"); err != nil {
		return err
	}
	for _, task := range tasks {
		if _, err := fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n",
			task.TaskName, task.Pending, task.InProgress, task.Failed, task.Workers); err != nil {
			return err
		}
	}
	return w.Flush()
}

func printStatusJSON(tasks []*adminproto.WorkQueueTaskStatus) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(tasks)
}
