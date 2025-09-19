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

package organizations

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
)

var (
	cloudProvider string
	region        string
	endpoint      string
	role          string
	usePathStyle  bool
	insecureTLS   bool
)

func getBucketConfigsCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "bucket-configs",
		Short: "Manage bucket configurations",
	}

	// List bucket configurations
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all bucket configurations",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListBucketConfigs()
		},
	}
	configCmd.AddCommand(listCmd)

	// Create bucket configuration
	createCmd := &cobra.Command{
		Use:   "create <bucket-name>",
		Short: "Create a new bucket configuration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucketName = args[0]
			return runCreateBucketConfig()
		},
	}
	createCmd.Flags().StringVar(&cloudProvider, "cloud-provider", "", "Cloud provider (aws/gcp/azure)")
	createCmd.Flags().StringVar(&region, "region", "", "Region")
	createCmd.Flags().StringVar(&endpoint, "endpoint", "", "Custom endpoint")
	createCmd.Flags().StringVar(&role, "role", "", "IAM role")
	createCmd.Flags().BoolVar(&usePathStyle, "use-path-style", false, "Use path-style S3 URLs")
	createCmd.Flags().BoolVar(&insecureTLS, "insecure-tls", false, "Allow insecure TLS connections")
	configCmd.AddCommand(createCmd)

	// Delete bucket configuration
	deleteCmd := &cobra.Command{
		Use:   "delete <bucket-name>",
		Short: "Delete a bucket configuration",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucketName = args[0]
			return runDeleteBucketConfig()
		},
	}
	configCmd.AddCommand(deleteCmd)

	return configCmd
}

func runListBucketConfigs() error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for bucket configs")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.ListBucketConfigurations(ctx, &adminproto.ListBucketConfigurationsRequest{})
	if err != nil {
		return fmt.Errorf("failed to list bucket configurations: %w", err)
	}

	if len(resp.Configurations) == 0 {
		fmt.Println("No bucket configurations found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET_NAME\tPROVIDER\tREGION\tENDPOINT\tROLE\tPATH_STYLE\tINSECURE_TLS")
	for _, cfg := range resp.Configurations {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%t\t%t\n",
			cfg.BucketName, cfg.CloudProvider, cfg.Region,
			cfg.Endpoint, cfg.Role, cfg.UsePathStyle, cfg.InsecureTls)
	}
	w.Flush()

	return nil
}

func runCreateBucketConfig() error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for bucket configs")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.CreateBucketConfiguration(ctx, &adminproto.CreateBucketConfigurationRequest{
		BucketName:    bucketName,
		CloudProvider: cloudProvider,
		Region:        region,
		Endpoint:      endpoint,
		Role:          role,
		UsePathStyle:  usePathStyle,
		InsecureTls:   insecureTLS,
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket configuration: %w", err)
	}

	fmt.Printf("Created bucket configuration:\n")
	fmt.Printf("  Bucket Name: %s\n", resp.Configuration.BucketName)
	if resp.Configuration.CloudProvider != "" {
		fmt.Printf("  Cloud Provider: %s\n", resp.Configuration.CloudProvider)
	}
	if resp.Configuration.Region != "" {
		fmt.Printf("  Region: %s\n", resp.Configuration.Region)
	}
	if resp.Configuration.Endpoint != "" {
		fmt.Printf("  Endpoint: %s\n", resp.Configuration.Endpoint)
	}
	if resp.Configuration.Role != "" {
		fmt.Printf("  Role: %s\n", resp.Configuration.Role)
	}
	fmt.Printf("  Path Style: %t\n", resp.Configuration.UsePathStyle)
	fmt.Printf("  Insecure TLS: %t\n", resp.Configuration.InsecureTls)

	return nil
}

func runDeleteBucketConfig() error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for bucket configs")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	_, err = client.DeleteBucketConfiguration(ctx, &adminproto.DeleteBucketConfigurationRequest{
		BucketName: bucketName,
	})
	if err != nil {
		return fmt.Errorf("failed to delete bucket configuration: %w", err)
	}

	fmt.Printf("Deleted bucket configuration for %s\n", bucketName)
	return nil
}
