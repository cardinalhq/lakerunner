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
	bucketName    string
	instanceNum   int32
	collectorName string
)

func getBucketsCmd() *cobra.Command {
	bucketsCmd := &cobra.Command{
		Use:   "buckets",
		Short: "Manage organization bucket associations",
	}

	// List buckets
	listCmd := &cobra.Command{
		Use:   "list <organization-id>",
		Short: "List buckets for an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			orgID := args[0]
			return runListBuckets(orgID)
		},
	}
	bucketsCmd.AddCommand(listCmd)

	// Add bucket
	addCmd := &cobra.Command{
		Use:   "add <organization-id>",
		Short: "Add a bucket to an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			orgID := args[0]
			return runAddBucket(orgID)
		},
	}
	addCmd.Flags().StringVar(&bucketName, "bucket-name", "", "Bucket name (required)")
	addCmd.Flags().Int32Var(&instanceNum, "instance-num", 0, "Instance number")
	addCmd.Flags().StringVar(&collectorName, "collector-name", "", "Collector name")
	addCmd.MarkFlagRequired("bucket-name")
	bucketsCmd.AddCommand(addCmd)

	// Delete bucket
	deleteCmd := &cobra.Command{
		Use:   "delete <organization-id>",
		Short: "Remove a bucket from an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			orgID := args[0]
			return runDeleteBucket(orgID)
		},
	}
	deleteCmd.Flags().StringVar(&bucketName, "bucket-name", "", "Bucket name (required)")
	deleteCmd.Flags().Int32Var(&instanceNum, "instance-num", 0, "Instance number")
	deleteCmd.Flags().StringVar(&collectorName, "collector-name", "", "Collector name")
	deleteCmd.MarkFlagRequired("bucket-name")
	bucketsCmd.AddCommand(deleteCmd)

	return bucketsCmd
}

func runListBuckets(orgID string) error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for buckets")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.ListOrganizationBuckets(ctx, &adminproto.ListOrganizationBucketsRequest{
		OrganizationId: orgID,
	})
	if err != nil {
		return fmt.Errorf("failed to list buckets: %w", err)
	}

	if len(resp.Buckets) == 0 {
		fmt.Println("No buckets found for organization", orgID)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET_NAME\tINSTANCE_NUM\tCOLLECTOR_NAME")
	for _, bucket := range resp.Buckets {
		fmt.Fprintf(w, "%s\t%d\t%s\n", bucket.BucketName, bucket.InstanceNum, bucket.CollectorName)
	}
	w.Flush()

	return nil
}

func runAddBucket(orgID string) error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for buckets")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.AddOrganizationBucket(ctx, &adminproto.AddOrganizationBucketRequest{
		OrganizationId: orgID,
		BucketName:     bucketName,
		InstanceNum:    instanceNum,
		CollectorName:  collectorName,
	})
	if err != nil {
		return fmt.Errorf("failed to add bucket: %w", err)
	}

	fmt.Printf("Added bucket to organization:\n")
	fmt.Printf("  Organization ID: %s\n", resp.Bucket.OrganizationId)
	fmt.Printf("  Bucket Name: %s\n", resp.Bucket.BucketName)
	fmt.Printf("  Instance Num: %d\n", resp.Bucket.InstanceNum)
	if resp.Bucket.CollectorName != "" {
		fmt.Printf("  Collector Name: %s\n", resp.Bucket.CollectorName)
	}

	return nil
}

func runDeleteBucket(orgID string) error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for buckets")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	_, err = client.DeleteOrganizationBucket(ctx, &adminproto.DeleteOrganizationBucketRequest{
		OrganizationId: orgID,
		BucketName:     bucketName,
		InstanceNum:    instanceNum,
		CollectorName:  collectorName,
	})
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	fmt.Printf("Removed bucket %s from organization %s\n", bucketName, orgID)
	return nil
}
