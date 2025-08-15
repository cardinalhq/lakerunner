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

package debug

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slog"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
)

func GetS3LSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "s3ls",
		Short: "List a bucket prefix in S3",
		RunE: func(c *cobra.Command, _ []string) error {
			bucketID, err := c.Flags().GetString("bucket")
			if err != nil {
				return fmt.Errorf("failed to get bucket flag: %w", err)
			}
			prefix, err := c.Flags().GetString("prefix")
			if err != nil {
				return fmt.Errorf("failed to get prefix flag: %w", err)
			}
			region, err := c.Flags().GetString("region")
			if err != nil {
				return fmt.Errorf("failed to get region flag: %w", err)
			}
			role, err := c.Flags().GetString("role")
			if err != nil {
				return fmt.Errorf("failed to get role flag: %w", err)
			}

			return runS3LS(bucketID, prefix, region, role)
		},
	}

	cmd.Flags().String("bucket", "", "S3 bucket to list")
	if err := cmd.MarkFlagRequired("bucket"); err != nil {
		panic(fmt.Errorf("failed to mark bucket flag as required: %w", err))
	}

	cmd.Flags().String("prefix", "", "S3 prefix to list")
	if err := cmd.MarkFlagRequired("prefix"); err != nil {
		panic(fmt.Errorf("failed to mark prefix flag as required: %w", err))
	}

	cmd.Flags().String("region", "us-east-2", "AWS region of the S3 bucket")
	if err := cmd.MarkFlagRequired("region"); err != nil {
		panic(fmt.Errorf("failed to mark region flag as required: %w", err))
	}

	cmd.Flags().String("role", "", "AWS IAM role to assume for S3 access")

	return cmd
}

func runS3LS(bucketID string, prefix string, region string, role string) error {
	ctx := context.Background()

	// Initialize AWS S3 client
	mgr, err := awsclient.NewManager(ctx,
		awsclient.WithAssumeRoleSessionName("lakerunner-import"),
	)
	if err != nil {
		return err
	}

	var opts []awsclient.S3Option
	if role != "" {
		opts = append(opts, awsclient.WithRole(role))
	}
	if region != "" {
		opts = append(opts, awsclient.WithRegion(region))
	}
	s3client, err := mgr.GetS3(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	// List objects in the specified S3 bucket and prefix
	err = listS3Objects(ctx, s3client.Client, bucketID, prefix)
	if err != nil {
		return err
	}

	return nil
}

// listS3Objects returns all object keys under the given prefix.
// It logs any paging/list errors and bubbles them up.
func listS3Objects(ctx context.Context, s3client *s3.Client, bucketID, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(s3client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketID),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			slog.Error("Failed to list S3 objects",
				slog.String("bucket", bucketID),
				slog.String("prefix", prefix),
				slog.Any("error", err),
			)
			return err
		}

		for _, obj := range page.Contents {
			fmt.Println(aws.ToString(obj.Key))
		}
	}

	return nil
}
