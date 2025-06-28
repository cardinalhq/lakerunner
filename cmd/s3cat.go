// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
)

func init() {
	cmd := &cobra.Command{
		Use:   "s3-cat",
		Short: "return Base64 for an S3 file",
		RunE: func(c *cobra.Command, _ []string) error {
			bucketID, err := c.Flags().GetString("bucket")
			if err != nil {
				return fmt.Errorf("failed to get bucket flag: %w", err)
			}
			objectID, err := c.Flags().GetString("objectid")
			if err != nil {
				return fmt.Errorf("failed to get objectid flag: %w", err)
			}
			region, err := c.Flags().GetString("region")
			if err != nil {
				return fmt.Errorf("failed to get region flag: %w", err)
			}
			role, err := c.Flags().GetString("role")
			if err != nil {
				return fmt.Errorf("failed to get role flag: %w", err)
			}

			return runS3Cat(bucketID, objectID, region, role)
		},
	}

	rootCmd.AddCommand(cmd)

	cmd.Flags().String("bucket", "", "S3 bucket")
	if err := cmd.MarkFlagRequired("bucket"); err != nil {
		panic(fmt.Errorf("failed to mark bucket flag as required: %w", err))
	}

	cmd.Flags().String("objectid", "", "S3 objectid")
	if err := cmd.MarkFlagRequired("objectid"); err != nil {
		panic(fmt.Errorf("failed to mark objectid flag as required: %w", err))
	}

	cmd.Flags().String("region", "us-east-2", "AWS region of the S3 bucket")
	if err := cmd.MarkFlagRequired("region"); err != nil {
		panic(fmt.Errorf("failed to mark region flag as required: %w", err))
	}

	cmd.Flags().String("role", "", "AWS IAM role to assume for S3 access")
}

func runS3Cat(bucketID string, objectID string, region string, role string) error {
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

	tmpdir, err := os.MkdirTemp("", "lakerunner-s3cat")
	if err != nil {
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer os.RemoveAll(tmpdir)

	fn, size, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, bucketID, objectID)
	if err != nil {
		return fmt.Errorf("failed to download S3 object: %w", err)
	}
	fmt.Printf("Downloaded %s (%d bytes) to %s\n", objectID, size, fn)

	// Convert to base64 so we can print it to stdout
	data, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("failed to read downloaded file: %w", err)
	}
	b64 := base64.StdEncoding.EncodeToString(data)
	fmt.Println(b64)

	return nil
}
