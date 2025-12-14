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
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
)

var (
	configFieldName string
)

func getConfigCmd() *cobra.Command {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Manage organization configuration",
	}

	configCmd.AddCommand(getLogStreamConfigCmd())

	return configCmd
}

func getLogStreamConfigCmd() *cobra.Command {
	logStreamCmd := &cobra.Command{
		Use:   "log-stream",
		Short: "Manage log stream field configuration",
	}

	// Get log stream config
	getCmd := &cobra.Command{
		Use:   "get <organization-id>",
		Short: "Get log stream field configuration for an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			orgID := args[0]
			return runGetLogStreamConfig(orgID)
		},
	}
	logStreamCmd.AddCommand(getCmd)

	// Set log stream config
	setCmd := &cobra.Command{
		Use:   "set <organization-id>",
		Short: "Set log stream field configuration for an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			orgID := args[0]
			return runSetLogStreamConfig(orgID)
		},
	}
	setCmd.Flags().StringVar(&configFieldName, "field", "", "Field name for stream identification (required)")
	_ = setCmd.MarkFlagRequired("field")
	logStreamCmd.AddCommand(setCmd)

	// Delete log stream config
	deleteCmd := &cobra.Command{
		Use:   "delete <organization-id>",
		Short: "Delete log stream field configuration (reverts to system default)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			orgID := args[0]
			return runDeleteLogStreamConfig(orgID)
		},
	}
	logStreamCmd.AddCommand(deleteCmd)

	return logStreamCmd
}

func runGetLogStreamConfig(orgID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	resp, err := client.GetLogStreamConfig(ctx, &adminproto.GetLogStreamConfigRequest{
		OrganizationId: orgID,
	})
	if err != nil {
		return fmt.Errorf("failed to get log stream config: %w", err)
	}

	source := "organization-specific"
	if resp.IsDefault {
		source = "system default"
	}

	fmt.Printf("Organization: %s\n", orgID)
	fmt.Printf("Field:        %s\n", resp.Config.FieldName)
	fmt.Printf("Source:       %s\n", source)
	return nil
}

func runSetLogStreamConfig(orgID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	resp, err := client.SetLogStreamConfig(ctx, &adminproto.SetLogStreamConfigRequest{
		OrganizationId: orgID,
		FieldName:      configFieldName,
	})
	if err != nil {
		return fmt.Errorf("failed to set log stream config: %w", err)
	}

	fmt.Printf("Set log stream field for organization %s to: %s\n", orgID, resp.Config.FieldName)
	return nil
}

func runDeleteLogStreamConfig(orgID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	_, err = client.DeleteLogStreamConfig(ctx, &adminproto.DeleteLogStreamConfigRequest{
		OrganizationId: orgID,
	})
	if err != nil {
		return fmt.Errorf("failed to delete log stream config: %w", err)
	}

	fmt.Printf("Deleted log stream config for organization %s (will use system default)\n", orgID)
	return nil
}
