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
	apiKeyName        string
	apiKeyDescription string
	apiKeyID          string
)

func getAPIKeysCmd() *cobra.Command {
	apiKeysCmd := &cobra.Command{
		Use:   "apikeys",
		Short: "Manage organization API keys",
	}

	// List API keys
	listCmd := &cobra.Command{
		Use:   "list <organization-id>",
		Short: "List API keys for an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			orgID := args[0]
			return runListAPIKeys(orgID)
		},
	}
	apiKeysCmd.AddCommand(listCmd)

	// Create API key
	createCmd := &cobra.Command{
		Use:   "create <organization-id>",
		Short: "Create a new API key for an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			orgID := args[0]
			return runCreateAPIKey(orgID)
		},
	}
	createCmd.Flags().StringVar(&apiKeyName, "name", "", "API key name (required)")
	createCmd.Flags().StringVar(&apiKeyDescription, "description", "", "API key description")
	createCmd.MarkFlagRequired("name")
	apiKeysCmd.AddCommand(createCmd)

	// Delete API key
	deleteCmd := &cobra.Command{
		Use:   "delete <api-key-id>",
		Short: "Delete an API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			keyID := args[0]
			return runDeleteAPIKey(keyID)
		},
	}
	apiKeysCmd.AddCommand(deleteCmd)

	return apiKeysCmd
}

func runListAPIKeys(orgID string) error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for API keys")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.ListOrganizationAPIKeys(ctx, &adminproto.ListOrganizationAPIKeysRequest{
		OrganizationId: orgID,
	})
	if err != nil {
		return fmt.Errorf("failed to list API keys: %w", err)
	}

	if len(resp.ApiKeys) == 0 {
		fmt.Println("No API keys found for organization", orgID)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tDESCRIPTION\tKEY_PREVIEW")
	for _, key := range resp.ApiKeys {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s...\n", key.Id, key.Name, key.Description, key.KeyPreview)
	}
	w.Flush()

	return nil
}

func runCreateAPIKey(orgID string) error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for API keys")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.CreateOrganizationAPIKey(ctx, &adminproto.CreateOrganizationAPIKeyRequest{
		OrganizationId: orgID,
		Name:           apiKeyName,
		Description:    apiKeyDescription,
	})
	if err != nil {
		return fmt.Errorf("failed to create API key: %w", err)
	}

	fmt.Printf("Created API key:\n")
	fmt.Printf("  ID: %s\n", resp.ApiKey.Id)
	fmt.Printf("  Name: %s\n", resp.ApiKey.Name)
	if resp.ApiKey.Description != "" {
		fmt.Printf("  Description: %s\n", resp.ApiKey.Description)
	}
	fmt.Printf("\n")
	fmt.Printf("API Key (save this - it won't be shown again):\n")
	fmt.Printf("  %s\n", resp.FullKey)

	return nil
}

func runDeleteAPIKey(keyID string) error {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+apiKey)
	}

	if useLocal {
		// Local implementation would go here
		return fmt.Errorf("local mode not implemented for API keys")
	}

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	_, err = client.DeleteOrganizationAPIKey(ctx, &adminproto.DeleteOrganizationAPIKeyRequest{
		Id: keyID,
	})
	if err != nil {
		return fmt.Errorf("failed to delete API key: %w", err)
	}

	fmt.Printf("Deleted API key %s\n", keyID)
	return nil
}
