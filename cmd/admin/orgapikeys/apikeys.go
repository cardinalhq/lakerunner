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

package orgapikeys

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/configdb"
)

var (
	apiKey            string
	adminAddr         string
	useLocal          bool
	createDescription string
)

// SetAPIKey configures the API key used for auth with the admin service.
func SetAPIKey(key string) {
	apiKey = key
}

// GetOrgAPIKeysCmd provides commands for managing organization API keys.
func GetOrgAPIKeysCmd() *cobra.Command {
	keysCmd := &cobra.Command{
		Use:   "org-apikeys",
		Short: "Manage organization API keys",
	}

	keysCmd.PersistentFlags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	keysCmd.PersistentFlags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List organization API keys",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runOrgAPIKeysList()
		},
	}
	keysCmd.AddCommand(listCmd)

	createCmd := &cobra.Command{
		Use:   "create <org-id> <name>",
		Short: "Create an organization API key",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			orgID := args[0]
			name := args[1]
			return runOrgAPIKeysCreate(orgID, name, createDescription)
		},
	}
	createCmd.Flags().StringVar(&createDescription, "description", "", "Description for the API key")
	keysCmd.AddCommand(createCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <api-key-id>",
		Short: "Delete an organization API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			return runOrgAPIKeysDelete(id)
		},
	}
	keysCmd.AddCommand(deleteCmd)

	return keysCmd
}

func runOrgAPIKeysList() error {
	if useLocal {
		return runLocalOrgAPIKeysList()
	}
	_ = apiKey
	return fmt.Errorf("remote organization API key operations not implemented")
}

func runLocalOrgAPIKeysList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	keys, err := store.GetAllOrganizationAPIKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to list organization API keys: %w", err)
	}

	if len(keys) == 0 {
		fmt.Println("No organization API keys found")
		return nil
	}

	printOrgAPIKeysTable(keys)
	return nil
}

func runOrgAPIKeysCreate(orgID, name, description string) error {
	if useLocal {
		return runLocalOrgAPIKeysCreate(orgID, name, description)
	}
	_ = apiKey
	return fmt.Errorf("remote organization API key operations not implemented")
}

func runLocalOrgAPIKeysCreate(orgID, name, description string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	apiKeyValue, err := generateAPIKey()
	if err != nil {
		return fmt.Errorf("failed to generate API key: %w", err)
	}
	keyHash := hashAPIKey(apiKeyValue)

	var descPtr *string
	if description != "" {
		descPtr = &description
	}

	row, err := store.CreateOrganizationAPIKey(ctx, configdb.CreateOrganizationAPIKeyParams{
		KeyHash:     keyHash,
		Name:        name,
		Description: descPtr,
	})
	if err != nil {
		return fmt.Errorf("failed to create organization API key: %w", err)
	}

	_, err = store.CreateOrganizationAPIKeyMapping(ctx, configdb.CreateOrganizationAPIKeyMappingParams{
		ApiKeyID:       row.ID,
		OrganizationID: orgUUID,
	})
	if err != nil {
		return fmt.Errorf("failed to create organization API key mapping: %w", err)
	}

	fmt.Printf("Created API key %s for organization %s: %s\n", row.ID, orgID, apiKeyValue)
	return nil
}

func runOrgAPIKeysDelete(id string) error {
	if useLocal {
		return runLocalOrgAPIKeysDelete(id)
	}
	_ = apiKey
	return fmt.Errorf("remote organization API key operations not implemented")
}

func runLocalOrgAPIKeysDelete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	apiKeyID, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("invalid API key ID: %w", err)
	}

	if err := store.DeleteOrganizationAPIKey(ctx, apiKeyID); err != nil {
		return fmt.Errorf("failed to delete organization API key: %w", err)
	}

	fmt.Printf("Deleted organization API key %s\n", id)
	return nil
}

func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", h)
}

func printOrgAPIKeysTable(keys []configdb.GetAllOrganizationAPIKeysRow) {
	headers := []string{"ID", "Name", "Organization"}
	widths := []int{len(headers[0]), len(headers[1]), len(headers[2])}

	for _, k := range keys {
		if l := len(k.ID.String()); l > widths[0] {
			widths[0] = l
		}
		if l := len(k.Name); l > widths[1] {
			widths[1] = l
		}
		if l := len(k.OrganizationID.String()); l > widths[2] {
			widths[2] = l
		}
	}

	fmt.Print("┌")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │ %-*s │\n",
		widths[0], headers[0],
		widths[1], headers[1],
		widths[2], headers[2],
	)

	fmt.Print("├")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┤")

	for _, k := range keys {
		fmt.Printf("│ %-*s │ %-*s │ %-*s │\n",
			widths[0], k.ID.String(),
			widths[1], k.Name,
			widths[2], k.OrganizationID.String(),
		)
	}

	fmt.Print("└")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┘")
}
