// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package adminapikeys

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

// GetAdminAPIKeysCmd provides commands for managing admin API keys.
func GetAdminAPIKeysCmd() *cobra.Command {
	keysCmd := &cobra.Command{
		Use:   "admin-apikeys",
		Short: "Manage admin API keys",
	}

	keysCmd.PersistentFlags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	keysCmd.PersistentFlags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List admin API keys",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAdminAPIKeysList()
		},
	}
	keysCmd.AddCommand(listCmd)

	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create an admin API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			return runAdminAPIKeysCreate(name, createDescription)
		},
	}
	createCmd.Flags().StringVar(&createDescription, "description", "", "Description for the API key")
	keysCmd.AddCommand(createCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <api-key-id>",
		Short: "Delete an admin API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			return runAdminAPIKeysDelete(id)
		},
	}
	keysCmd.AddCommand(deleteCmd)

	return keysCmd
}

func runAdminAPIKeysList() error {
	if useLocal {
		return runLocalAdminAPIKeysList()
	}
	_ = apiKey
	return fmt.Errorf("remote admin API key operations not implemented")
}

func runLocalAdminAPIKeysList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	keys, err := store.GetAllAdminAPIKeys(ctx)
	if err != nil {
		return fmt.Errorf("failed to list admin API keys: %w", err)
	}

	if len(keys) == 0 {
		fmt.Println("No admin API keys found")
		return nil
	}

	printAdminAPIKeysTable(keys)
	return nil
}

func runAdminAPIKeysCreate(name, description string) error {
	if useLocal {
		return runLocalAdminAPIKeysCreate(name, description)
	}
	_ = apiKey
	return fmt.Errorf("remote admin API key operations not implemented")
}

func runLocalAdminAPIKeysCreate(name, description string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
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

	row, err := store.CreateAdminAPIKey(ctx, configdb.CreateAdminAPIKeyParams{
		KeyHash:     keyHash,
		Name:        name,
		Description: descPtr,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin API key: %w", err)
	}

	fmt.Printf("Created API key %s: %s\n", row.ID, apiKeyValue)
	return nil
}

func runAdminAPIKeysDelete(id string) error {
	if useLocal {
		return runLocalAdminAPIKeysDelete(id)
	}
	_ = apiKey
	return fmt.Errorf("remote admin API key operations not implemented")
}

func runLocalAdminAPIKeysDelete(id string) error {
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

	if err := store.DeleteAdminAPIKey(ctx, apiKeyID); err != nil {
		return fmt.Errorf("failed to delete admin API key: %w", err)
	}

	fmt.Printf("Deleted admin API key %s\n", id)
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

func printAdminAPIKeysTable(keys []configdb.AdminApiKey) {
	headers := []string{"ID", "Name"}
	widths := []int{len(headers[0]), len(headers[1])}

	for _, k := range keys {
		if l := len(k.ID.String()); l > widths[0] {
			widths[0] = l
		}
		if l := len(k.Name); l > widths[1] {
			widths[1] = l
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

	fmt.Printf("│ %-*s │ %-*s │\n",
		widths[0], headers[0],
		widths[1], headers[1],
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
		fmt.Printf("│ %-*s │ %-*s │\n",
			widths[0], k.ID.String(),
			widths[1], k.Name,
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
