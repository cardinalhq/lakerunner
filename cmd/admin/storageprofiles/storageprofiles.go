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

package storageprofiles

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/configdb"
)

var (
	apiKey    string
	adminAddr string
	useLocal  bool

	createProviderType string
	createRegion       string
	createEndpoint     string
	createRole         string
	createUsePathStyle bool
	createInsecureTLS  bool
)

// SetAPIKey configures the API key used for auth with the admin service.
// Currently a no-op until remote operations are implemented.
func SetAPIKey(key string) {
	apiKey = key
}

// GetStorageProfilesCmd provides commands for managing storage profiles.
func GetStorageProfilesCmd() *cobra.Command {
	spCmd := &cobra.Command{
		Use:   "storage-profiles",
		Short: "Manage storage profiles",
	}

	spCmd.PersistentFlags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	spCmd.PersistentFlags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List storage profiles",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runStorageProfilesList()
		},
	}
	spCmd.AddCommand(listCmd)

	createCmd := &cobra.Command{
		Use:   "create <bucket>",
		Short: "Create a storage profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			bucket := args[0]
			return runStorageProfilesCreate(bucket)
		},
	}
	createCmd.Flags().StringVar(&createProviderType, "provider", "", "Provider type (aws, gcp, azure, local)")
	createCmd.Flags().StringVar(&createRegion, "region", "", "Bucket region")
	createCmd.Flags().StringVar(&createEndpoint, "endpoint", "", "Custom endpoint")
	createCmd.Flags().StringVar(&createRole, "role", "", "Access role")
	createCmd.Flags().BoolVar(&createUsePathStyle, "path-style", false, "Use path-style addressing")
	createCmd.Flags().BoolVar(&createInsecureTLS, "insecure-tls", false, "Disable TLS verification")
	spCmd.AddCommand(createCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <bucket>",
		Short: "Delete a storage profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			bucket := args[0]
			return runStorageProfilesDelete(bucket)
		},
	}
	spCmd.AddCommand(deleteCmd)

	return spCmd
}

func runStorageProfilesList() error {
	if useLocal {
		return runLocalStorageProfilesList()
	}
	_ = apiKey
	return fmt.Errorf("remote storage profile operations not implemented")
}

func runLocalStorageProfilesList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	profiles, err := store.ListBucketConfigurations(ctx)
	if err != nil {
		return fmt.Errorf("failed to list storage profiles: %w", err)
	}

	if len(profiles) == 0 {
		fmt.Println("No storage profiles found")
		return nil
	}

	printStorageProfilesTable(profiles)
	return nil
}

func runStorageProfilesCreate(bucket string) error {
	if useLocal {
		return runLocalStorageProfilesCreate(bucket)
	}
	_ = apiKey
	return fmt.Errorf("remote storage profile operations not implemented")
}

func runLocalStorageProfilesCreate(bucket string) error {
	if createProviderType == "" || createRegion == "" {
		return fmt.Errorf("provider type and region are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	params := configdb.CreateBucketConfigurationParams{
		BucketName:    bucket,
		CloudProvider: createProviderType,
		Region:        createRegion,
		UsePathStyle:  createUsePathStyle,
		InsecureTls:   createInsecureTLS,
	}
	if createEndpoint != "" {
		params.Endpoint = &createEndpoint
	}
	if createRole != "" {
		params.Role = &createRole
	}

	_, err = store.CreateBucketConfiguration(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to create storage profile: %w", err)
	}

	fmt.Printf("Created storage profile for bucket %s\n", bucket)
	return nil
}

func runStorageProfilesDelete(bucket string) error {
	if useLocal {
		return runLocalStorageProfilesDelete(bucket)
	}
	_ = apiKey
	return fmt.Errorf("remote storage profile operations not implemented")
}

func runLocalStorageProfilesDelete(bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	if err := store.DeleteBucketConfiguration(ctx, bucket); err != nil {
		return fmt.Errorf("failed to delete storage profile: %w", err)
	}

	fmt.Printf("Deleted storage profile %s\n", bucket)
	return nil
}

func printStorageProfilesTable(profiles []configdb.BucketConfiguration) {
	headers := []string{"Bucket", "Cloud", "Region", "Endpoint", "Role", "PathStyle", "InsecureTLS"}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}

	for _, p := range profiles {
		fields := []string{
			p.BucketName,
			p.ProviderType,
			p.Region,
			valueOrEmpty(p.Endpoint),
			valueOrEmpty(p.Role),
			fmt.Sprintf("%t", p.UsePathStyle),
			fmt.Sprintf("%t", p.InsecureTls),
		}
		for i, f := range fields {
			if l := len(f); l > widths[i] {
				widths[i] = l
			}
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

	fmt.Printf("│ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │\n",
		widths[0], headers[0],
		widths[1], headers[1],
		widths[2], headers[2],
		widths[3], headers[3],
		widths[4], headers[4],
		widths[5], headers[5],
		widths[6], headers[6],
	)

	fmt.Print("├")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┤")

	for _, p := range profiles {
		endpoint := valueOrEmpty(p.Endpoint)
		role := valueOrEmpty(p.Role)
		fmt.Printf("│ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │\n",
			widths[0], p.BucketName,
			widths[1], p.ProviderType,
			widths[2], p.Region,
			widths[3], endpoint,
			widths[4], role,
			widths[5], fmt.Sprintf("%t", p.UsePathStyle),
			widths[6], fmt.Sprintf("%t", p.InsecureTls),
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

func valueOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
