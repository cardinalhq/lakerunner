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

package bucketprefixes

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/configdb"
)

var (
	apiKey    string
	adminAddr string
	useLocal  bool

	createSignal string
)

// SetAPIKey configures the API key used for auth with the admin service.
// Currently a no-op until remote operations are implemented.
func SetAPIKey(key string) {
	apiKey = key
}

// GetBucketPrefixesCmd provides commands for managing bucket prefix mappings.
func GetBucketPrefixesCmd() *cobra.Command {
	bpCmd := &cobra.Command{
		Use:   "bucket-prefixes",
		Short: "Manage bucket prefix mappings",
	}

	bpCmd.PersistentFlags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	bpCmd.PersistentFlags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List bucket prefix mappings",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runBucketPrefixesList()
		},
	}
	bpCmd.AddCommand(listCmd)

	createCmd := &cobra.Command{
		Use:   "create <bucket> <org-id> <prefix>",
		Short: "Create a bucket prefix mapping",
		Args:  cobra.ExactArgs(3),
		RunE: func(_ *cobra.Command, args []string) error {
			bucket := args[0]
			orgID := args[1]
			prefix := args[2]
			return runBucketPrefixesCreate(bucket, orgID, prefix, createSignal)
		},
	}
	createCmd.Flags().StringVar(&createSignal, "signal", "metrics", "Signal type (metrics|logs|traces)")
	bpCmd.AddCommand(createCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <mapping-id>",
		Short: "Delete a bucket prefix mapping",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			return runBucketPrefixesDelete(id)
		},
	}
	bpCmd.AddCommand(deleteCmd)

	return bpCmd
}

func runBucketPrefixesList() error {
	if useLocal {
		return runLocalBucketPrefixesList()
	}
	_ = apiKey
	return fmt.Errorf("remote bucket prefix operations not implemented")
}

func runLocalBucketPrefixesList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	mappings, err := store.ListBucketPrefixMappings(ctx)
	if err != nil {
		return fmt.Errorf("failed to list bucket prefix mappings: %w", err)
	}

	if len(mappings) == 0 {
		fmt.Println("No bucket prefix mappings found")
		return nil
	}

	printBucketPrefixMappingsTable(mappings)
	return nil
}

func runBucketPrefixesCreate(bucket, orgID, prefix, signal string) error {
	if useLocal {
		return runLocalBucketPrefixesCreate(bucket, orgID, prefix, signal)
	}
	_ = apiKey
	return fmt.Errorf("remote bucket prefix operations not implemented")
}

func runLocalBucketPrefixesCreate(bucket, orgID, prefix, signal string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	bucketCfg, err := store.GetBucketConfiguration(ctx, bucket)
	if err != nil {
		return fmt.Errorf("failed to get bucket configuration: %w", err)
	}

	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	_, err = store.CreateBucketPrefixMapping(ctx, configdb.CreateBucketPrefixMappingParams{
		BucketID:       bucketCfg.ID,
		OrganizationID: orgUUID,
		PathPrefix:     prefix,
		Signal:         signal,
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket prefix mapping: %w", err)
	}

	fmt.Printf("Created bucket prefix mapping for bucket %s and organization %s\n", bucket, orgID)
	return nil
}

func runBucketPrefixesDelete(id string) error {
	if useLocal {
		return runLocalBucketPrefixesDelete(id)
	}
	_ = apiKey
	return fmt.Errorf("remote bucket prefix operations not implemented")
}

func runLocalBucketPrefixesDelete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	uuidVal, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("invalid mapping ID: %w", err)
	}

	if err := store.DeleteBucketPrefixMapping(ctx, uuidVal); err != nil {
		return fmt.Errorf("failed to delete bucket prefix mapping: %w", err)
	}

	fmt.Printf("Deleted bucket prefix mapping %s\n", id)
	return nil
}

func printBucketPrefixMappingsTable(mappings []configdb.ListBucketPrefixMappingsRow) {
	headers := []string{"ID", "Bucket", "Organization", "Prefix", "Signal"}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}

	for _, m := range mappings {
		fields := []string{
			m.ID.String(),
			m.BucketName,
			m.OrganizationID.String(),
			m.PathPrefix,
			m.Signal,
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

	fmt.Printf("│ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │\n",
		widths[0], headers[0],
		widths[1], headers[1],
		widths[2], headers[2],
		widths[3], headers[3],
		widths[4], headers[4],
	)

	fmt.Print("├")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┤")

	for _, m := range mappings {
		fmt.Printf("│ %-*s │ %-*s │ %-*s │ %-*s │ %-*s │\n",
			widths[0], m.ID.String(),
			widths[1], m.BucketName,
			widths[2], m.OrganizationID.String(),
			widths[3], m.PathPrefix,
			widths[4], m.Signal,
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
