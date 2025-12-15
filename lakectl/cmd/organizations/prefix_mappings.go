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

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/adminclient"
)

var (
	pathPrefix   string
	signal       string
	mappingID    string
	filterBucket string
	filterOrg    string
)

func getPrefixMappingsCmd() *cobra.Command {
	prefixCmd := &cobra.Command{
		Use:   "prefix-mappings",
		Short: "Manage bucket prefix mappings",
	}

	// List prefix mappings
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List bucket prefix mappings",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runListPrefixMappings()
		},
	}
	listCmd.Flags().StringVar(&filterBucket, "bucket", "", "Filter by bucket name")
	listCmd.Flags().StringVar(&filterOrg, "organization", "", "Filter by organization ID")
	prefixCmd.AddCommand(listCmd)

	// Create prefix mapping
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new bucket prefix mapping",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runCreatePrefixMapping()
		},
	}
	createCmd.Flags().StringVar(&bucketName, "bucket-name", "", "Bucket name (required)")
	createCmd.Flags().StringVar(&filterOrg, "organization-id", "", "Organization ID (required)")
	createCmd.Flags().StringVar(&pathPrefix, "path-prefix", "", "Path prefix (required)")
	createCmd.Flags().StringVar(&signal, "signal", "", "Signal type (logs/metrics/traces) (required)")
	_ = createCmd.MarkFlagRequired("bucket-name")
	_ = createCmd.MarkFlagRequired("organization-id")
	_ = createCmd.MarkFlagRequired("path-prefix")
	_ = createCmd.MarkFlagRequired("signal")
	prefixCmd.AddCommand(createCmd)

	// Delete prefix mapping
	deleteCmd := &cobra.Command{
		Use:   "delete <mapping-id>",
		Short: "Delete a bucket prefix mapping",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			mappingID = args[0]
			return runDeletePrefixMapping()
		},
	}
	prefixCmd.AddCommand(deleteCmd)

	return prefixCmd
}

func runListPrefixMappings() error {
	ctx := context.Background()

	if useLocal {
		return fmt.Errorf("local mode not implemented for prefix mappings")
	}

	client, cleanup, err := adminclient.CreateClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx = adminclient.AttachAPIKey(ctx)

	resp, err := client.ListBucketPrefixMappings(ctx, &adminproto.ListBucketPrefixMappingsRequest{
		BucketName:     filterBucket,
		OrganizationId: filterOrg,
	})
	if err != nil {
		return fmt.Errorf("failed to list prefix mappings: %w", err)
	}

	if len(resp.Mappings) == 0 {
		fmt.Println("No prefix mappings found")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tBUCKET_NAME\tORGANIZATION_ID\tPATH_PREFIX\tSIGNAL")
	for _, mapping := range resp.Mappings {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			mapping.Id, mapping.BucketName, mapping.OrganizationId,
			mapping.PathPrefix, mapping.Signal)
	}
	_ = w.Flush()

	return nil
}

func runCreatePrefixMapping() error {
	ctx := context.Background()

	if useLocal {
		return fmt.Errorf("local mode not implemented for prefix mappings")
	}

	client, cleanup, err := adminclient.CreateClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx = adminclient.AttachAPIKey(ctx)

	resp, err := client.CreateBucketPrefixMapping(ctx, &adminproto.CreateBucketPrefixMappingRequest{
		BucketName:     bucketName,
		OrganizationId: filterOrg,
		PathPrefix:     pathPrefix,
		Signal:         signal,
	})
	if err != nil {
		return fmt.Errorf("failed to create prefix mapping: %w", err)
	}

	fmt.Printf("Created prefix mapping:\n")
	fmt.Printf("  ID: %s\n", resp.Mapping.Id)
	fmt.Printf("  Bucket: %s\n", resp.Mapping.BucketName)
	fmt.Printf("  Organization: %s\n", resp.Mapping.OrganizationId)
	fmt.Printf("  Path Prefix: %s\n", resp.Mapping.PathPrefix)
	fmt.Printf("  Signal: %s\n", resp.Mapping.Signal)

	return nil
}

func runDeletePrefixMapping() error {
	ctx := context.Background()

	if useLocal {
		return fmt.Errorf("local mode not implemented for prefix mappings")
	}

	client, cleanup, err := adminclient.CreateClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx = adminclient.AttachAPIKey(ctx)

	_, err = client.DeleteBucketPrefixMapping(ctx, &adminproto.DeleteBucketPrefixMappingRequest{
		Id: mappingID,
	})
	if err != nil {
		return fmt.Errorf("failed to delete prefix mapping: %w", err)
	}

	fmt.Printf("Deleted prefix mapping %s\n", mappingID)
	return nil
}
