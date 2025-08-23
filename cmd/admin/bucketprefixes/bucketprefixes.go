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

package bucketprefixes

import (
	"fmt"

	"github.com/spf13/cobra"
)

var apiKey string

// SetAPIKey configures the API key used for auth with the admin service.
func SetAPIKey(key string) {
	apiKey = key
}

// GetBucketPrefixesCmd provides commands for managing bucket prefix mappings.
func GetBucketPrefixesCmd() *cobra.Command {
	bpCmd := &cobra.Command{
		Use:   "bucket-prefixes",
		Short: "Manage bucket prefix mappings",
	}

	bpCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List bucket prefix mappings",
		RunE:  notImplemented,
	})

	bpCmd.AddCommand(&cobra.Command{
		Use:   "create",
		Short: "Create a bucket prefix mapping",
		RunE:  notImplemented,
	})

	bpCmd.AddCommand(&cobra.Command{
		Use:   "delete",
		Short: "Delete a bucket prefix mapping",
		RunE:  notImplemented,
	})

	return bpCmd
}

func notImplemented(_ *cobra.Command, _ []string) error {
	return fmt.Errorf("not implemented")
}
