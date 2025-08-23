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
	"fmt"

	"github.com/spf13/cobra"
)

var apiKey string

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

	keysCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List organization API keys",
		RunE:  notImplemented,
	})

	keysCmd.AddCommand(&cobra.Command{
		Use:   "create",
		Short: "Create an organization API key",
		RunE:  notImplemented,
	})

	keysCmd.AddCommand(&cobra.Command{
		Use:   "delete",
		Short: "Delete an organization API key",
		RunE:  notImplemented,
	})

	return keysCmd
}

func notImplemented(_ *cobra.Command, _ []string) error {
	return fmt.Errorf("not implemented")
}
