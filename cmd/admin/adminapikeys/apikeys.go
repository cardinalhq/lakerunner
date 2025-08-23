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

package adminapikeys

import (
	"fmt"

	"github.com/spf13/cobra"
)

var apiKey string

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

	keysCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List admin API keys",
		RunE:  notImplemented,
	})

	keysCmd.AddCommand(&cobra.Command{
		Use:   "create",
		Short: "Create an admin API key",
		RunE:  notImplemented,
	})

	keysCmd.AddCommand(&cobra.Command{
		Use:   "delete",
		Short: "Delete an admin API key",
		RunE:  notImplemented,
	})

	return keysCmd
}

func notImplemented(_ *cobra.Command, _ []string) error {
	return fmt.Errorf("not implemented")
}
