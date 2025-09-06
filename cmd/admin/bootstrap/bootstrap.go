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

package bootstrap

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/bootstrap"
)

func GetBootstrapCmd() *cobra.Command {
	bootstrapCmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap configuration management",
	}

	var bootstrapFile string
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import configuration from YAML file (one-time bootstrap)",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Connect to configdb
			configDBPool, err := dbopen.ConnectToConfigDB(ctx)
			if err != nil {
				return fmt.Errorf("failed to connect to configdb: %w", err)
			}
			defer configDBPool.Close()

			// Run import
			return bootstrap.ImportFromYAML(ctx, bootstrapFile, configDBPool)
		},
	}
	importCmd.Flags().StringVar(&bootstrapFile, "file", "", "YAML file to import (required)")
	if err := importCmd.MarkFlagRequired("file"); err != nil {
		panic(fmt.Sprintf("failed to mark flag as required: %v", err))
	}

	bootstrapCmd.AddCommand(importCmd)
	return bootstrapCmd
}
