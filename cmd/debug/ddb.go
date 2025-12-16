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

package debug

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

func GetDDBCmd() *cobra.Command {
	ddbCmd := &cobra.Command{
		Use:   "ddb",
		Short: "DuckDB debugging commands",
	}

	ddbCmd.AddCommand(getVersionCmd())

	return ddbCmd
}

func getVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Check DuckDB version and validate it matches expected version",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVersion(cmd.Context())
		},
	}
}

// runVersion checks DuckDB version and validates it matches expected version.
func runVersion(ctx context.Context) error {
	// Use temporary file-based S3DB pool
	s3db, err := duckdbx.NewS3DB()
	if err != nil {
		return err
	}
	defer func() {
		_ = s3db.Close()
		// Clean up the temp database file
		if dbPath := s3db.GetDatabasePath(); dbPath != "" {
			_ = os.RemoveAll(filepath.Dir(dbPath))
		}
	}()

	c, release, err := s3db.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer release()

	// Get DuckDB version
	var version string
	err = c.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to get DuckDB version: %w", err)
	}

	fmt.Printf("DuckDB version: %s\n", version)

	// Expected version - this should match the version in go.mod or build args
	expectedVersion := "v1.3.2"

	// Check if the running version contains the expected version
	// DuckDB version string is like "v1.3.2 af7bcaf"
	if !strings.Contains(version, expectedVersion) {
		return fmt.Errorf("DuckDB version mismatch: expected %s, got %s", expectedVersion, version)
	}

	return nil
}
