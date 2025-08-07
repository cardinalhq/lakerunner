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

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	mdbmigrations "github.com/cardinalhq/lakerunner/lrdb/migrations"
)

func init() {
	rootCmd.AddCommand(MigrateCmd)
}

var MigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long:  "Run database migrations",
	RunE:  migrate,
}

func migrate(_ *cobra.Command, _ []string) error {
	if err := migratelrdb(); err != nil {
		return fmt.Errorf("failed to migrate lrdb db: %w", err)
	}

	return nil
}

func migratelrdb() error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Minute))
	defer cancel()
	pool, err := dbopen.ConnectTolrdb(ctx)
	if err != nil {
		return err
	}
	return mdbmigrations.RunMigrationsUp(context.Background(), pool)
}
