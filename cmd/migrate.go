// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	mdbmigrations "github.com/cardinalhq/lakerunner/pkg/lrdb/migrations"
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
