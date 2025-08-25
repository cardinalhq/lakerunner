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

package dbopen

import "github.com/cardinalhq/lakerunner/migrations"

// Options configures database connection behavior
type Options struct {
	MigrationCheckOptions []migrations.CheckOption
}

// SkipMigrationCheck returns Options that skip migration checking entirely
func SkipMigrationCheck() Options {
	return Options{
		MigrationCheckOptions: []migrations.CheckOption{
			migrations.WithCheckMode(migrations.CheckModeSkip),
		},
	}
}

// WarnOnMigrationMismatch returns Options that warn on migration mismatches but continue
func WarnOnMigrationMismatch() Options {
	return Options{
		MigrationCheckOptions: []migrations.CheckOption{
			migrations.WithCheckMode(migrations.CheckModeWarn),
		},
	}
}

// WaitForMigrations returns Options that wait for migrations to complete (default behavior)
func WaitForMigrations() Options {
	return Options{
		MigrationCheckOptions: []migrations.CheckOption{
			migrations.WithCheckMode(migrations.CheckModeWait),
		},
	}
}
