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

package dbopen

import (
	"testing"

	"github.com/cardinalhq/lakerunner/migrations"
)

func TestOptions(t *testing.T) {
	// Test default options
	opts := Options{}
	if len(opts.MigrationCheckOptions) != 0 {
		t.Error("Expected MigrationCheckOptions to default to empty")
	}

	// Test skip migration check
	opts = SkipMigrationCheck()
	if len(opts.MigrationCheckOptions) == 0 {
		t.Error("Expected MigrationCheckOptions to be set")
	}

	// Test warn on mismatch
	opts = WarnOnMigrationMismatch()
	if len(opts.MigrationCheckOptions) == 0 {
		t.Error("Expected MigrationCheckOptions to be set")
	}

	// Test wait for migrations
	opts = WaitForMigrations()
	if len(opts.MigrationCheckOptions) == 0 {
		t.Error("Expected MigrationCheckOptions to be set")
	}
}

func TestMigrationCheckModes(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"Skip", SkipMigrationCheck()},
		{"Warn", WarnOnMigrationMismatch()},
		{"Wait", WaitForMigrations()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.opts.MigrationCheckOptions) == 0 {
				t.Error("Expected migration check options to be set")
			}
		})
	}
}

func TestCustomMigrationOptions(t *testing.T) {
	opts := Options{
		MigrationCheckOptions: []migrations.CheckOption{
			migrations.WithCheckMode(migrations.CheckModeWarn),
			migrations.WithTimeout(60),
		},
	}

	if len(opts.MigrationCheckOptions) != 2 {
		t.Errorf("Expected 2 migration check options, got %d", len(opts.MigrationCheckOptions))
	}
}
