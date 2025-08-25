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

package migrations

import (
	"os"
	"testing"
	"time"
)

func TestExtractLatestMigrationVersion(t *testing.T) {
	got, err := extractLatestMigrationVersion(migrationFiles)
	if err != nil {
		t.Errorf("extractLatestMigrationVersion() error = %v", err)
		return
	}
	// The exact version depends on the current migrations, but it should be > 0
	if got == 0 {
		t.Error("extractLatestMigrationVersion() returned 0, expected a valid version")
	}
	t.Logf("Latest configdb migration version: %d", got)
}

func TestGetMigrationCheckConfig(t *testing.T) {
	// Save original environment
	originalVars := make(map[string]string)
	envVars := []string{
		"CONFIGDB_MIGRATION_CHECK_ENABLED",
		"MIGRATION_CHECK_TIMEOUT",
		"MIGRATION_CHECK_RETRY_INTERVAL",
		"MIGRATION_CHECK_ALLOW_DIRTY",
	}

	for _, key := range envVars {
		if val := os.Getenv(key); val != "" {
			originalVars[key] = val
		}
		os.Unsetenv(key)
	}
	defer func() {
		for _, key := range envVars {
			os.Unsetenv(key)
		}
		for key, val := range originalVars {
			os.Setenv(key, val)
		}
	}()

	// Test defaults
	config := getMigrationCheckConfig()
	if !config.Enabled {
		t.Error("Expected Enabled to default to true")
	}
	if config.Timeout != 60*time.Second {
		t.Errorf("Expected Timeout to default to 60s, got %v", config.Timeout)
	}
	if config.RetryInterval != 5*time.Second {
		t.Errorf("Expected RetryInterval to default to 5s, got %v", config.RetryInterval)
	}
	if config.AllowDirty {
		t.Error("Expected AllowDirty to default to false")
	}

	// Test custom values
	os.Setenv("CONFIGDB_MIGRATION_CHECK_ENABLED", "false")
	os.Setenv("MIGRATION_CHECK_TIMEOUT", "30s")
	os.Setenv("MIGRATION_CHECK_RETRY_INTERVAL", "2s")
	os.Setenv("MIGRATION_CHECK_ALLOW_DIRTY", "true")

	config = getMigrationCheckConfig()
	if config.Enabled {
		t.Error("Expected Enabled to be false")
	}
	if config.Timeout != 30*time.Second {
		t.Errorf("Expected Timeout to be 30s, got %v", config.Timeout)
	}
	if config.RetryInterval != 2*time.Second {
		t.Errorf("Expected RetryInterval to be 2s, got %v", config.RetryInterval)
	}
	if !config.AllowDirty {
		t.Error("Expected AllowDirty to be true")
	}
}
