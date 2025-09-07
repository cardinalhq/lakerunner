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
	"os"
	"testing"

	"github.com/cardinalhq/lakerunner/config"
)

func TestBatchConfigurationDefaults(t *testing.T) {
	// Clear any environment variables that might interfere
	oldBatch := os.Getenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES")
	oldMax := os.Getenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE")
	defer func() {
		if oldBatch != "" {
			os.Setenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES", oldBatch)
		}
		if oldMax != "" {
			os.Setenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE", oldMax)
		}
	}()
	os.Unsetenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES")
	os.Unsetenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE")

	// Test that batch configuration returns expected defaults
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	expectedTargetSize := int64(100 * 1024 * 1024) // 100MB default
	if cfg.Batch.TargetSizeBytes != expectedTargetSize {
		t.Errorf("Expected default target size of %d bytes, got %d", expectedTargetSize, cfg.Batch.TargetSizeBytes)
	}

	expectedMaxBatch := 100 // Default from config
	if cfg.Batch.MaxBatchSize != expectedMaxBatch {
		t.Errorf("Expected default max batch size of %d, got %d", expectedMaxBatch, cfg.Batch.MaxBatchSize)
	}

	expectedMaxTotal := int64(1024 * 1024 * 1024) // 1GB default
	if cfg.Batch.MaxTotalSize != expectedMaxTotal {
		t.Errorf("Expected default max total size of %d bytes, got %d", expectedMaxTotal, cfg.Batch.MaxTotalSize)
	}
}

func TestBatchConfigurationWithEnvironment(t *testing.T) {
	// Save original environment variables
	oldTargetSize := os.Getenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES")
	oldMaxBatch := os.Getenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE")
	oldMaxTotal := os.Getenv("LAKERUNNER_BATCH_MAX_TOTAL_SIZE")
	oldMaxAge := os.Getenv("LAKERUNNER_BATCH_MAX_AGE_SECONDS")
	oldMinBatch := os.Getenv("LAKERUNNER_BATCH_MIN_BATCH_SIZE")

	// Set environment variables
	os.Setenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES", "2097152") // 2MB
	os.Setenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE", "50")
	os.Setenv("LAKERUNNER_BATCH_MAX_TOTAL_SIZE", "104857600") // 100MB
	os.Setenv("LAKERUNNER_BATCH_MAX_AGE_SECONDS", "600")      // 10 minutes
	os.Setenv("LAKERUNNER_BATCH_MIN_BATCH_SIZE", "5")

	defer func() {
		// Restore original environment variables
		if oldTargetSize != "" {
			os.Setenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES", oldTargetSize)
		} else {
			os.Unsetenv("LAKERUNNER_BATCH_TARGET_SIZE_BYTES")
		}
		if oldMaxBatch != "" {
			os.Setenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE", oldMaxBatch)
		} else {
			os.Unsetenv("LAKERUNNER_BATCH_MAX_BATCH_SIZE")
		}
		if oldMaxTotal != "" {
			os.Setenv("LAKERUNNER_BATCH_MAX_TOTAL_SIZE", oldMaxTotal)
		} else {
			os.Unsetenv("LAKERUNNER_BATCH_MAX_TOTAL_SIZE")
		}
		if oldMaxAge != "" {
			os.Setenv("LAKERUNNER_BATCH_MAX_AGE_SECONDS", oldMaxAge)
		} else {
			os.Unsetenv("LAKERUNNER_BATCH_MAX_AGE_SECONDS")
		}
		if oldMinBatch != "" {
			os.Setenv("LAKERUNNER_BATCH_MIN_BATCH_SIZE", oldMinBatch)
		} else {
			os.Unsetenv("LAKERUNNER_BATCH_MIN_BATCH_SIZE")
		}
	}()

	// Load config with environment variables
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Test configured values
	expectedTargetSize := int64(2 * 1024 * 1024) // 2MB
	if cfg.Batch.TargetSizeBytes != expectedTargetSize {
		t.Errorf("Expected configured target size of %d bytes, got %d", expectedTargetSize, cfg.Batch.TargetSizeBytes)
	}

	if cfg.Batch.MaxBatchSize != 50 {
		t.Errorf("Expected configured max batch size of 50, got %d", cfg.Batch.MaxBatchSize)
	}

	expectedMaxTotal := int64(100 * 1024 * 1024) // 100MB
	if cfg.Batch.MaxTotalSize != expectedMaxTotal {
		t.Errorf("Expected configured max total size of %d bytes, got %d", expectedMaxTotal, cfg.Batch.MaxTotalSize)
	}

	if cfg.Batch.MaxAgeSeconds != 600 {
		t.Errorf("Expected configured max age of 600 seconds, got %d", cfg.Batch.MaxAgeSeconds)
	}

	if cfg.Batch.MinBatchSize != 5 {
		t.Errorf("Expected configured min batch size of 5, got %d", cfg.Batch.MinBatchSize)
	}
}
