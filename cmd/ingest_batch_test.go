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

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func TestBatchConfigurationDefaults(t *testing.T) {
	// Test that batch configuration returns expected defaults
	logsSize := helpers.GetBatchSizeForSignal("logs")
	if logsSize != 1 {
		t.Errorf("Expected default logs batch size of 1, got %d", logsSize)
	}

	metricsSize := helpers.GetBatchSizeForSignal("metrics")
	if metricsSize != 1 {
		t.Errorf("Expected default metrics batch size of 1, got %d", metricsSize)
	}

	tracesSize := helpers.GetBatchSizeForSignal("traces")
	if tracesSize != 1 {
		t.Errorf("Expected default traces batch size of 1, got %d", tracesSize)
	}

	targetSize := helpers.GetTargetSizeBytes()
	expectedSize := int64(1024 * 1024) // 1MB
	if targetSize != expectedSize {
		t.Errorf("Expected default target size of %d bytes, got %d", expectedSize, targetSize)
	}

	maxBatch := helpers.GetMaxBatchSize()
	if maxBatch != 20 {
		t.Errorf("Expected default max batch size of 20, got %d", maxBatch)
	}
}

func TestBatchConfigurationWithEnvironment(t *testing.T) {
	// Set environment variables
	os.Setenv("LAKERUNNER_LOGS_BATCH_SIZE", "5")
	os.Setenv("LAKERUNNER_METRICS_BATCH_SIZE", "10")
	os.Setenv("LAKERUNNER_TRACES_BATCH_SIZE", "15")
	os.Setenv("LAKERUNNER_TARGET_SIZE_BYTES", "2097152")
	os.Setenv("LAKERUNNER_MAX_BATCH_SIZE", "50")

	defer func() {
		os.Unsetenv("LAKERUNNER_LOGS_BATCH_SIZE")
		os.Unsetenv("LAKERUNNER_METRICS_BATCH_SIZE")
		os.Unsetenv("LAKERUNNER_TRACES_BATCH_SIZE")
		os.Unsetenv("LAKERUNNER_TARGET_SIZE_BYTES")
		os.Unsetenv("LAKERUNNER_MAX_BATCH_SIZE")
	}()

	// Test configured values
	logsSize := helpers.GetBatchSizeForSignal("logs")
	if logsSize != 5 {
		t.Errorf("Expected configured logs batch size of 5, got %d", logsSize)
	}

	metricsSize := helpers.GetBatchSizeForSignal("metrics")
	if metricsSize != 10 {
		t.Errorf("Expected configured metrics batch size of 10, got %d", metricsSize)
	}

	tracesSize := helpers.GetBatchSizeForSignal("traces")
	if tracesSize != 15 {
		t.Errorf("Expected configured traces batch size of 15, got %d", tracesSize)
	}

	targetSize := helpers.GetTargetSizeBytes()
	expectedSize := int64(2 * 1024 * 1024) // 2MB
	if targetSize != expectedSize {
		t.Errorf("Expected configured target size of %d bytes, got %d", expectedSize, targetSize)
	}

	maxBatch := helpers.GetMaxBatchSize()
	if maxBatch != 50 {
		t.Errorf("Expected configured max batch size of 50, got %d", maxBatch)
	}
}
