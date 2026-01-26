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

package metricsprocessing

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// BenchmarkProcessMetricsWithDuckDB benchmarks the pure DuckDB metrics processing pipeline
func BenchmarkProcessMetricsWithDuckDB(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	// Verify test file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()
		ctx := context.Background()

		// Copy test file to temp location to simulate downloaded segment
		testData, err := os.ReadFile(testFile)
		require.NoError(b, err)

		segmentFile := filepath.Join(tmpDir, "segment.binpb.gz")
		err = os.WriteFile(segmentFile, testData, 0644)
		require.NoError(b, err)

		// Benchmark the pure DuckDB processing pipeline
		result, err := processMetricIngestWithDuckDB(ctx, []string{segmentFile}, orgID.String(), tmpDir)
		require.NoError(b, err)
		require.NotNil(b, result)
	}
}
