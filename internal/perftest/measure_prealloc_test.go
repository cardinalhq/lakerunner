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

package perftest

import (
	"runtime"
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
)

// TestMeasurePreallocMemory measures how much memory 400k pre-allocated rows consume.
func TestMeasurePreallocMemory(t *testing.T) {
	// Get baseline BEFORE allocation
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	t.Logf("Baseline before allocation:")
	t.Logf("  HeapAlloc: %.2f MB", float64(baseline.HeapAlloc)/(1024*1024))
	t.Logf("  HeapInuse: %.2f MB", float64(baseline.HeapInuse)/(1024*1024))
	t.Logf("  Sys (RSS): %.2f MB", float64(baseline.Sys)/(1024*1024))

	// Allocate 400k rows
	gen := NewSyntheticDataGenerator()
	batches := gen.GenerateBatches(400000, 1000)

	// Force GC to stabilize heap
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	t.Logf("\nAfter allocating 400k rows (400 batches):")
	t.Logf("  HeapAlloc: %.2f MB", float64(after.HeapAlloc)/(1024*1024))
	t.Logf("  HeapInuse: %.2f MB", float64(after.HeapInuse)/(1024*1024))
	t.Logf("  Sys (RSS): %.2f MB", float64(after.Sys)/(1024*1024))

	heapDelta := int64(after.HeapAlloc - baseline.HeapAlloc)
	heapInuseDelta := int64(after.HeapInuse - baseline.HeapInuse)
	sysDelta := int64(after.Sys - baseline.Sys)

	t.Logf("\nMemory consumed by 400k rows:")
	t.Logf("  HeapAlloc delta: %.2f MB", float64(heapDelta)/(1024*1024))
	t.Logf("  HeapInuse delta: %.2f MB", float64(heapInuseDelta)/(1024*1024))
	t.Logf("  Sys (RSS) delta: %.2f MB", float64(sysDelta)/(1024*1024))

	// Calculate recommended GOMEMLIMIT
	// Current limit: 1538 MB
	// Add 1.25x the RSS delta for safety
	currentLimit := int64(1538 * 1024 * 1024)
	safetyBuffer := int64(float64(sysDelta) * 1.25)
	recommendedLimit := currentLimit + safetyBuffer

	t.Logf("\nRecommended GOMEMLIMIT adjustment:")
	t.Logf("  Current limit: %.2f MB", float64(currentLimit)/(1024*1024))
	t.Logf("  Pre-alloc overhead (1.25x): %.2f MB", float64(safetyBuffer)/(1024*1024))
	t.Logf("  Recommended new limit: %.2f MB", float64(recommendedLimit)/(1024*1024))
	t.Logf("  Recommended new limit (bytes): %d", recommendedLimit)

	// Clean up
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}
}
