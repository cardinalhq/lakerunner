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

package pubsub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatsAggregator_RecordFileTypes(t *testing.T) {
	sa := NewStatsAggregator(time.Minute)

	// Record file types for logs
	sa.RecordFileTypes("logs", map[string]int{
		"json":    5,
		"json.gz": 3,
		"other":   1,
	})

	// Record more file types for logs (should accumulate)
	sa.RecordFileTypes("logs", map[string]int{
		"json":    2,
		"json.gz": 1,
	})

	// Record file types for metrics
	sa.RecordFileTypes("metrics", map[string]int{
		"binpb":    10,
		"binpb.gz": 5,
	})

	// Record file types for traces
	sa.RecordFileTypes("traces", map[string]int{
		"parquet": 15,
	})

	// Verify accumulated counts
	sa.mu.Lock()
	defer sa.mu.Unlock()

	// Check logs counts
	assert.Equal(t, int64(7), sa.stats["logs"].fileTypeCounts["json"])
	assert.Equal(t, int64(4), sa.stats["logs"].fileTypeCounts["json.gz"])
	assert.Equal(t, int64(1), sa.stats["logs"].fileTypeCounts["other"])

	// Check metrics counts
	assert.Equal(t, int64(10), sa.stats["metrics"].fileTypeCounts["binpb"])
	assert.Equal(t, int64(5), sa.stats["metrics"].fileTypeCounts["binpb.gz"])

	// Check traces counts
	assert.Equal(t, int64(15), sa.stats["traces"].fileTypeCounts["parquet"])
}

func TestStatsAggregator_RecordFileTypesWithProcessing(t *testing.T) {
	sa := NewStatsAggregator(time.Minute)

	// Record both processing stats and file types
	sa.RecordProcessed("logs", 10)
	sa.RecordFileTypes("logs", map[string]int{
		"json":    6,
		"json.gz": 4,
	})

	sa.RecordProcessed("metrics", 5)
	sa.RecordFileTypes("metrics", map[string]int{
		"binpb":    3,
		"binpb.gz": 2,
	})

	sa.RecordFailed("logs", 2)
	sa.RecordSkipped("traces", 1)

	// Verify all stats are properly recorded
	sa.mu.Lock()
	defer sa.mu.Unlock()

	// Check logs
	assert.Equal(t, int64(10), sa.stats["logs"].processed)
	assert.Equal(t, int64(2), sa.stats["logs"].failed)
	assert.Equal(t, int64(6), sa.stats["logs"].fileTypeCounts["json"])
	assert.Equal(t, int64(4), sa.stats["logs"].fileTypeCounts["json.gz"])

	// Check metrics
	assert.Equal(t, int64(5), sa.stats["metrics"].processed)
	assert.Equal(t, int64(3), sa.stats["metrics"].fileTypeCounts["binpb"])
	assert.Equal(t, int64(2), sa.stats["metrics"].fileTypeCounts["binpb.gz"])

	// Check traces
	assert.Equal(t, int64(1), sa.stats["traces"].skipped)
}
