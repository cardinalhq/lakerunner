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

package factories

import (
	"fmt"
	"slices"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

// NewMetricsWriter creates a writer optimized for metrics data.
// Metrics are expected to be pre-ordered by TID and should not be split across TID boundaries.
func NewMetricsWriter(baseName, tmpdir string, targetFileSize int64, bytesPerRecord float64) (*parquetwriter.UnifiedWriter, error) {
	config := parquetwriter.WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		TargetFileSize: targetFileSize,

		// Metrics are already TID-ordered from the TID merger
		OrderBy: parquetwriter.OrderPresumed,

		// Group by TID and never split TID groups across files
		GroupKeyFunc: func(row map[string]any) any {
			if tid, ok := row["_cardinalhq.tid"].(int64); ok {
				return tid
			}
			return nil
		},
		NoSplitGroups: true,

		BytesPerRecord: bytesPerRecord,
		StatsProvider:  &MetricsStatsProvider{},
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// MetricsStatsProvider collects TID-related statistics for metrics files.
type MetricsStatsProvider struct{}

func (p *MetricsStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
	return &MetricsStatsAccumulator{
		tidSeen: make(map[int64]bool),
	}
}

// MetricsStatsAccumulator collects metrics-specific statistics.
type MetricsStatsAccumulator struct {
	tidSeen map[int64]bool
	minTID  int64
	maxTID  int64
	first   bool
}

func (a *MetricsStatsAccumulator) Add(row map[string]any) {
	if tid, ok := row["_cardinalhq.tid"].(int64); ok {
		a.tidSeen[tid] = true

		if !a.first {
			a.minTID = tid
			a.maxTID = tid
			a.first = true
		} else {
			if tid < a.minTID {
				a.minTID = tid
			}
			if tid > a.maxTID {
				a.maxTID = tid
			}
		}
	}
}

func (a *MetricsStatsAccumulator) Finalize() any {
	// Extract sorted list of TIDs from the map
	tids := make([]int64, 0, len(a.tidSeen))
	for tid := range a.tidSeen {
		tids = append(tids, tid)
	}
	slices.Sort(tids)

	return MetricsFileStats{
		TIDCount: int32(len(a.tidSeen)),
		MinTID:   a.minTID,
		MaxTID:   a.maxTID,
		TIDs:     tids,
	}
}

// MetricsFileStats contains statistics about a metrics file.
type MetricsFileStats struct {
	TIDCount int32   // Number of unique TIDs in the file
	MinTID   int64   // Minimum TID value
	MaxTID   int64   // Maximum TID value
	TIDs     []int64 // Actual list of unique TIDs in this file
}

// ValidateMetricsRow checks that a row has the required fields for metrics processing.
func ValidateMetricsRow(row map[string]any) error {
	if _, ok := row["_cardinalhq.tid"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.tid")
	}
	return nil
}
