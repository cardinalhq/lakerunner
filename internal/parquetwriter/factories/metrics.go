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

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

// NewMetricsWriter creates a writer optimized for metrics data.
// Metrics are ordered by metric name and grouped for efficient fingerprinting.
func NewMetricsWriter(baseName, tmpdir string, targetFileSize int64, bytesPerRecord float64) (*parquetwriter.UnifiedWriter, error) {
	config := parquetwriter.WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		TargetFileSize: targetFileSize,

		// Order by [metric name, TID] for efficient grouping
		OrderBy:      parquetwriter.OrderMergeSort,
		OrderKeyFunc: helpers.MetricsOrderKeyFunc(),

		// Group by [metric name, TID] - don't split groups with same name+TID
		GroupKeyFunc:  helpers.MetricsGroupKeyFunc(),
		NoSplitGroups: true,

		BytesPerRecord: bytesPerRecord,
		StatsProvider:  &MetricsStatsProvider{},
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// MetricsStatsProvider collects metric name statistics for metrics files.
type MetricsStatsProvider struct{}

func (p *MetricsStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
	return &MetricsStatsAccumulator{
		metricNames: mapset.NewSet[string](),
	}
}

// MetricsStatsAccumulator collects metrics-specific statistics.
type MetricsStatsAccumulator struct {
	metricNames mapset.Set[string]
	firstTS     int64
	lastTS      int64
	first       bool
}

func (a *MetricsStatsAccumulator) Add(row map[string]any) {
	// Track metric name for fingerprinting
	if name, ok := row["_cardinalhq.name"].(string); ok && name != "" {
		a.metricNames.Add(name)
	}

	// Track timestamp range
	if ts, ok := row["_cardinalhq.timestamp"].(int64); ok {
		if !a.first {
			a.firstTS = ts
			a.lastTS = ts
			a.first = true
		} else {
			if ts < a.firstTS {
				a.firstTS = ts
			}
			if ts > a.lastTS {
				a.lastTS = ts
			}
		}
	} else {
		// Debug: log when timestamp is missing or wrong type
		if tsVal, exists := row["_cardinalhq.timestamp"]; exists {
			// Timestamp exists but wrong type - this could be the issue
			fmt.Printf("DEBUG: timestamp wrong type: %T = %v\n", tsVal, tsVal)
		} else {
			// Timestamp completely missing
			fmt.Printf("DEBUG: timestamp missing from row\n")
		}
	}
}

func (a *MetricsStatsAccumulator) Finalize() any {
	// Create a map with metric names as a set for fingerprinting
	tagValuesByName := map[string]mapset.Set[string]{
		"_cardinalhq.name": a.metricNames,
	}

	// Generate fingerprints using the same approach as buffet
	fingerprintSet := fingerprint.ToFingerprints(tagValuesByName)
	fingerprints := fingerprintSet.ToSlice()
	slices.Sort(fingerprints)

	return MetricsFileStats{
		FirstTS:      a.firstTS,
		LastTS:       a.lastTS,
		Fingerprints: fingerprints,
	}
}

// MetricsFileStats contains statistics about a metrics file.
type MetricsFileStats struct {
	FirstTS      int64   // Earliest timestamp
	LastTS       int64   // Latest timestamp
	Fingerprints []int64 // Fingerprints for indexing
}

// ValidateMetricsRow checks that a row has the required fields for metrics processing.
func ValidateMetricsRow(row map[string]any) error {
	if _, ok := row["_cardinalhq.name"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.name")
	}
	if _, ok := row["_cardinalhq.tid"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.tid")
	}
	return nil
}
