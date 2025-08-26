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
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

// NewMetricsWriter creates a writer optimized for metrics data.
// Metrics are ordered by metric name and grouped for efficient fingerprinting.
func NewMetricsWriter(baseName, tmpdir string, targetFileSize int64, recordsPerFile int64) (*parquetwriter.UnifiedWriter, error) {
	config := parquetwriter.WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		TargetFileSize: targetFileSize,

		// Input is already globally sorted by our pipeline, no ordering needed
		OrderBy: parquetwriter.OrderNone,

		// Group by [metric name, TID] - don't split groups with same name+TID
		GroupKeyFunc:  metricsGroupKeyFunc(),
		NoSplitGroups: true,

		RecordsPerFile: recordsPerFile,
		StatsProvider:  &MetricsStatsProvider{},
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// metricsGroupKeyFunc returns the grouping key function for metrics.
// Groups by [metric name, TID] only - keeps all timestamps for the same metric together
// for efficient rollup aggregation.
func metricsGroupKeyFunc() func(row map[string]any) any {
	return func(row map[string]any) any {
		name, nameOk := row["_cardinalhq.name"].(string)
		if !nameOk {
			return nil
		}

		// Handle both string and int64 TID values
		var tid int64
		switch v := row["_cardinalhq.tid"].(type) {
		case int64:
			tid = v
		case string:
			// TID is incorrectly stored as string, parse it
			if parsed, err := fmt.Sscanf(v, "%d", &tid); err != nil || parsed != 1 {
				return nil
			}
		default:
			return nil
		}

		// Group by [name, tid] only - no timestamp for better rollup aggregation
		return fmt.Sprintf("%s:%d", name, tid)
	}
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
	nameVal, ok := row["_cardinalhq.name"]
	if !ok {
		return fmt.Errorf("missing required field: _cardinalhq.name")
	}
	name, ok := nameVal.(string)
	if !ok {
		return fmt.Errorf("field _cardinalhq.name must be a string, got %T", nameVal)
	}
	if len(name) == 0 {
		return fmt.Errorf("field _cardinalhq.name cannot be empty")
	}

	tidVal, ok := row["_cardinalhq.tid"]
	if !ok {
		return fmt.Errorf("missing required field: _cardinalhq.tid")
	}
	if _, ok := tidVal.(int64); !ok {
		return fmt.Errorf("field _cardinalhq.tid must be an int64, got %T", tidVal)
	}
	return nil
}
