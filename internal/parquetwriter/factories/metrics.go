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

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// NewMetricsWriter creates a writer optimized for metrics data.
// Metrics are ordered by metric name and grouped for efficient fingerprinting.
// The schema must be provided from the reader and cannot be nil.
// Uses DuckDB backend for lower memory usage during large metric ingestion.
func NewMetricsWriter(tmpdir string, schema *filereader.ReaderSchema, recordsPerFile int64) (parquetwriter.ParquetWriter, error) {
	config := parquetwriter.WriterConfig{
		TmpDir:      tmpdir,
		Schema:      schema,
		BackendType: parquetwriter.BackendDuckDB,

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
func metricsGroupKeyFunc() func(row pipeline.Row) any {
	metricNameKey := wkk.NewRowKey("metric_name")
	return func(row pipeline.Row) any {
		name, nameOk := row[metricNameKey].(string)
		if !nameOk {
			return nil
		}

		// Handle both string and int64 TID values
		var tid int64
		switch v := row[wkk.RowKeyCTID].(type) {
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
		metricNames:  mapset.NewSet[string](),
		metricTypes:  make(map[string]int16),
		labelColumns: mapset.NewSet[string](),
	}
}

// MetricsStatsAccumulator collects metrics-specific statistics.
type MetricsStatsAccumulator struct {
	metricNames  mapset.Set[string]
	metricTypes  map[string]int16 // metric_name -> metric_type (parallel to metricNames)
	labelColumns mapset.Set[string]
	firstTS      int64
	lastTS       int64
	first        bool
}

func (a *MetricsStatsAccumulator) Add(row pipeline.Row) {
	// Track metric name and type for fingerprinting
	metricNameKey := wkk.NewRowKey("metric_name")
	if name, ok := row[metricNameKey].(string); ok && name != "" {
		a.metricNames.Add(name)
		// Track metric type - only set if not already set (first occurrence wins)
		if _, exists := a.metricTypes[name]; !exists {
			if metricType, ok := row[wkk.RowKeyCMetricType].(string); ok {
				a.metricTypes[name] = lrdb.MetricTypeFromString(metricType)
			} else {
				a.metricTypes[name] = lrdb.MetricTypeUnknown
			}
		}
	}

	// Track label column names for label_name_map
	for key := range row {
		keyStr := string(key.Value())
		if isLabelColumn(keyStr) {
			a.labelColumns.Add(keyStr)
		}
	}

	// Track timestamp range
	if ts, ok := row[wkk.RowKeyCTimestamp].(int64); ok {
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
		if tsVal, exists := row[wkk.RowKeyCTimestamp]; exists {
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
		"metric_name": a.metricNames,
	}

	// Generate fingerprints using the same approach
	fingerprintSet := fingerprint.ToFingerprints(tagValuesByName)
	fingerprints := fingerprintSet.ToSlice()
	slices.Sort(fingerprints)

	// Build label name map from collected columns
	labelNameMap := buildLabelNameMap(a.labelColumns)

	// Extract metric names as sorted slice for storage
	metricNames := a.metricNames.ToSlice()
	slices.Sort(metricNames)

	// Build parallel metric types array (same order as metricNames)
	metricTypes := make([]int16, len(metricNames))
	for i, name := range metricNames {
		metricTypes[i] = a.metricTypes[name]
	}

	return MetricsFileStats{
		FirstTS:      a.firstTS,
		LastTS:       a.lastTS,
		Fingerprints: fingerprints,
		LabelNameMap: labelNameMap,
		MetricNames:  metricNames,
		MetricTypes:  metricTypes,
	}
}

// MetricsFileStats contains statistics about a metrics file.
type MetricsFileStats struct {
	FirstTS      int64    // Earliest timestamp
	LastTS       int64    // Latest timestamp
	Fingerprints []int64  // Fingerprints for indexing
	LabelNameMap []byte   // JSON map of label column names to dotted names
	MetricNames  []string // Unique metric names observed in this file
	MetricTypes  []int16  // Metric types parallel to MetricNames (use lrdb.MetricType* constants)
}

// ValidateMetricsRow checks that a row has the required fields for metrics processing.
func ValidateMetricsRow(row map[string]any) error {
	nameVal, ok := row["metric_name"]
	if !ok {
		return fmt.Errorf("missing required field: metric_name")
	}
	name, ok := nameVal.(string)
	if !ok {
		return fmt.Errorf("field metric_name must be a string, got %T", nameVal)
	}
	if len(name) == 0 {
		return fmt.Errorf("field metric_name cannot be empty")
	}

	tidVal, ok := row["chq_tid"]
	if !ok {
		return fmt.Errorf("missing required field: chq_tid")
	}
	if _, ok := tidVal.(int64); !ok {
		return fmt.Errorf("field chq_tid must be an int64, got %T", tidVal)
	}
	return nil
}
