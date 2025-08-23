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

package parquetwriter

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// Signal-specific factory functions for creating optimally configured writers

// NewMetricsWriter creates a writer optimized for metrics data.
// Metrics are expected to be pre-ordered by TID and should not be split across TID boundaries.
func NewMetricsWriter(baseName, tmpdir string, schemaNodes map[string]parquet.Node, targetFileSize int64) (*UnifiedWriter, error) {
	config := WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		SchemaNodes:    schemaNodes,
		TargetFileSize: targetFileSize,

		// Metrics are already TID-ordered from the TID merger
		OrderBy: OrderPresumed,

		// Group by TID and never split TID groups across files
		GroupKeyFunc: func(row map[string]any) any {
			if tid, ok := row["_cardinalhq.tid"].(int64); ok {
				return tid
			}
			return nil
		},
		NoSplitGroups: true,

		SizeEstimator: NewAdaptiveSizeEstimator(),
		StatsProvider: &MetricsStatsProvider{},
	}

	return NewUnifiedWriter(config)
}

// NewLogsWriter creates a writer optimized for logs data.
// Logs need to be sorted by timestamp and can be split freely.
func NewLogsWriter(baseName, tmpdir string, schemaNodes map[string]parquet.Node, targetFileSize int64) (*UnifiedWriter, error) {
	config := WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		SchemaNodes:    schemaNodes,
		TargetFileSize: targetFileSize,

		// Logs need to be sorted by timestamp using external merge sort for large datasets
		OrderBy: OrderMergeSort,
		OrderKeyFunc: func(row map[string]any) any {
			if ts, ok := row["_cardinalhq.timestamp"].(int64); ok {
				return ts
			}
			// Try float64 conversion (common in JSON parsing)
			if ts, ok := row["_cardinalhq.timestamp"].(float64); ok {
				return int64(ts)
			}
			return int64(0) // Fallback
		},

		// Logs can be split anywhere - no grouping constraints
		NoSplitGroups: false,

		SizeEstimator: NewAdaptiveSizeEstimator(),
		StatsProvider: &LogsStatsProvider{},
	}

	return NewUnifiedWriter(config)
}

// NewTracesWriter creates a writer optimized for traces data.
// Traces are grouped by slot and can be split within slots but benefit from locality.
func NewTracesWriter(baseName, tmpdir string, schemaNodes map[string]parquet.Node, targetFileSize int64, slotID int32) (*UnifiedWriter, error) {
	config := WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		SchemaNodes:    schemaNodes,
		TargetFileSize: targetFileSize,

		// Traces typically don't need strong ordering, but could be sorted by timestamp if desired
		OrderBy: OrderInMemory, // Use in-memory sorting for moderate datasets
		OrderKeyFunc: func(row map[string]any) any {
			// Sort by start time if available, otherwise by trace ID
			if startTime, ok := row["_cardinalhq.start_time_unix_ns"].(int64); ok {
				return startTime
			}
			if traceID, ok := row["_cardinalhq.trace_id"].(string); ok {
				return traceID
			}
			return ""
		},

		// Group by slot but allow splitting within slots
		GroupKeyFunc: func(row map[string]any) any {
			return slotID
		},
		NoSplitGroups: false, // Allow splitting within slots for size management

		SizeEstimator: NewAdaptiveSizeEstimator(),
		StatsProvider: &TracesStatsProvider{SlotID: slotID},
	}

	return NewUnifiedWriter(config)
}

// NewCustomWriter creates a writer with fully custom configuration.
// This provides maximum flexibility for specialized use cases.
func NewCustomWriter(config WriterConfig) (*UnifiedWriter, error) {
	return NewUnifiedWriter(config)
}

// Signal-specific StatsProvider implementations

// MetricsStatsProvider collects TID-related statistics for metrics files.
type MetricsStatsProvider struct{}

func (p *MetricsStatsProvider) NewAccumulator() StatsAccumulator {
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
	return MetricsFileStats{
		TIDCount: int32(len(a.tidSeen)),
		MinTID:   a.minTID,
		MaxTID:   a.maxTID,
	}
}

// MetricsFileStats contains statistics about a metrics file.
type MetricsFileStats struct {
	TIDCount int32 // Number of unique TIDs in the file
	MinTID   int64 // Minimum TID value
	MaxTID   int64 // Maximum TID value
}

// LogsStatsProvider collects timestamp and fingerprint statistics for logs files.
type LogsStatsProvider struct{}

func (p *LogsStatsProvider) NewAccumulator() StatsAccumulator {
	return &LogsStatsAccumulator{
		fingerprints: make(map[int64]bool),
	}
}

// LogsStatsAccumulator collects logs-specific statistics.
type LogsStatsAccumulator struct {
	fingerprints map[int64]bool
	firstTS      int64
	lastTS       int64
	first        bool
}

func (a *LogsStatsAccumulator) Add(row map[string]any) {
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
	}

	// Track fingerprints if available
	if fp, ok := row["_cardinalhq.fingerprint"].(int64); ok {
		a.fingerprints[fp] = true
	}
}

func (a *LogsStatsAccumulator) Finalize() any {
	return LogsFileStats{
		FingerprintCount: int64(len(a.fingerprints)),
		FirstTS:          a.firstTS,
		LastTS:           a.lastTS,
	}
}

// LogsFileStats contains statistics about a logs file.
type LogsFileStats struct {
	FingerprintCount int64 // Number of unique fingerprints
	FirstTS          int64 // Earliest timestamp
	LastTS           int64 // Latest timestamp
}

// TracesStatsProvider collects trace-specific statistics.
type TracesStatsProvider struct {
	SlotID int32
}

func (p *TracesStatsProvider) NewAccumulator() StatsAccumulator {
	return &TracesStatsAccumulator{
		slotID:     p.SlotID,
		spansSeen:  make(map[string]bool),
		tracesSeen: make(map[string]bool),
	}
}

// TracesStatsAccumulator collects traces-specific statistics.
type TracesStatsAccumulator struct {
	slotID     int32
	spansSeen  map[string]bool
	tracesSeen map[string]bool
	firstTS    int64
	lastTS     int64
	first      bool
}

func (a *TracesStatsAccumulator) Add(row map[string]any) {
	// Track timestamp range
	if startTime, ok := row["_cardinalhq.start_time_unix_ns"].(int64); ok {
		if !a.first {
			a.firstTS = startTime
			a.lastTS = startTime
			a.first = true
		} else {
			if startTime < a.firstTS {
				a.firstTS = startTime
			}
			if startTime > a.lastTS {
				a.lastTS = startTime
			}
		}
	}

	// Track unique traces
	if traceID, ok := row["_cardinalhq.trace_id"].(string); ok {
		a.tracesSeen[traceID] = true
	}

	// Track unique spans
	if spanID, ok := row["_cardinalhq.span_id"].(string); ok {
		a.spansSeen[spanID] = true
	}
}

func (a *TracesStatsAccumulator) Finalize() any {
	return TracesFileStats{
		SlotID:     a.slotID,
		SpanCount:  int64(len(a.spansSeen)),
		TraceCount: int64(len(a.tracesSeen)),
		FirstTS:    a.firstTS,
		LastTS:     a.lastTS,
	}
}

// TracesFileStats contains statistics about a traces file.
type TracesFileStats struct {
	SlotID     int32 // Slot ID for this file
	SpanCount  int64 // Number of unique spans
	TraceCount int64 // Number of unique traces
	FirstTS    int64 // Earliest start time
	LastTS     int64 // Latest start time
}

// Helper function to create a writer configuration for testing.
func NewTestConfig(baseName, tmpdir string, nodes map[string]parquet.Node) WriterConfig {
	return WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		SchemaNodes:    nodes,
		TargetFileSize: 1000, // Small size for testing
		OrderBy:        OrderNone,
		SizeEstimator:  NewFixedSizeEstimator(50), // Fixed size for predictable tests
	}
}

// Validation helpers

// ValidateMetricsRow checks that a row has the required fields for metrics processing.
func ValidateMetricsRow(row map[string]any) error {
	if _, ok := row["_cardinalhq.tid"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.tid")
	}
	return nil
}

// ValidateLogsRow checks that a row has the required fields for logs processing.
func ValidateLogsRow(row map[string]any) error {
	if _, ok := row["_cardinalhq.timestamp"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.timestamp")
	}
	return nil
}

// ValidateTracesRow checks that a row has the required fields for traces processing.
func ValidateTracesRow(row map[string]any) error {
	if _, ok := row["_cardinalhq.trace_id"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.trace_id")
	}
	if _, ok := row["_cardinalhq.span_id"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.span_id")
	}
	return nil
}
