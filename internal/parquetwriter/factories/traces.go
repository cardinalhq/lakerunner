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

// NewTracesWriter creates a writer optimized for traces data.
// Traces are grouped by slot and can be split within slots but benefit from locality.
func NewTracesWriter(tmpdir string, slotID int32, recordsPerFile int64) (parquetwriter.ParquetWriter, error) {
	config := parquetwriter.WriterConfig{
		TmpDir: tmpdir,

		// Group by slot but allow splitting within slots
		GroupKeyFunc: func(row map[string]any) any {
			return slotID
		},
		NoSplitGroups: false, // Allow splitting within slots for size management

		RecordsPerFile: recordsPerFile,
		StatsProvider:  &TracesStatsProvider{SlotID: slotID},
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// TracesStatsProvider collects trace-specific statistics.
type TracesStatsProvider struct {
	SlotID int32
}

func (p *TracesStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
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
	// Extract sorted list of span IDs from the map
	spanIDs := make([]string, 0, len(a.spansSeen))
	for spanID := range a.spansSeen {
		spanIDs = append(spanIDs, spanID)
	}
	slices.Sort(spanIDs)

	// Extract sorted list of trace IDs from the map
	traceIDs := make([]string, 0, len(a.tracesSeen))
	for traceID := range a.tracesSeen {
		traceIDs = append(traceIDs, traceID)
	}
	slices.Sort(traceIDs)

	return TracesFileStats{
		SlotID:     a.slotID,
		SpanCount:  int64(len(a.spansSeen)),
		TraceCount: int64(len(a.tracesSeen)),
		FirstTS:    a.firstTS,
		LastTS:     a.lastTS,
		SpanIDs:    spanIDs,
		TraceIDs:   traceIDs,
	}
}

// TracesFileStats contains statistics about a traces file.
type TracesFileStats struct {
	SlotID     int32    // Slot ID for this file
	SpanCount  int64    // Number of unique spans
	TraceCount int64    // Number of unique traces
	FirstTS    int64    // Earliest start time
	LastTS     int64    // Latest start time
	SpanIDs    []string // Actual list of unique span IDs in this file
	TraceIDs   []string // Actual list of unique trace IDs in this file
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
