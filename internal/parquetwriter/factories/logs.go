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

// NewLogsWriter creates a writer optimized for logs data.
// Logs need to be sorted by timestamp and can be split freely.
func NewLogsWriter(baseName, tmpdir string, targetFileSize int64, bytesPerRecord float64) (*parquetwriter.UnifiedWriter, error) {
	config := parquetwriter.WriterConfig{
		BaseName:       baseName,
		TmpDir:         tmpdir,
		TargetFileSize: targetFileSize,

		// Logs need to be sorted by timestamp, use spillable orderer for automatic handling
		OrderBy: parquetwriter.OrderSpillable,
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

		BytesPerRecord: bytesPerRecord,
		StatsProvider:  &LogsStatsProvider{},
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// LogsStatsProvider collects timestamp and fingerprint statistics for logs files.
type LogsStatsProvider struct{}

func (p *LogsStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
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
	// Extract sorted list of fingerprints from the map
	fingerprints := make([]int64, 0, len(a.fingerprints))
	for fp := range a.fingerprints {
		fingerprints = append(fingerprints, fp)
	}
	slices.Sort(fingerprints)

	return LogsFileStats{
		FingerprintCount: int64(len(a.fingerprints)),
		FirstTS:          a.firstTS,
		LastTS:           a.lastTS,
		Fingerprints:     fingerprints,
	}
}

// LogsFileStats contains statistics about a logs file.
type LogsFileStats struct {
	FingerprintCount int64   // Number of unique fingerprints
	FirstTS          int64   // Earliest timestamp
	LastTS           int64   // Latest timestamp
	Fingerprints     []int64 // Actual list of unique fingerprints in this file
}

// ValidateLogsRow checks that a row has the required fields for logs processing.
func ValidateLogsRow(row map[string]any) error {
	if _, ok := row["_cardinalhq.timestamp"]; !ok {
		return fmt.Errorf("missing required field: _cardinalhq.timestamp")
	}
	return nil
}
