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

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

// NewLogsWriter creates a writer optimized for logs data.
// Logs need to be sorted by timestamp and can be split freely.
func NewLogsWriter(tmpdir string, recordsPerFile int64) (parquetwriter.ParquetWriter, error) {
	config := parquetwriter.WriterConfig{
		TmpDir: tmpdir,

		// Logs can be split anywhere - no grouping constraints
		NoSplitGroups: false,

		RecordsPerFile: recordsPerFile,
		StatsProvider:  &LogsStatsProvider{},
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// LogsStatsProvider collects timestamp and fingerprint statistics for logs files.
type LogsStatsProvider struct{}

func (p *LogsStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
	return &LogsStatsAccumulator{
		fingerprints: mapset.NewSet[int64](),
		labelColumns: mapset.NewSet[string](),
	}
}

// LogsStatsAccumulator collects logs-specific statistics.
type LogsStatsAccumulator struct {
	fingerprints mapset.Set[int64]
	labelColumns mapset.Set[string]
	firstTS      int64
	lastTS       int64
	first        bool
}

func (a *LogsStatsAccumulator) Add(row map[string]any) {
	// Track label column names for label_name_map
	for key := range row {
		if isLabelColumn(key) {
			a.labelColumns.Add(key)
		}
	}

	// Track timestamp range
	if ts, ok := row["chq_timestamp"].(int64); ok {
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

	// Generate comprehensive fingerprints for the row
	rowFingerprints := fingerprint.GenerateRowFingerprints(row)
	for _, fp := range rowFingerprints.ToSlice() {
		a.fingerprints.Add(fp)
	}
}

func (a *LogsStatsAccumulator) Finalize() any {
	labelNameMap := buildLabelNameMap(a.labelColumns)

	return LogsFileStats{
		FirstTS:      a.firstTS,
		LastTS:       a.lastTS,
		Fingerprints: a.fingerprints.ToSlice(),
		LabelNameMap: labelNameMap,
	}
}

// LogsFileStats contains statistics about a logs file.
type LogsFileStats struct {
	FirstTS      int64   // Earliest timestamp
	LastTS       int64   // Latest timestamp
	Fingerprints []int64 // Actual list of unique fingerprints in this file
	LabelNameMap []byte  // JSON map of label column names to dotted names
}

// ValidateLogsRow checks that a row has the required fields for logs processing.
func ValidateLogsRow(row map[string]any) error {
	if _, ok := row["chq_timestamp"]; !ok {
		return fmt.Errorf("missing required field: chq_timestamp")
	}
	return nil
}
