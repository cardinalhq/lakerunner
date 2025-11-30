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
	mapset "github.com/deckarep/golang-set/v2"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

// NewTracesWriter creates a writer optimized for traces data.
// The schema must be provided from the reader and cannot be nil.
// If backendType is empty, defaults to go-parquet backend.
func NewTracesWriter(tmpdir string, schema *filereader.ReaderSchema, recordsPerFile int64, backendType parquetwriter.BackendType) (parquetwriter.ParquetWriter, error) {
	config := parquetwriter.WriterConfig{
		TmpDir: tmpdir,
		Schema: schema,

		// No grouping needed since slots are removed
		GroupKeyFunc: func(row map[string]any) any {
			return 0
		},
		NoSplitGroups: false, // Allow splitting within slots for size management

		RecordsPerFile: recordsPerFile,
		StatsProvider:  &TracesStatsProvider{},
		BackendType:    backendType,
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// TracesStatsProvider collects trace-specific statistics.
type TracesStatsProvider struct{}

func (p *TracesStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
	return &TracesStatsAccumulator{
		fingerprints:       mapset.NewSet[int64](),
		labelColumns:       mapset.NewSet[string](),
		fieldFingerprinter: fingerprint.NewFieldFingerprinter(),
	}
}

// TracesStatsAccumulator collects traces-specific statistics.
type TracesStatsAccumulator struct {
	firstTS            int64
	lastTS             int64
	first              bool
	fingerprints       mapset.Set[int64]
	labelColumns       mapset.Set[string]
	fieldFingerprinter *fingerprint.FieldFingerprinter
}

func (a *TracesStatsAccumulator) Add(row map[string]any) {
	// Track label column names for label_name_map
	for key := range row {
		if isLabelColumn(key) {
			a.labelColumns.Add(key)
		}
	}

	// Track timestamp range
	if startTime, ok := row["chq_timestamp"].(int64); ok {
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

	// Generate comprehensive fingerprints for the row
	rowFingerprints := a.fieldFingerprinter.GenerateFingerprints(row)
	for _, fp := range rowFingerprints.ToSlice() {
		a.fingerprints.Add(fp)
	}
}

func (a *TracesStatsAccumulator) Finalize() any {
	labelNameMap := buildLabelNameMap(a.labelColumns)

	return TracesFileStats{
		FirstTS:      a.firstTS,
		LastTS:       a.lastTS,
		Fingerprints: a.fingerprints.ToSlice(),
		LabelNameMap: labelNameMap,
	}
}

// TracesFileStats contains statistics about a traces file.
type TracesFileStats struct {
	FirstTS      int64   // Earliest start time
	LastTS       int64   // Latest start time
	Fingerprints []int64 // Fingerprints of spans in this file
	LabelNameMap []byte  // JSON map of label column names to dotted names
}
