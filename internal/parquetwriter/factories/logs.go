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

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// NewLogsWriter creates a writer optimized for logs data.
// Logs are sorted by [service_identifier, fingerprint, timestamp] and grouped
// by [service_identifier, fingerprint] to keep related log entries together.
// The schema must be provided from the reader and cannot be nil.
// If backendType is empty, defaults to go-parquet backend.
func NewLogsWriter(tmpdir string, schema *filereader.ReaderSchema, recordsPerFile int64, backendType parquetwriter.BackendType) (parquetwriter.ParquetWriter, error) {
	config := parquetwriter.WriterConfig{
		TmpDir: tmpdir,
		Schema: schema,

		// Group by [service_identifier, fingerprint] - don't split groups with same service+fingerprint
		GroupKeyFunc:  logsGroupKeyFunc(),
		NoSplitGroups: true,

		RecordsPerFile: recordsPerFile,
		StatsProvider:  &LogsStatsProvider{},
		BackendType:    backendType,
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// logsGroupKeyFunc returns the grouping key function for logs.
// Groups by [service_identifier, fingerprint] - keeps all timestamps for the same
// service+fingerprint together for efficient querying.
// service_identifier is resource_customer_domain if set, otherwise resource_service_name.
func logsGroupKeyFunc() func(row pipeline.Row) any {
	return func(row pipeline.Row) any {
		// Get service identifier: customer_domain takes priority, then service_name, then empty
		serviceIdentifier := ""
		if domain, ok := row[wkk.RowKeyResourceCustomerDomain].(string); ok && domain != "" {
			serviceIdentifier = domain
		} else if serviceName, ok := row[wkk.RowKeyResourceServiceName].(string); ok && serviceName != "" {
			serviceIdentifier = serviceName
		}

		// Get fingerprint
		fingerprint, fpOk := row[wkk.RowKeyCFingerprint].(int64)
		if !fpOk {
			// If no fingerprint, still group by service identifier
			return serviceIdentifier
		}

		// Group by [service_identifier, fingerprint]
		return fmt.Sprintf("%s:%d", serviceIdentifier, fingerprint)
	}
}

// LogsStatsProvider collects timestamp and fingerprint statistics for logs files.
type LogsStatsProvider struct{}

func (p *LogsStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
	return &LogsStatsAccumulator{
		fingerprints:       mapset.NewSet[int64](),
		streamIDs:          mapset.NewSet[string](),
		fieldFingerprinter: fingerprint.NewFieldFingerprinter(),
	}
}

// LogsStatsAccumulator collects logs-specific statistics.
type LogsStatsAccumulator struct {
	fingerprints       mapset.Set[int64]
	streamIDs          mapset.Set[string]
	firstTS            int64
	lastTS             int64
	first              bool
	fieldFingerprinter *fingerprint.FieldFingerprinter
}

func (a *LogsStatsAccumulator) Add(row pipeline.Row) {
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
	}

	fps := a.fieldFingerprinter.GenerateFingerprints(row)
	a.fingerprints.Append(fps...)

	// Track stream_id if present
	if streamID, ok := row[wkk.RowKeyCStreamID].(string); ok && streamID != "" {
		a.streamIDs.Add(streamID)
	}
}

func (a *LogsStatsAccumulator) Finalize() any {
	return LogsFileStats{
		FirstTS:      a.firstTS,
		LastTS:       a.lastTS,
		Fingerprints: a.fingerprints.ToSlice(),
		StreamIDs:    a.streamIDs.ToSlice(),
	}
}

// LogsFileStats contains statistics about a logs file.
type LogsFileStats struct {
	FirstTS      int64    // Earliest timestamp
	LastTS       int64    // Latest timestamp
	Fingerprints []int64  // Actual list of unique fingerprints in this file
	StreamIDs    []string // Unique stream identifiers in this file
}

// ValidateLogsRow checks that a row has the required fields for logs processing.
func ValidateLogsRow(row map[string]any) error {
	if _, ok := row["chq_timestamp"]; !ok {
		return fmt.Errorf("missing required field: chq_timestamp")
	}
	return nil
}
