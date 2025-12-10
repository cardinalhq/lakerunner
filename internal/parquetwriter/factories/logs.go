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
		streamValues:       mapset.NewSet[string](),
		fieldFingerprinter: fingerprint.NewFieldFingerprinter(),
	}
}

// LogsStatsAccumulator collects logs-specific statistics.
type LogsStatsAccumulator struct {
	fingerprints       mapset.Set[int64]
	streamValues       mapset.Set[string]
	streamIdField      string // "resource_customer_domain", "resource_service_name", or empty
	firstTS            int64
	lastTS             int64
	first              bool
	fieldFingerprinter *fingerprint.FieldFingerprinter
	missingFieldCount  int64
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

	// Track stream field and values with priority:
	// resource_customer_domain > resource_service_name
	customerDomainField := wkk.RowKeyValue(wkk.RowKeyResourceCustomerDomain)
	serviceNameField := wkk.RowKeyValue(wkk.RowKeyResourceServiceName)

	if domain, ok := row[wkk.RowKeyResourceCustomerDomain].(string); ok && domain != "" {
		// Highest priority: resource_customer_domain
		if a.streamIdField != customerDomainField {
			// Upgrade to higher priority field - clear previous values
			a.streamIdField = customerDomainField
			a.streamValues.Clear()
		}
		a.streamValues.Add(domain)
	} else if serviceName, ok := row[wkk.RowKeyResourceServiceName].(string); ok && serviceName != "" {
		// Second priority: resource_service_name (only if we haven't seen customer_domain)
		if a.streamIdField == "" {
			a.streamIdField = serviceNameField
		}
		if a.streamIdField == serviceNameField {
			a.streamValues.Add(serviceName)
		}
		// If streamIdField is customer_domain, ignore service_name values
	} else {
		// Neither field present
		a.missingFieldCount++
	}
}

func (a *LogsStatsAccumulator) Finalize() any {
	var streamIdField *string
	var streamValues []string

	if a.streamIdField != "" && a.streamValues.Cardinality() > 0 {
		streamIdField = &a.streamIdField
		streamValues = a.streamValues.ToSlice()
	}

	return LogsFileStats{
		FirstTS:                 a.firstTS,
		LastTS:                  a.lastTS,
		Fingerprints:            a.fingerprints.ToSlice(),
		StreamIdField:           streamIdField,
		StreamValues:            streamValues,
		MissingStreamFieldCount: a.missingFieldCount,
	}
}

// LogsFileStats contains statistics about a logs file.
type LogsFileStats struct {
	FirstTS                 int64    // Earliest timestamp
	LastTS                  int64    // Latest timestamp
	Fingerprints            []int64  // Actual list of unique fingerprints in this file
	StreamIdField           *string  // The field used for stream identification (resource_customer_domain or resource_service_name)
	StreamValues            []string // Unique values from the stream field
	MissingStreamFieldCount int64    // Count of rows missing stream identification fields
}
