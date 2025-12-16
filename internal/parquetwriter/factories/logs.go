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
// If streamField is empty, defaults to priority: resource_customer_domain > resource_service_name.
func NewLogsWriter(tmpdir string, schema *filereader.ReaderSchema, recordsPerFile int64, backendType parquetwriter.BackendType, streamField string) (parquetwriter.ParquetWriter, error) {
	config := parquetwriter.WriterConfig{
		TmpDir: tmpdir,
		Schema: schema,

		// Group by [service_identifier, fingerprint] - don't split groups with same service+fingerprint
		GroupKeyFunc:  logsGroupKeyFunc(streamField),
		NoSplitGroups: true,

		RecordsPerFile: recordsPerFile,
		StatsProvider:  &LogsStatsProvider{StreamField: streamField},
		BackendType:    backendType,
	}

	return parquetwriter.NewUnifiedWriter(config)
}

// logsGroupKeyFunc returns the grouping key function for logs.
// Groups by [service_identifier, fingerprint] - keeps all timestamps for the same
// service+fingerprint together for efficient querying.
// If streamField is specified, that field is used for service_identifier.
// Otherwise falls back to: resource_customer_domain > resource_service_name.
func logsGroupKeyFunc(streamField string) func(row pipeline.Row) any {
	return func(row pipeline.Row) any {
		serviceIdentifier := getStreamValue(row, streamField)

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

// getStreamValue extracts the stream identifier value from a row.
// If streamField is specified, that field is used.
// Otherwise falls back to: resource_customer_domain > resource_service_name.
func getStreamValue(row pipeline.Row, streamField string) string {
	if streamField != "" {
		rowKey := wkk.NewRowKey(streamField)
		if val, ok := row[rowKey].(string); ok && val != "" {
			return val
		}
		return ""
	}

	// Default fallback: resource_customer_domain > resource_service_name
	if domain, ok := row[wkk.RowKeyResourceCustomerDomain].(string); ok && domain != "" {
		return domain
	}
	if serviceName, ok := row[wkk.RowKeyResourceServiceName].(string); ok && serviceName != "" {
		return serviceName
	}
	return ""
}

// LogAggKey is the grouping key for log aggregation (10s buckets).
type LogAggKey struct {
	TimestampBucket int64  // Timestamp floored to 10s (10000ms) bucket
	LogLevel        string // Log level (info, error, etc.)
	StreamId        string // Stream identifier value
}

// LogsStatsProvider collects timestamp and fingerprint statistics for logs files.
type LogsStatsProvider struct {
	StreamField string // Optional: specific field to use for stream identification
}

func (p *LogsStatsProvider) NewAccumulator() parquetwriter.StatsAccumulator {
	return &LogsStatsAccumulator{
		fingerprints:       mapset.NewSet[int64](),
		streamValues:       mapset.NewSet[string](),
		fieldFingerprinter: fingerprint.NewFieldFingerprinter(p.StreamField),
		streamField:        p.StreamField,
		aggCounts:          make(map[LogAggKey]int64),
	}
}

// LogsStatsAccumulator collects logs-specific statistics.
type LogsStatsAccumulator struct {
	fingerprints       mapset.Set[int64]
	streamValues       mapset.Set[string]
	streamIdField      string // The field used for stream identification
	firstTS            int64
	lastTS             int64
	first              bool
	fieldFingerprinter *fingerprint.FieldFingerprinter
	missingFieldCount  int64
	streamField        string              // Configured stream field (empty = use default priority)
	aggCounts          map[LogAggKey]int64 // Aggregation counts for 10s buckets
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

	// Track stream field and values
	if a.streamField != "" {
		// Use configured stream field
		a.streamIdField = a.streamField
		if val, ok := row[wkk.NewRowKey(a.streamField)].(string); ok && val != "" {
			a.streamValues.Add(val)
		} else {
			a.missingFieldCount++
		}
	} else {
		// Default priority: resource_customer_domain > resource_service_name
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

	// Aggregation: floor timestamp to 10s bucket and count
	if ts, ok := row[wkk.RowKeyCTimestamp].(int64); ok {
		bucket := (ts / 10000) * 10000 // 10s = 10000ms
		logLevel, _ := row[wkk.RowKeyCLevel].(string)
		streamId := getStreamValue(row, a.streamField)
		key := LogAggKey{TimestampBucket: bucket, LogLevel: logLevel, StreamId: streamId}
		a.aggCounts[key]++
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
		AggCounts:               a.aggCounts,
	}
}

// LogsFileStats contains statistics about a logs file.
type LogsFileStats struct {
	FirstTS                 int64               // Earliest timestamp
	LastTS                  int64               // Latest timestamp
	Fingerprints            []int64             // Actual list of unique fingerprints in this file
	StreamIdField           *string             // The field used for stream identification (resource_customer_domain or resource_service_name)
	StreamValues            []string            // Unique values from the stream field
	MissingStreamFieldCount int64               // Count of rows missing stream identification fields
	AggCounts               map[LogAggKey]int64 // Aggregation counts for 10s buckets (timestamp, log_level, stream_id) -> count
}
