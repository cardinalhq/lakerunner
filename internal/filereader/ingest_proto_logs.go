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

package filereader

import (
	"context"
	"fmt"
	"io"
	"maps"

	"github.com/cardinalhq/lakerunner/internal/exemplars"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/translate"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// IngestProtoLogsReader reads rows from OpenTelemetry protobuf logs format.
//
// Implements OTELLogsProvider interface.
type IngestProtoLogsReader struct {
	closed    bool
	rowCount  int64
	batchSize int

	// Streaming iterator state for logs
	logs              *plog.Logs
	resourceIndex     int
	scopeIndex        int
	logIndex          int
	exemplarProcessor *exemplars.Processor
	orgId             string
}

var _ Reader = (*IngestProtoLogsReader)(nil)
var _ OTELLogsProvider = (*IngestProtoLogsReader)(nil)

// NewIngestProtoLogsReader creates a new IngestProtoLogsReader for the given io.Reader.
func NewIngestProtoLogsReader(reader io.Reader, opts ReaderOptions) (*IngestProtoLogsReader, error) {
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	protoReader := &IngestProtoLogsReader{
		batchSize:         batchSize,
		exemplarProcessor: opts.ExemplarProcessor,
		orgId:             opts.OrgID,
	}

	logs, err := parseProtoToOtelLogs(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL logs: %w", err)
	}
	protoReader.logs = logs

	return protoReader, nil
}

// Next returns the next batch of rows from the OTEL logs.
func (r *IngestProtoLogsReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		row, err := r.getLogRow(ctx)
		if err != nil {
			if err == io.EOF {
				if batch.Len() == 0 {
					pipeline.ReturnBatch(batch)
					return nil, io.EOF
				}
				break
			}
			pipeline.ReturnBatch(batch)
			return nil, err
		}

		rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoLogsReader"),
		))

		batchRow := batch.AddRow()
		maps.Copy(batchRow, row)
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoLogsReader"),
		))
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// getLogRow handles reading the next log row.
func (r *IngestProtoLogsReader) getLogRow(ctx context.Context) (Row, error) {
	if r.logs == nil {
		return nil, io.EOF
	}

	// Iterator pattern: advance through resources -> scopes -> logs
	for r.resourceIndex < r.logs.ResourceLogs().Len() {
		rl := r.logs.ResourceLogs().At(r.resourceIndex)

		for r.scopeIndex < rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(r.scopeIndex)

			if r.logIndex < sl.LogRecords().Len() {
				logRecord := sl.LogRecords().At(r.logIndex)
				row := r.buildLogRow(ctx, rl, sl, logRecord)
				if r.exemplarProcessor != nil {
					err := r.exemplarProcessor.ProcessLogs(ctx, r.orgId, rl, sl, logRecord)
					if err != nil {
						continue // Skip exemplar errors
					}
				}
				r.logIndex++
				return r.processRow(row)
			}

			r.scopeIndex++
			r.logIndex = 0
		}

		r.resourceIndex++
		r.scopeIndex = 0
		r.logIndex = 0
	}

	return nil, io.EOF
}

// buildLogRow creates a row from a single log record and its context.
func (r *IngestProtoLogsReader) buildLogRow(ctx context.Context, rl plog.ResourceLogs, sl plog.ScopeLogs, logRecord plog.LogRecord) map[string]any {
	ret := map[string]any{}

	rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "resource")] = value
		return true
	})

	sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "scope")] = value
		return true
	})

	logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "log")] = value
		return true
	})

	message := logRecord.Body().AsString()
	ret["_cardinalhq_message"] = message
	ret["_cardinalhq_timestamp"] = logRecord.Timestamp().AsTime().UnixMilli()
	ret["_cardinalhq_tsns"] = int64(logRecord.Timestamp())
	ret["observed_timestamp"] = logRecord.ObservedTimestamp().AsTime().UnixMilli()
	ret["_cardinalhq_level"] = logRecord.SeverityText()
	ret["severity_number"] = int64(logRecord.SeverityNumber())
	if r.exemplarProcessor != nil {
		tenant := r.exemplarProcessor.GetTenant(ctx, r.orgId)
		trieClusterManager := tenant.GetTrieClusterManager()
		fingerprint, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
		if err == nil {
			ret["_cardinalhq_fingerprint"] = fingerprint
		}
		logRecord.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
	}
	return ret
}

func (r *IngestProtoLogsReader) processRow(row map[string]any) (Row, error) {
	result := make(Row)
	for k, v := range row {
		result[wkk.NewRowKey(k)] = v
	}
	return result, nil
}

// GetOTELLogs returns the underlying parsed OTEL logs structure.
// This allows access to the original log body and metadata not available in the row format.
func (r *IngestProtoLogsReader) GetOTELLogs() (*plog.Logs, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}
	if r.logs == nil {
		return nil, fmt.Errorf("no logs data available")
	}
	return r.logs, nil
}

// Close closes the reader and releases resources.
func (r *IngestProtoLogsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.logs = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *IngestProtoLogsReader) TotalRowsReturned() int64 {
	return r.rowCount
}

func parseProtoToOtelLogs(reader io.Reader) (*plog.Logs, error) {
	unmarshaler := &plog.ProtoUnmarshaler{}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	logs, err := unmarshaler.UnmarshalLogs(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf logs: %w", err)
	}

	return &logs, nil
}
