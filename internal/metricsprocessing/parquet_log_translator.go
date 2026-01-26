// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package metricsprocessing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// Pre-defined well-known keys for field detection
var (
	// timestampNsKey: "timestamp_ns" (nanoseconds)
	timestampNsKey = wkk.NewRowKey("timestamp_ns")

	// timestampNanosKey: "timestamp_nanos" (nanoseconds)
	timestampNanosKey = wkk.NewRowKey("timestamp_nanos")

	// timestampNanoKey: "timestamp_nano" (nanoseconds)
	timestampNanoKey = wkk.NewRowKey("timestamp_nano")

	// tsnsKey: "tsns" (nanoseconds)
	tsnsKey = wkk.NewRowKey("tsns")

	// timestampUsKey: "timestamp_us" (microseconds)
	timestampUsKey = wkk.NewRowKey("timestamp_us")

	// timestampMicrosKey: "timestamp_micros" (microseconds)
	timestampMicrosKey = wkk.NewRowKey("timestamp_micros")

	// timestampMicroKey: "timestamp_micro" (microseconds)
	timestampMicroKey = wkk.NewRowKey("timestamp_micro")

	// timestampKey: "timestamp" (milliseconds)
	timestampKey = wkk.NewRowKey("timestamp")

	// atTimestampKey: "@timestamp" (Elastic-style)
	atTimestampKey = wkk.NewRowKey("@timestamp")

	// timeKey: "time"
	timeKey = wkk.NewRowKey("time")

	// tsKey: "ts"
	tsKey = wkk.NewRowKey("ts")

	// datetimeKey: "datetime"
	datetimeKey = wkk.NewRowKey("datetime")

	// dateTimeKey: "date_time"
	dateTimeKey = wkk.NewRowKey("date_time")

	// eventTimeKey: "event_time"
	eventTimeKey = wkk.NewRowKey("event_time")

	// eventDotTimeKey: "event.time"
	eventDotTimeKey = wkk.NewRowKey("event.time")

	// logTimestampKey: "log_timestamp"
	logTimestampKey = wkk.NewRowKey("log_timestamp")

	// logDotTimestampKey: "log.timestamp"
	logDotTimestampKey = wkk.NewRowKey("log.timestamp")

	// timestampMsKey: "timestamp_ms" (milliseconds)
	timestampMsKey = wkk.NewRowKey("timestamp_ms")

	// timestampMillisKey: "timestamp_millis" (milliseconds)
	timestampMillisKey = wkk.NewRowKey("timestamp_millis")

	// messageKey: "message"
	messageKey = wkk.NewRowKey("message")

	// msgKey: "msg"
	msgKey = wkk.NewRowKey("msg")

	// bodyKey: "body"
	bodyKey = wkk.NewRowKey("body")

	// logKey: "log"
	logKey = wkk.NewRowKey("log")

	// logMessageKey: "log_message"
	logMessageKey = wkk.NewRowKey("log_message")

	// logDotMessageKey: "log.message"
	logDotMessageKey = wkk.NewRowKey("log.message")

	// textKey: "text"
	textKey = wkk.NewRowKey("text")

	// contentKey: "content"
	contentKey = wkk.NewRowKey("content")

	// eventKey: "event"
	eventKey = wkk.NewRowKey("event")

	// eventDotMessageKey: "event.message"
	eventDotMessageKey = wkk.NewRowKey("event.message")

	// rawKey: "raw"
	rawKey = wkk.NewRowKey("raw")

	// rawMessageKey: "raw_message"
	rawMessageKey = wkk.NewRowKey("raw_message")

	// severityTextKey: "severity_text"
	severityTextKey = wkk.NewRowKey("severity_text")

	// severityTextAltKey: "severityText" (camelCase)
	severityTextAltKey = wkk.NewRowKey("severityText")

	// severityDotTextKey: "severity.text"
	severityDotTextKey = wkk.NewRowKey("severity.text")

	// levelKey: "level"
	levelKey = wkk.NewRowKey("level")

	// logLevelKey: "log_level"
	logLevelKey = wkk.NewRowKey("log_level")

	// logDotLevelKey: "log.level"
	logDotLevelKey = wkk.NewRowKey("log.level")

	// loglevelKey: "loglevel"
	loglevelKey = wkk.NewRowKey("loglevel")

	// logLevelCamelKey: "logLevel" (camelCase)
	logLevelCamelKey = wkk.NewRowKey("logLevel")

	// severityKey: "severity"
	severityKey = wkk.NewRowKey("severity")

	// sevKey: "sev"
	sevKey = wkk.NewRowKey("sev")

	// lvlKey: "lvl"
	lvlKey = wkk.NewRowKey("lvl")
)

// ParquetLogTranslatingReader wraps a reader and translates parquet logs to CardinalHQ format.
// It properly transforms the schema to reflect row transformations (resource_ prefixing, etc.)
type ParquetLogTranslatingReader struct {
	wrapped     filereader.Reader
	orgID       string
	bucket      string
	objectID    string
	schema      *filereader.ReaderSchema
	closed      bool
	rowCount    int64
	schemaBuilt bool
}

// NewParquetLogTranslatingReader creates a reader that translates parquet logs
func NewParquetLogTranslatingReader(wrapped filereader.Reader, orgID, bucket, objectID string) *ParquetLogTranslatingReader {
	// Get source schema from wrapped reader (which should be fully built in its constructor)
	sourceSchema := wrapped.GetSchema()
	if sourceSchema == nil {
		sourceSchema = filereader.NewReaderSchema()
	}

	r := &ParquetLogTranslatingReader{
		wrapped:  wrapped,
		orgID:    orgID,
		bucket:   bucket,
		objectID: objectID,
	}

	// Build the transformed schema immediately in the constructor
	r.schema = r.transformSchema(sourceSchema)
	r.schemaBuilt = true

	return r
}

var _ filereader.Reader = (*ParquetLogTranslatingReader)(nil)

// timestampResult contains detected timestamp information
type timestampResult struct {
	ms    int64 // milliseconds
	ns    int64 // nanoseconds (if available with higher precision)
	found bool
}

// detectTimestampField attempts to find a timestamp field in the row
// Returns both millisecond and nanosecond precision if available, plus the field name that was used
func detectTimestampField(row *pipeline.Row) (timestampResult, string) {
	// Check for nanosecond precision fields first
	nanosecondFields := []wkk.RowKey{
		timestampNsKey,
		timestampNanosKey,
		timestampNanoKey,
		tsnsKey,
		wkk.RowKeyCTsns,
	}

	for _, key := range nanosecondFields {
		if val, exists := (*row)[key]; exists {
			switch v := val.(type) {
			case int64:
				// Found nanoseconds - return both ms and ns
				return timestampResult{
					ms:    v / 1000000, // Convert ns to ms
					ns:    v,
					found: true,
				}, wkk.RowKeyValue(key)
			case float64:
				ns := int64(v)
				return timestampResult{
					ms:    ns / 1000000,
					ns:    ns,
					found: true,
				}, wkk.RowKeyValue(key)
			}
		}
	}

	// Check for microsecond precision fields
	microsecondFields := []wkk.RowKey{
		timestampUsKey,
		timestampMicrosKey,
		timestampMicroKey,
	}

	for _, key := range microsecondFields {
		if val, exists := (*row)[key]; exists {
			switch v := val.(type) {
			case int64:
				// Found microseconds - convert to both ms and ns
				return timestampResult{
					ms:    v / 1000, // Convert us to ms
					ns:    v * 1000, // Convert us to ns
					found: true,
				}, wkk.RowKeyValue(key)
			case float64:
				us := int64(v)
				return timestampResult{
					ms:    us / 1000,
					ns:    us * 1000,
					found: true,
				}, wkk.RowKeyValue(key)
			}
		}
	}

	// Common millisecond/general timestamp field names
	timestampFields := []wkk.RowKey{
		timestampKey,
		atTimestampKey,
		timeKey,
		tsKey,
		datetimeKey,
		dateTimeKey,
		eventTimeKey,
		eventDotTimeKey,
		logTimestampKey,
		logDotTimestampKey,
		timestampMsKey,
		timestampMillisKey,
	}

	for _, key := range timestampFields {
		if val, exists := (*row)[key]; exists {
			// Try to convert to int64 (assume milliseconds unless it's time.Time)
			switch v := val.(type) {
			case int64:
				// Assume milliseconds
				return timestampResult{
					ms:    v,
					ns:    v * 1000000, // Convert ms to ns
					found: true,
				}, wkk.RowKeyValue(key)
			case int32:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}, wkk.RowKeyValue(key)
			case float64:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}, wkk.RowKeyValue(key)
			case float32:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}, wkk.RowKeyValue(key)
			case int:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}, wkk.RowKeyValue(key)
			case time.Time:
				// Handle time.Time objects with full precision
				return timestampResult{
					ms:    v.UnixMilli(),
					ns:    v.UnixNano(),
					found: true,
				}, wkk.RowKeyValue(key)
			case arrow.Timestamp:
				// Handle Arrow timestamps - value is already in milliseconds
				return timestampResult{
					ms:    int64(v),
					ns:    int64(v) * 1000000, // Convert ms to ns
					found: true,
				}, wkk.RowKeyValue(key)
			default:
				// Try to extract int64 value from other types (like arrow types)
				if tsValue, ok := extractInt64(v); ok {
					return timestampResult{
						ms:    tsValue,
						ns:    tsValue * 1000000, // Convert ms to ns
						found: true,
					}, wkk.RowKeyValue(key)
				}
			case *time.Time:
				if v != nil {
					return timestampResult{
						ms:    v.UnixMilli(),
						ns:    v.UnixNano(),
						found: true,
					}, wkk.RowKeyValue(key)
				}
			}
		}
	}

	// Also check for chq_timestamp in case it's already present
	if val, exists := (*row)[wkk.RowKeyCTimestamp]; exists {
		if ts, ok := val.(int64); ok {
			return timestampResult{
				ms:    ts,
				ns:    ts * 1000000,
				found: true,
			}, "chq_timestamp"
		}
	}

	return timestampResult{found: false}, ""
}

// extractInt64 attempts to extract int64 value from numeric types only
// Does NOT attempt to parse string representations to avoid parsing
// human-readable timestamps like "2025-09-13 18:09:15" as "2025"
func extractInt64(val any) (int64, bool) {
	switch v := val.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case uint32:
		return int64(v), true
	}
	return 0, false
}

// detectLevelField attempts to find a severity/level field in the row
// Returns the level string (uppercase), a boolean indicating if found, and the field name used
func detectLevelField(row *pipeline.Row) (string, bool, string) {
	// Check common level/severity field names
	levelFields := []wkk.RowKey{
		severityTextKey,
		severityTextAltKey,
		severityDotTextKey,
		levelKey,
		logLevelKey,
		logDotLevelKey,
		loglevelKey,
		logLevelCamelKey,
		severityKey,
		sevKey,
		lvlKey,
	}

	for _, key := range levelFields {
		if val, exists := (*row)[key]; exists {
			switch v := val.(type) {
			case string:
				if v != "" {
					return strings.ToUpper(v), true, wkk.RowKeyValue(key)
				}
			case []byte:
				if len(v) > 0 {
					return strings.ToUpper(string(v)), true, wkk.RowKeyValue(key)
				}
			}
		}
	}

	return "", false, ""
}

// detectMessageField attempts to find a message field in the row
// Returns the message string, a boolean indicating if found, and the field name used
func detectMessageField(row *pipeline.Row) (string, bool, string) {
	// Common message field names to check (in priority order)
	messageFields := []wkk.RowKey{
		messageKey,
		msgKey,
		bodyKey,
		logKey,
		logMessageKey,
		logDotMessageKey,
		textKey,
		contentKey,
		eventKey,
		eventDotMessageKey,
		rawKey,
		rawMessageKey,
	}

	for _, key := range messageFields {
		if val, exists := (*row)[key]; exists {
			// Try to convert to string
			switch v := val.(type) {
			case string:
				if v != "" {
					return v, true, wkk.RowKeyValue(key)
				}
			case []byte:
				if len(v) > 0 {
					return string(v), true, wkk.RowKeyValue(key)
				}
			}
		}
	}

	// Also check for log_message in case it's already present
	if val, exists := (*row)[wkk.RowKeyCMessage]; exists {
		if msg, ok := val.(string); ok && msg != "" {
			return msg, true, "log_message"
		}
	}

	return "", false, ""
}

// translateParquetLogRow processes Parquet rows with timestamp detection and fingerprinting.
// This is a standalone function that can be used by both the Reader implementation and
// any compatibility wrappers.
func translateParquetLogRow(_ context.Context, row *pipeline.Row, bucket, objectID string) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Track which fields are special (timestamp/message) so we don't duplicate them as attributes
	specialFields := make(map[wkk.RowKey]bool)

	// Detect timestamp first before any field processing
	timestampResult, usedTimestampField := detectTimestampField(row)
	var timestampMs, timestampNs int64
	if timestampResult.found {
		timestampMs = timestampResult.ms
		timestampNs = timestampResult.ns
	} else {
		// Default to current time if no timestamp found (matching Scala behavior)
		now := time.Now()
		timestampMs = now.UnixMilli()
		timestampNs = now.UnixNano()
	}

	// Mark timestamp fields as special to avoid duplicating as attributes
	timestampFieldKeys := []wkk.RowKey{
		timestampKey, atTimestampKey, timeKey, tsKey, datetimeKey, dateTimeKey,
		eventTimeKey, eventDotTimeKey, logTimestampKey, logDotTimestampKey,
		timestampMsKey, timestampMillisKey, timestampNsKey, timestampNanosKey,
		timestampNanoKey, tsnsKey, timestampUsKey, timestampMicrosKey, timestampMicroKey,
		wkk.RowKeyCTsns,
	}
	for _, key := range timestampFieldKeys {
		specialFields[key] = true
	}

	// Detect message
	message, messageFound, usedMessageField := detectMessageField(row)
	if !messageFound {
		message = ""
	}

	// Mark message fields as special to avoid duplicating as attributes
	messageFieldKeys := []wkk.RowKey{
		messageKey, msgKey, bodyKey, logKey, logMessageKey, logDotMessageKey,
		textKey, contentKey, eventKey, eventDotMessageKey, rawKey, rawMessageKey,
	}
	for _, key := range messageFieldKeys {
		specialFields[key] = true
	}

	// Detect level/severity
	level, levelFound, usedLevelField := detectLevelField(row)
	if !levelFound {
		// Default to INFO if no level found
		level = "INFO"
	}

	// Mark level/severity fields as special to avoid duplicating as attributes
	levelFieldKeys := []wkk.RowKey{
		severityTextKey, severityTextAltKey, severityDotTextKey,
		levelKey, logLevelKey, logDotLevelKey,
		loglevelKey, logLevelCamelKey, severityKey,
		sevKey, lvlKey,
	}
	for _, key := range levelFieldKeys {
		specialFields[key] = true
	}

	// Process all other fields as attributes (matching OTLP collector behavior)
	// First pass: collect all fields that need processing
	fieldsToProcess := make(map[wkk.RowKey]any)
	for key, value := range *row {
		// Get the string representation of the key
		keyStr := wkk.RowKeyValue(key)
		if keyStr == "" {
			delete(*row, key) // Remove empty keys
			continue
		}

		// Skip underscore fields, resource_ fields, and chq_ fields (keep them as-is)
		if strings.HasPrefix(keyStr, "resource_") || strings.HasPrefix(keyStr, "chq_") || keyStr[0] == '_' {
			// Remove underscore fields but keep existing resource_ and chq_ fields
			if keyStr[0] == '_' {
				delete(*row, key)
			}
			// Keep existing resource_ and chq_ fields as-is
			continue
		}

		// Special fields get processed as resource attributes but the original field is removed
		// However, exclude the specific fields that were used for timestamp, message, or level detection
		if specialFields[key] {
			if keyStr != usedTimestampField && keyStr != usedMessageField && keyStr != usedLevelField {
				fieldsToProcess[key] = value
			}
			delete(*row, key) // Remove the original special field
			continue
		}

		// This field needs to be processed and prefixed with resource.
		fieldsToProcess[key] = value
		delete(*row, key) // Remove the original field
	}

	// Second pass: process and add back with resource. prefix
	// Note: Flattening is already done by IngestLogParquetReader, so all values are already flat
	for key, value := range fieldsToProcess {
		keyStr := wkk.RowKeyValue(key)
		// Add with resource_ prefix (no flattening needed - already done by reader)
		(*row)[wkk.NewRowKey("resource_"+keyStr)] = value
	}

	// Add standard resource fields
	resourceFile := getResourceFile(objectID)
	(*row)[wkk.RowKeyResourceBucketName] = bucket
	(*row)[wkk.RowKeyResourceFileName] = "./" + objectID
	(*row)[wkk.RowKeyResourceFile] = resourceFile
	(*row)[wkk.RowKeyResourceFileType] = helpers.GetFileType(objectID)

	// Extract customer domain if pattern matches
	var customerDomain string
	if customerDomain = helpers.ExtractCustomerDomain(resourceFile); customerDomain != "" {
		(*row)[wkk.RowKeyResourceCustomerDomain] = customerDomain
	}

	// Ensure required CardinalhQ fields are set
	(*row)[wkk.RowKeyCTelemetryType] = "logs"
	(*row)[wkk.RowKeyCName] = "log_events"
	(*row)[wkk.RowKeyCValue] = float64(1.0)

	// Set timestamp fields with proper precision
	(*row)[wkk.RowKeyCTimestamp] = timestampMs
	// Set nanosecond timestamp with preserved precision
	(*row)[wkk.RowKeyCTsns] = timestampNs

	// Set message field
	(*row)[wkk.RowKeyCMessage] = message

	// Set level field
	(*row)[wkk.RowKeyCLevel] = level

	return nil
}

// Next reads and translates the next batch of rows
func (r *ParquetLogTranslatingReader) Next(ctx context.Context) (*filereader.Batch, error) {
	if r.closed {
		return nil, errors.New("reader is closed")
	}

	// Get batch from wrapped reader
	batch, err := r.wrapped.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("wrapped reader error: %w", err)
	}
	if batch == nil {
		return nil, io.EOF
	}

	// Translate each row in place
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if err := translateParquetLogRow(ctx, &row, r.bucket, r.objectID); err != nil {
			// Log error but don't fail the entire batch
			// In production, we may want to track these failures
			continue
		}
	}

	r.rowCount += int64(batch.Len())
	return batch, nil
}

// GetSchema returns the transformed schema
func (r *ParquetLogTranslatingReader) GetSchema() *filereader.ReaderSchema {
	// Schema is built in constructor, just return a copy
	return r.schema.Copy()
}

// transformSchema applies the same field transformations to the schema that TranslateRow applies to rows
func (r *ParquetLogTranslatingReader) transformSchema(source *filereader.ReaderSchema) *filereader.ReaderSchema {
	transformed := filereader.NewReaderSchema()

	// Process each column from source schema
	for _, col := range source.Columns() {
		colName := string(col.Name.Value())

		// Skip underscore fields (removed by translator)
		if strings.HasPrefix(colName, "_") {
			continue
		}

		// Keep resource_ and chq_ fields as-is
		if strings.HasPrefix(colName, "resource_") || strings.HasPrefix(colName, "chq_") {
			transformed.AddColumn(col.Name, col.Name, col.DataType, col.HasNonNull)
			continue
		}

		// All other fields get resource_ prefix
		newName := wkk.NewRowKey("resource_" + colName)
		transformed.AddColumn(newName, col.Name, col.DataType, col.HasNonNull)
	}

	// Add standard fields that translator adds
	transformed.AddColumn(wkk.RowKeyResourceBucketName, wkk.RowKeyResourceBucketName, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyResourceFileName, wkk.RowKeyResourceFileName, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyResourceFile, wkk.RowKeyResourceFile, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyResourceFileType, wkk.RowKeyResourceFileType, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyCTelemetryType, wkk.RowKeyCTelemetryType, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyCName, wkk.RowKeyCName, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyCValue, wkk.RowKeyCValue, filereader.DataTypeFloat64, true)
	transformed.AddColumn(wkk.RowKeyCTimestamp, wkk.RowKeyCTimestamp, filereader.DataTypeInt64, true)
	transformed.AddColumn(wkk.RowKeyCTsns, wkk.RowKeyCTsns, filereader.DataTypeInt64, true)
	transformed.AddColumn(wkk.RowKeyCMessage, wkk.RowKeyCMessage, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyCLevel, wkk.RowKeyCLevel, filereader.DataTypeString, true)

	// Fields we add conditionally but must be in schema for parquet writer
	transformed.AddColumn(wkk.RowKeyResourceCustomerDomain, wkk.RowKeyResourceCustomerDomain, filereader.DataTypeString, true)
	transformed.AddColumn(wkk.RowKeyCFingerprint, wkk.RowKeyCFingerprint, filereader.DataTypeInt64, true)
	transformed.AddColumn(wkk.RowKeyCID, wkk.RowKeyCID, filereader.DataTypeString, true) // Added by FileSplitter

	return transformed
}

// Close closes the wrapped reader
func (r *ParquetLogTranslatingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.wrapped != nil {
		return r.wrapped.Close()
	}
	return nil
}

// TotalRowsReturned returns the total number of rows successfully returned
func (r *ParquetLogTranslatingReader) TotalRowsReturned() int64 {
	return r.rowCount
}
