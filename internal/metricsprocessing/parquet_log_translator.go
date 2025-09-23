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

package metricsprocessing

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/oteltools/pkg/translate"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// ParquetLogTranslator handles Parquet-specific log translation with timestamp detection and fingerprinting
type ParquetLogTranslator struct {
	OrgID             string
	Bucket            string
	ObjectID          string
	ExemplarProcessor *exemplars.Processor
}

// flattenValue recursively flattens nested structures into dot-notation fields
func (t *ParquetLogTranslator) flattenValue(prefix string, value any, result map[wkk.RowKey]any) {
	switch v := value.(type) {
	case map[string]any:
		// Handle nested maps
		for key, nestedVal := range v {
			newPrefix := key
			if prefix != "" {
				newPrefix = prefix + "." + key
			}
			t.flattenValue(newPrefix, nestedVal, result)
		}
	case []any:
		// Handle arrays
		for idx, elem := range v {
			newPrefix := fmt.Sprintf("%s[%d]", prefix, idx)
			t.flattenValue(newPrefix, elem, result)
		}
	case nil:
		// Skip nil values
		return
	default:
		// Store the flattened value
		if prefix != "" {
			result[wkk.NewRowKey(prefix)] = value
		}
	}
}

// timestampResult contains detected timestamp information
type timestampResult struct {
	ms    int64 // milliseconds
	ns    int64 // nanoseconds (if available with higher precision)
	found bool
}

// detectTimestampField attempts to find a timestamp field in the row
// Returns both millisecond and nanosecond precision if available
func (t *ParquetLogTranslator) detectTimestampField(row *filereader.Row) timestampResult {
	// Check for nanosecond precision fields first
	nanosecondFields := []string{
		"timestamp_ns",
		"timestamp_nanos",
		"timestamp_nano",
		"tsns",
		"_cardinalhq.tsns",
	}

	for _, field := range nanosecondFields {
		key := wkk.NewRowKey(field)
		if val, exists := (*row)[key]; exists {
			switch v := val.(type) {
			case int64:
				// Found nanoseconds - return both ms and ns
				return timestampResult{
					ms:    v / 1000000, // Convert ns to ms
					ns:    v,
					found: true,
				}
			case float64:
				ns := int64(v)
				return timestampResult{
					ms:    ns / 1000000,
					ns:    ns,
					found: true,
				}
			}
		}
	}

	// Check for microsecond precision fields
	microsecondFields := []string{
		"timestamp_us",
		"timestamp_micros",
		"timestamp_micro",
	}

	for _, field := range microsecondFields {
		key := wkk.NewRowKey(field)
		if val, exists := (*row)[key]; exists {
			switch v := val.(type) {
			case int64:
				// Found microseconds - convert to both ms and ns
				return timestampResult{
					ms:    v / 1000, // Convert us to ms
					ns:    v * 1000, // Convert us to ns
					found: true,
				}
			case float64:
				us := int64(v)
				return timestampResult{
					ms:    us / 1000,
					ns:    us * 1000,
					found: true,
				}
			}
		}
	}

	// Common millisecond/general timestamp field names
	timestampFields := []string{
		"timestamp",
		"@timestamp",
		"time",
		"ts",
		"datetime",
		"date_time",
		"event_time",
		"event.time",
		"log_timestamp",
		"log.timestamp",
		"timestamp_ms",
		"timestamp_millis",
	}

	for _, field := range timestampFields {
		key := wkk.NewRowKey(field)
		if val, exists := (*row)[key]; exists {
			// Try to convert to int64 (assume milliseconds unless it's time.Time)
			switch v := val.(type) {
			case int64:
				// Assume milliseconds
				return timestampResult{
					ms:    v,
					ns:    v * 1000000, // Convert ms to ns
					found: true,
				}
			case int32:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}
			case float64:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}
			case float32:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}
			case int:
				ms := int64(v)
				return timestampResult{
					ms:    ms,
					ns:    ms * 1000000,
					found: true,
				}
			case time.Time:
				// Handle time.Time objects with full precision
				return timestampResult{
					ms:    v.UnixMilli(),
					ns:    v.UnixNano(),
					found: true,
				}
			case *time.Time:
				if v != nil {
					return timestampResult{
						ms:    v.UnixMilli(),
						ns:    v.UnixNano(),
						found: true,
					}
				}
			}
		}
	}

	// Also check for _cardinalhq.timestamp in case it's already present
	if val, exists := (*row)[wkk.RowKeyCTimestamp]; exists {
		if ts, ok := val.(int64); ok {
			return timestampResult{
				ms:    ts,
				ns:    ts * 1000000,
				found: true,
			}
		}
	}

	return timestampResult{found: false}
}

// detectMessageField attempts to find a message field in the row
func (t *ParquetLogTranslator) detectMessageField(row *filereader.Row) (string, bool) {
	// Common message field names to check (in priority order)
	messageFields := []string{
		"message",
		"msg",
		"body",
		"log",
		"log_message",
		"log.message",
		"text",
		"content",
		"event",
		"event.message",
		"raw",
		"raw_message",
	}

	for _, field := range messageFields {
		key := wkk.NewRowKey(field)
		if val, exists := (*row)[key]; exists {
			// Try to convert to string
			switch v := val.(type) {
			case string:
				if v != "" {
					return v, true
				}
			case []byte:
				if len(v) > 0 {
					return string(v), true
				}
			}
		}
	}

	// Also check for _cardinalhq.message in case it's already present
	messageKey := wkk.NewRowKey(translate.CardinalFieldMessage)
	if val, exists := (*row)[messageKey]; exists {
		if msg, ok := val.(string); ok && msg != "" {
			return msg, true
		}
	}

	return "", false
}

// TranslateRow processes Parquet rows with timestamp detection and fingerprinting
func (t *ParquetLogTranslator) TranslateRow(ctx context.Context, row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Track which fields are special (timestamp/message) so we don't duplicate them as attributes
	specialFields := make(map[wkk.RowKey]bool)

	// Detect timestamp first
	timestampResult := t.detectTimestampField(row)
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
	timestampFieldNames := []string{
		"timestamp", "@timestamp", "time", "ts", "datetime", "date_time",
		"event_time", "event.time", "log_timestamp", "log.timestamp",
		"timestamp_ms", "timestamp_millis", "timestamp_ns", "timestamp_nanos",
		"timestamp_nano", "tsns", "timestamp_us", "timestamp_micros", "timestamp_micro",
		"_cardinalhq.tsns",
	}
	for _, field := range timestampFieldNames {
		specialFields[wkk.NewRowKey(field)] = true
	}

	// Detect message
	message, messageFound := t.detectMessageField(row)
	if !messageFound {
		message = ""
	}

	// Mark message fields as special to avoid duplicating as attributes
	messageFieldNames := []string{
		"message", "msg", "body", "log", "log_message", "log.message",
		"text", "content", "event", "event.message", "raw", "raw_message",
	}
	for _, field := range messageFieldNames {
		specialFields[wkk.NewRowKey(field)] = true
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

		// Skip special fields, _cardinalhq fields, and fields already with resource. prefix
		if specialFields[key] || strings.HasPrefix(keyStr, "_cardinalhq.") || strings.HasPrefix(keyStr, "resource.") || keyStr[0] == '_' {
			// Remove special fields and underscore fields
			if specialFields[key] || keyStr[0] == '_' {
				delete(*row, key)
			}
			// Keep existing resource. fields as-is
			continue
		}

		// This field needs to be processed and prefixed with resource.
		fieldsToProcess[key] = value
		delete(*row, key) // Remove the original field
	}

	// Second pass: process and add back with resource. prefix
	for key, value := range fieldsToProcess {
		keyStr := wkk.RowKeyValue(key)

		// If it's a nested structure, flatten it with resource. prefix
		switch v := value.(type) {
		case map[string]any:
			flattened := make(map[wkk.RowKey]any)
			t.flattenValue("resource."+keyStr, v, flattened)
			maps.Copy((*row), flattened)
		case []any:
			flattened := make(map[wkk.RowKey]any)
			t.flattenValue("resource."+keyStr, v, flattened)
			maps.Copy((*row), flattened)
		default:
			// Simple value - add with resource. prefix
			(*row)[wkk.NewRowKey("resource."+keyStr)] = value
		}
	}

	// Add standard resource fields
	(*row)[wkk.NewRowKey("resource.bucket.name")] = t.Bucket
	(*row)[wkk.NewRowKey("resource.file.name")] = "./" + t.ObjectID
	(*row)[wkk.NewRowKey("resource.file.type")] = helpers.GetFileType(t.ObjectID)

	// Ensure required CardinalhQ fields are set
	(*row)[wkk.RowKeyCTelemetryType] = "logs"
	(*row)[wkk.RowKeyCName] = "log.events"
	(*row)[wkk.RowKeyCValue] = float64(1.0)

	// Set timestamp fields with proper precision
	(*row)[wkk.RowKeyCTimestamp] = timestampMs
	// Set nanosecond timestamp with preserved precision
	(*row)[wkk.NewRowKey("_cardinalhq.tsns")] = timestampNs

	// Set message field
	messageKey := wkk.NewRowKey(translate.CardinalFieldMessage)
	(*row)[messageKey] = message

	// Fingerprint the message if we have one and an exemplar processor
	if message != "" && t.ExemplarProcessor != nil {
		tenant := t.ExemplarProcessor.GetTenant(ctx, t.OrgID)
		if tenant != nil {
			trieClusterManager := tenant.GetTrieClusterManager()
			fingerprint, _, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
			if err == nil {
				(*row)[wkk.NewRowKey(translate.CardinalFieldFingerprint)] = fingerprint
			}
		}
	}

	return nil
}
