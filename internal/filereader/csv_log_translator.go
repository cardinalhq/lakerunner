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
	"regexp"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

var (
	// Pattern to sanitize field names - removes non-alphanumeric chars except underscores
	fieldSanitizeRegex = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
	// Pattern to match Unix timestamp strings (all digits)
	timestampDigitsRegex = regexp.MustCompile(`^\d+$`)
)

const (
	// timestampSecondsThreshold is used to distinguish between second-based and millisecond-based Unix timestamps.
	// Values below this threshold are assumed to be in seconds and will be converted to milliseconds.
	// 2e9 milliseconds = 2,000,000,000 ms ≈ January 24, 2033. Any real millisecond timestamp will be much larger than this value.
	// Therefore, if a timestamp value is less than 2e9, it is treated as seconds (not milliseconds).
	timestampSecondsThreshold = 2e9
)

// CSVLogTranslator handles translation for CSV log files
type CSVLogTranslator struct {
	orgID    string
	bucket   string
	objectID string
}

// NewCSVLogTranslator creates a new CSV log translator
func NewCSVLogTranslator(opts ReaderOptions) *CSVLogTranslator {
	return &CSVLogTranslator{
		orgID:    opts.OrgID,
		bucket:   opts.Bucket,
		objectID: opts.ObjectID,
	}
}

// TranslateRow handles CSV-specific field translation for logs
func (t *CSVLogTranslator) TranslateRow(ctx context.Context, row *pipeline.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Collect original keys and values, then normalize field names
	type keyVal struct {
		key   wkk.RowKey
		value any
	}
	originals := make([]keyVal, 0, len(*row))
	for k, v := range *row {
		originals = append(originals, keyVal{k, v})
	}
	// Sort for deterministic processing
	sort.Slice(originals, func(i, j int) bool {
		return wkk.RowKeyValue(originals[i].key) < wkk.RowKeyValue(originals[j].key)
	})

	// Clear and rebuild in place with normalized names
	clear(*row)
	usedNames := make(map[string]int)

	for _, kv := range originals {
		originalName := wkk.RowKeyValue(kv.key)
		normalizedName := strings.ToLower(originalName)

		finalName := normalizedName
		if count, exists := usedNames[normalizedName]; exists {
			count++
			usedNames[normalizedName] = count
			finalName = fmt.Sprintf("%s_%d", normalizedName, count)
		} else {
			usedNames[normalizedName] = 1
		}

		(*row)[wkk.NewRowKey(finalName)] = kv.value
	}

	// Special handling for "data" field as message
	dataKey := wkk.NewRowKey("data")
	logMessageKey := wkk.NewRowKey("log_message")
	if dataVal, exists := (*row)[dataKey]; exists {
		if msg, isString := dataVal.(string); isString && msg != "" {
			(*row)[logMessageKey] = msg
		}
		delete(*row, dataKey)
	}

	// Look for timestamp fields
	timestampFound := false
	var timestamp int64

	timestampFields := []string{
		"timestamp", "time", "datetime", "date",
		"publish_time", "event_timestamp", "created_at", "updated_at",
		"@timestamp", "ts", "eventtime", "event_time",
	}

	for _, fieldName := range timestampFields {
		key := wkk.NewRowKey(fieldName)
		if val, exists := (*row)[key]; exists {
			if ts := t.parseTimestamp(val); ts > 0 {
				timestamp = ts
				timestampFound = true
				delete(*row, key)
				break
			}
		}
	}

	if !timestampFound {
		timestamp = time.Now().UnixMilli()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "logs"),
			attribute.String("reason", "no_timestamp_field"),
		))
	}

	(*row)[wkk.RowKeyCTimestamp] = timestamp

	// Collect keys to rename to log_* namespace
	keysToRename := make([]wkk.RowKey, 0)
	for key := range *row {
		if key == wkk.RowKeyCTimestamp || key == logMessageKey {
			continue
		}
		keysToRename = append(keysToRename, key)
	}
	sort.Slice(keysToRename, func(i, j int) bool {
		return wkk.RowKeyValue(keysToRename[i]) < wkk.RowKeyValue(keysToRename[j])
	})

	// Rename fields to log_* namespace
	usedSanitizedNames := make(map[string]int)
	for _, key := range keysToRename {
		value := (*row)[key]
		delete(*row, key)

		originalName := wkk.RowKeyValue(key)
		sanitized := t.sanitizeFieldName(originalName)
		if sanitized == "" || sanitized == "data" {
			continue
		}

		finalSanitized := sanitized
		if count, exists := usedSanitizedNames[sanitized]; exists {
			count++
			usedSanitizedNames[sanitized] = count
			finalSanitized = fmt.Sprintf("%s_%d", sanitized, count)
		} else {
			usedSanitizedNames[sanitized] = 1
		}

		(*row)[wkk.NewRowKey("log_"+finalSanitized)] = value
	}

	// Add resource fields
	(*row)[wkk.RowKeyResourceBucketName] = t.bucket
	(*row)[wkk.RowKeyResourceFileName] = "./" + t.objectID
	(*row)[wkk.RowKeyResourceFileType] = helpers.GetFileType(t.objectID)
	(*row)[wkk.NewRowKey("resource_service_name")] = "csv-import"

	if t.orgID != "" {
		(*row)[wkk.NewRowKey("chq_organization_id")] = t.orgID
	}

	return nil
}

// parseTimestamp attempts to parse various timestamp formats
func (t *CSVLogTranslator) parseTimestamp(val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		return t.normalizeTimestamp(v)
	case float64:
		return t.normalizeTimestamp(int64(v))
	case int:
		return t.normalizeTimestamp(int64(v))
	case string:
		// Try to parse as number first
		if ts := t.parseTimestampString(v); ts > 0 {
			return ts
		}
		// Try common date formats
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			return parsed.UnixMilli()
		}
		if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return parsed.UnixMilli()
		}
		if parsed, err := time.Parse("2006-01-02 15:04:05.000000 MST", v); err == nil {
			return parsed.UnixMilli()
		}
		if parsed, err := time.Parse("2006-01-02T15:04:05.000Z", v); err == nil {
			return parsed.UnixMilli()
		}
		if parsed, err := time.Parse("2006-01-02T15:04:05Z", v); err == nil {
			return parsed.UnixMilli()
		}
		if parsed, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

// parseTimestampString attempts to parse a timestamp string as a number
func (t *CSVLogTranslator) parseTimestampString(s string) int64 {
	s = strings.TrimSpace(s)
	// Check if it looks like a Unix timestamp (all digits)
	if timestampDigitsRegex.MatchString(s) {
		var ts int64
		_, _ = fmt.Sscanf(s, "%d", &ts)
		return t.normalizeTimestamp(ts)
	}
	return 0
}

// normalizeTimestamp converts timestamps to milliseconds
// Detects nanoseconds (> 1e15), seconds (< 1e11), and milliseconds
func (t *CSVLogTranslator) normalizeTimestamp(ts int64) int64 {
	if ts <= 0 {
		return 0
	}

	// Nanoseconds (after year 2001 in nanoseconds)
	if ts > 1e15 {
		return ts / 1e6
	}

	// Seconds (before year 2033 in milliseconds)
	if ts < timestampSecondsThreshold {
		return ts * 1000
	}

	// Assume milliseconds
	return ts
}

// sanitizeFieldName removes special characters from field names and converts to lowercase
func (t *CSVLogTranslator) sanitizeFieldName(name string) string {
	// Convert to lowercase first
	name = strings.ToLower(name)

	// Replace spaces and special characters with underscores
	sanitized := fieldSanitizeRegex.ReplaceAllString(name, "_")

	// Remove leading/trailing underscores
	sanitized = strings.Trim(sanitized, "_")

	// Collapse multiple underscores
	for strings.Contains(sanitized, "__") {
		sanitized = strings.ReplaceAll(sanitized, "__", "_")
	}

	return sanitized
}
