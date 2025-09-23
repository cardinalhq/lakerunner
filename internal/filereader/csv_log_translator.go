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
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

var (
	// Pattern to sanitize field names - removes non-alphanumeric chars except dots and underscores
	fieldSanitizeRegex = regexp.MustCompile(`[^a-zA-Z0-9._]+`)
	// Pattern to match Unix timestamp strings (all digits)
	timestampDigitsRegex = regexp.MustCompile(`^\d+$`)
)

const (
	// timestampSecondsThreshold represents the cutoff point between seconds and milliseconds
	// Values below this are assumed to be in seconds and will be converted to milliseconds
	// 2e9 milliseconds = 2,000,000,000 ms ≈ January 24, 2033 in milliseconds
	// This ensures timestamps before 2033 in millisecond format are treated as seconds
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
func (t *CSVLogTranslator) TranslateRow(ctx context.Context, row *Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// First, normalize all field names to lowercase with deduplication
	// Sort keys to ensure deterministic processing order
	keys := make([]wkk.RowKey, 0, len(*row))
	for key := range *row {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return wkk.RowKeyValue(keys[i]) < wkk.RowKeyValue(keys[j])
	})

	normalizedRow := make(Row)
	usedNames := make(map[string]int) // Track how many times each lowercased name has been used

	for _, key := range keys {
		value := (*row)[key]
		originalName := wkk.RowKeyValue(key)
		normalizedName := strings.ToLower(originalName)

		// Handle duplicates by adding numbered suffixes
		finalName := normalizedName
		if count, exists := usedNames[normalizedName]; exists {
			count++
			usedNames[normalizedName] = count
			finalName = fmt.Sprintf("%s_%d", normalizedName, count)
		} else {
			usedNames[normalizedName] = 1
		}

		normalizedKey := wkk.NewRowKey(finalName)
		normalizedRow[normalizedKey] = value
	}
	*row = normalizedRow

	// Special handling for "data" field as message
	dataKey := wkk.NewRowKey("data")
	if dataVal, exists := (*row)[dataKey]; exists {
		// Set as message
		if msg, isString := dataVal.(string); isString && msg != "" {
			(*row)[wkk.RowKeyCMessage] = msg
		}
		// Remove from row to avoid duplication
		delete(*row, dataKey)
	}

	// Look for timestamp fields and try to detect the right one
	timestampFound := false
	var timestamp int64

	// Check common timestamp field names (all lowercase now)
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
				// Remove the original timestamp field to avoid duplication
				delete(*row, key)
				break
			}
		}
	}

	// If no timestamp found, use current time
	if !timestampFound {
		timestamp = time.Now().UnixMilli()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "logs"),
			attribute.String("reason", "no_timestamp_field"),
		))
	}

	(*row)[wkk.RowKeyCTimestamp] = timestamp

	// Map remaining fields to log.* namespace with sanitized names
	// Sort keys to ensure deterministic processing order for sanitization too
	remainingKeys := make([]wkk.RowKey, 0)
	for key := range *row {
		// Skip already processed fields
		if key == wkk.RowKeyCTimestamp || key == wkk.RowKeyCMessage {
			continue
		}
		remainingKeys = append(remainingKeys, key)
	}
	sort.Slice(remainingKeys, func(i, j int) bool {
		return wkk.RowKeyValue(remainingKeys[i]) < wkk.RowKeyValue(remainingKeys[j])
	})

	newFields := make(map[wkk.RowKey]interface{})
	// Add already processed fields
	newFields[wkk.RowKeyCTimestamp] = (*row)[wkk.RowKeyCTimestamp]
	if msgVal, exists := (*row)[wkk.RowKeyCMessage]; exists {
		newFields[wkk.RowKeyCMessage] = msgVal
	}

	usedSanitizedNames := make(map[string]int) // Track sanitized name usage

	for _, key := range remainingKeys {
		value := (*row)[key]
		originalName := wkk.RowKeyValue(key)

		// Sanitize the field name
		sanitized := t.sanitizeFieldName(originalName)
		if sanitized != "" && sanitized != "data" { // Skip empty names and "data" field
			// Handle duplicates in sanitized names by adding numbered suffixes
			finalSanitized := sanitized
			if count, exists := usedSanitizedNames[sanitized]; exists {
				count++
				usedSanitizedNames[sanitized] = count
				finalSanitized = fmt.Sprintf("%s_%d", sanitized, count)
			} else {
				usedSanitizedNames[sanitized] = 1
			}

			newKey := wkk.NewRowKey("log." + finalSanitized)
			newFields[newKey] = value
		}
	}

	// Replace row content with new fields
	*row = newFields

	// Add resource fields (standard for all log readers)
	(*row)[wkk.NewRowKey("resource.bucket.name")] = t.bucket
	(*row)[wkk.NewRowKey("resource.file.name")] = "./" + t.objectID
	(*row)[wkk.NewRowKey("resource.file.type")] = helpers.GetFileType(t.objectID)

	// Add organization ID if available
	if t.orgID != "" {
		(*row)[wkk.NewRowKey("_cardinalhq.organization_id")] = t.orgID
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

// sanitizeFieldName removes special characters from field names
func (t *CSVLogTranslator) sanitizeFieldName(name string) string {
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
