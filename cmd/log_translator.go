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

package cmd

import (
	"fmt"
	"time"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// LogTranslator adds fingerprinting and resource metadata to log rows
type LogTranslator struct {
	orgID              string
	bucket             string
	objectID           string
	trieClusterManager *fingerprinter.TrieClusterManager
}

// TranslateRow adds fingerprint and resource fields to each row
func (t *LogTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Always recalculate fingerprint from log content, ignoring any existing _cardinalhq.fingerprint field
	fingerprint, err := t.calculateFingerprint(*row)
	if err != nil {
		return fmt.Errorf("failed to calculate fingerprint: %w", err)
	}

	// Always set the recalculated fingerprint, overwriting any existing one
	if fingerprint != 0 {
		(*row)["_cardinalhq.fingerprint"] = fingerprint
	} else {
		// Remove existing fingerprint field if we calculated 0 (no valid message found)
		delete(*row, "_cardinalhq.fingerprint")
	}

	// Add resource fields
	(*row)["resource.bucket.name"] = t.bucket
	(*row)["resource.file.name"] = "./" + t.objectID
	(*row)["resource.file.type"] = ingestlogs.GetFileType(t.objectID)

	// Set _cardinalhq.level from severity_text if not already set
	if _, ok := (*row)["_cardinalhq.level"]; !ok {
		if severityText, exists := (*row)["severity_text"]; exists {
			if level, isString := severityText.(string); isString && level != "" {
				(*row)["_cardinalhq.level"] = level
			}
		}
	}

	// Set _cardinalhq.message from body if not already set
	if _, ok := (*row)["_cardinalhq.message"]; !ok {
		if body, exists := (*row)["body"]; exists {
			if message, isString := body.(string); isString && message != "" {
				(*row)["_cardinalhq.message"] = message
			}
		}
	}

	// Ensure timestamp is present and properly typed
	if ts, ok := (*row)["_cardinalhq.timestamp"]; ok {
		convertedTS := ensureInt64(ts)
		if convertedTS == -1 {
			// Invalid timestamp type, use current time
			(*row)["_cardinalhq.timestamp"] = time.Now().UnixMilli()
		} else {
			(*row)["_cardinalhq.timestamp"] = convertedTS
		}
	} else {
		// If no _cardinalhq.timestamp field exists, try to find timestamp from other common fields
		timestamp := t.extractTimestamp(*row)
		if timestamp != 0 {
			(*row)["_cardinalhq.timestamp"] = timestamp
		} else {
			// As a last resort, use current time (this is better than failing completely)
			(*row)["_cardinalhq.timestamp"] = time.Now().UnixMilli()
		}
	}

	return nil
}

// calculateFingerprint computes a fingerprint for the log record
func (t *LogTranslator) calculateFingerprint(row filereader.Row) (int64, error) {
	// Extract the log message/body using the same logic as the original system
	var message string

	// Try common message field names in order of preference
	for _, field := range []string{"_cardinalhq.message", "message", "body", "msg", "log"} {
		if val, ok := row[field]; ok {
			if str, ok := val.(string); ok && str != "" {
				message = str
				break
			}
		}
	}

	// If no message found, return 0 (no fingerprint)
	if message == "" {
		return 0, nil
	}

	// Use the same fingerprinter as the original system
	fingerprint, _, _, err := fingerprinter.Fingerprint(message, t.trieClusterManager)
	if err != nil {
		return 0, err
	}

	return fingerprint, nil
}

// ensureInt64 converts timestamp to int64 if it's not already
// Returns -1 for unrecognized types to indicate invalid timestamp
func ensureInt64(ts interface{}) int64 {
	switch v := ts.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	default:
		return -1 // Return -1 to indicate invalid timestamp
	}
}

// extractTimestamp attempts to extract a timestamp from common timestamp fields
func (t *LogTranslator) extractTimestamp(row filereader.Row) int64 {
	// Try common timestamp field names
	for _, field := range []string{"timestamp", "observed_timestamp", "time", "@timestamp", "ts", "created_at", "observed_time_unix_nano"} {
		if val, ok := row[field]; ok {
			if timestamp := ensureInt64(val); timestamp != 0 && timestamp != -1 {
				// Convert nanoseconds to milliseconds if the value is very large
				if timestamp > 1e15 { // Assume nanoseconds if > year 33658
					return timestamp / 1e6
				}
				// Convert seconds to milliseconds if the value is reasonable for seconds
				if timestamp > 1e9 && timestamp < 1e12 { // Reasonable range for Unix seconds
					return timestamp * 1000
				}
				// Assume it's already in milliseconds
				return timestamp
			}
		}
	}
	return 0
}
