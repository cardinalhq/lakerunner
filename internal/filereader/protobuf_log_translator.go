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
	"fmt"
	"time"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// ProtoBinLogTranslator handles translation for protobuf binary log files
type ProtoBinLogTranslator struct {
	orgID              string
	bucket             string
	objectID           string
	trieClusterManager *fingerprinter.TrieClusterManager
}

// NewProtoBinLogTranslator creates a new protobuf log translator
func NewProtoBinLogTranslator(orgID, bucket, objectID string, trieClusterManager *fingerprinter.TrieClusterManager) *ProtoBinLogTranslator {
	return &ProtoBinLogTranslator{
		orgID:              orgID,
		bucket:             bucket,
		objectID:           objectID,
		trieClusterManager: trieClusterManager,
	}
}

// TranslateRow handles protobuf-specific field translation
func (t *ProtoBinLogTranslator) TranslateRow(row *Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Handle timestamp: use timestamp, fallback to observed_timestamp
	// NOTE: _cardinalhq.timestamp should already be validated by ingest_proto_logs.go
	// This is a secondary validation for other readers that may not have done it
	if _, ok := (*row)[wkk.RowKeyCTimestamp]; !ok {
		var timestamp int64
		if ts, exists := (*row)[wkk.NewRowKey("timestamp")]; exists {
			timestamp = ensureInt64(ts)
		}
		// Reject Unix epoch (0) and invalid timestamps (-1)
		if timestamp <= 0 {
			if obsTs, exists := (*row)[wkk.NewRowKey("observed_timestamp")]; exists {
				obsTimestamp := ensureInt64(obsTs)
				if obsTimestamp > 0 {
					timestamp = obsTimestamp
				}
			}
		}
		if timestamp > 0 {
			// Convert nanoseconds to milliseconds if needed
			if timestamp > 1e15 {
				timestamp = timestamp / 1e6
			}
			(*row)[wkk.RowKeyCTimestamp] = timestamp
		} else {
			// Use current time as last resort to avoid 1970-01-01 dates
			(*row)[wkk.RowKeyCTimestamp] = time.Now().UnixMilli()
		}
	}

	// Handle level: use severity_text if _cardinalhq.level not set
	if _, ok := (*row)[wkk.NewRowKey("_cardinalhq.level")]; !ok {
		if severityText, exists := (*row)[wkk.NewRowKey("severity_text")]; exists {
			if level, isString := severityText.(string); isString && level != "" {
				(*row)[wkk.NewRowKey("_cardinalhq.level")] = level
			}
		}
	}

	// Handle message: use body if _cardinalhq.message not set
	if _, ok := (*row)[wkk.RowKeyCMessage]; !ok {
		if body, exists := (*row)[wkk.NewRowKey("body")]; exists {
			if message, isString := body.(string); isString && message != "" {
				(*row)[wkk.RowKeyCMessage] = message
			}
		}
	}

	// Calculate fingerprint from message
	fingerprint, err := t.calculateFingerprint(*row)
	if err != nil {
		return fmt.Errorf("failed to calculate fingerprint: %w", err)
	}

	if fingerprint != 0 {
		(*row)[wkk.RowKeyCFingerprint] = fingerprint
	} else {
		delete(*row, wkk.RowKeyCFingerprint)
	}

	// Add resource fields (only for logs signal)
	(*row)[wkk.NewRowKey("resource.bucket.name")] = t.bucket
	(*row)[wkk.NewRowKey("resource.file.name")] = "./" + t.objectID
	(*row)[wkk.NewRowKey("resource.file.type")] = helpers.GetFileType(t.objectID)

	return nil
}

// calculateFingerprint computes a fingerprint for the protobuf log record
func (t *ProtoBinLogTranslator) calculateFingerprint(row Row) (int64, error) {
	var message string

	// Only look at _cardinalhq.message field for protobuf logs
	if val, ok := row[wkk.RowKeyCMessage]; ok {
		if str, ok := val.(string); ok && str != "" {
			message = str
		}
	}

	if message == "" {
		return 0, nil
	}

	fingerprint, _, _, err := fingerprinter.Fingerprint(message, t.trieClusterManager)
	if err != nil {
		return 0, err
	}

	return fingerprint, nil
}

// ensureInt64 converts timestamp to int64 if it's not already
// Returns -1 for unrecognized types to indicate invalid timestamp
func ensureInt64(ts any) int64 {
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
