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

package messages

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ObjStoreNotificationMessage represents an object storage file notification event
// Using short JSON keys to minimize message size for high-volume Kafka usage
// This implements GroupableMessage interface for ingestion processing
type ObjStoreNotificationMessage struct {
	OrganizationID uuid.UUID `json:"o"` // org_id
	InstanceNum    int16     `json:"i"` // instance_num
	Bucket         string    `json:"b"` // bucket
	ObjectID       string    `json:"k"` // key/object_id
	FileSize       int64     `json:"s"` // size
	QueuedAt       time.Time `json:"t"` // timestamp
}

// IngestKey represents the key for grouping messages for ingestion (signal-agnostic)
type IngestKey struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
}

// GetOrgID returns the organization ID from the key
func (k IngestKey) GetOrgID() uuid.UUID {
	return k.OrganizationID
}

// GetInstanceNum returns the instance number from the key
func (k IngestKey) GetInstanceNum() int16 {
	return k.InstanceNum
}

// GroupingKey returns the key used to group messages for ingestion
func (m *ObjStoreNotificationMessage) GroupingKey() any {
	return IngestKey{
		OrganizationID: m.OrganizationID,
		InstanceNum:    m.InstanceNum,
	}
}

// Compression ratio estimates per signal type (based on empirical measurements)
const (
	logsCompressionRatio    = 150
	metricsCompressionRatio = 20
	tracesCompressionRatio  = 40
	defaultCompressionRatio = 150 // Conservative default for unknown signals
)

// RecordCount returns the estimated uncompressed size for batching purposes.
// - .gz files: signal-specific decompression ratio (logs=150x, metrics=20x, traces=40x)
// - Non-gz files: actual size (already uncompressed)
func (m *ObjStoreNotificationMessage) RecordCount() int64 {
	// Only estimate compression for .gz files
	if !strings.HasSuffix(m.ObjectID, ".gz") {
		return m.FileSize
	}

	// Extract signal from filename (e.g., "logs_123.binpb.gz" -> "logs")
	signal := extractSignalFromPath(m.ObjectID)

	switch signal {
	case "logs":
		return m.FileSize * logsCompressionRatio
	case "metrics":
		return m.FileSize * metricsCompressionRatio
	case "traces":
		return m.FileSize * tracesCompressionRatio
	default:
		return m.FileSize * defaultCompressionRatio
	}
}

// extractSignalFromPath extracts the signal type from an object path.
// Handles formats like:
// - "otel-raw/logs/..." or "logs-raw/..."
// - "path/to/logs_123.binpb.gz" (signal prefix in filename)
func extractSignalFromPath(objectID string) string {
	// Check for otel-raw/{signal}/ or {signal}-raw/ path patterns
	for _, signal := range []string{"logs", "metrics", "traces"} {
		if strings.Contains(objectID, "/"+signal+"/") ||
			strings.HasPrefix(objectID, signal+"-raw/") ||
			strings.HasPrefix(objectID, "otel-raw/"+signal+"/") {
			return signal
		}
	}

	// Check filename prefix (e.g., "logs_123.binpb.gz")
	// Find the last path component
	lastSlash := strings.LastIndex(objectID, "/")
	filename := objectID
	if lastSlash >= 0 {
		filename = objectID[lastSlash+1:]
	}

	for _, signal := range []string{"logs", "metrics", "traces"} {
		if strings.HasPrefix(filename, signal+"_") {
			return signal
		}
	}

	return ""
}

// Marshal converts the message to JSON bytes
func (m *ObjStoreNotificationMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to ObjStoreNotificationMessage
func (m *ObjStoreNotificationMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// IsParquet returns true if this notification is for a parquet file
func (m *ObjStoreNotificationMessage) IsParquet() bool {
	return strings.HasSuffix(m.ObjectID, ".parquet")
}
