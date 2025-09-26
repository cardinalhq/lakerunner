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

// RecordCount returns the adjusted file size for batching purposes
// - .gz files: actual size
// - Non-gz JSON or binpb: 10x smaller than actual size (since they're uncompressed)
// - Parquet and other files: actual size
func (m *ObjStoreNotificationMessage) RecordCount() int64 {
	// Check if file is compressed (.gz)
	if strings.HasSuffix(m.ObjectID, ".gz") {
		return m.FileSize
	}

	// Check if file is uncompressed JSON or binpb
	if strings.HasSuffix(m.ObjectID, ".json") || strings.HasSuffix(m.ObjectID, ".binpb") {
		// These will expand significantly when processed, so use 10x smaller threshold
		return m.FileSize / 10
	}

	// Parquet and other files use actual size
	return m.FileSize
}

// Marshal converts the message to JSON bytes
func (m *ObjStoreNotificationMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to ObjStoreNotificationMessage
func (m *ObjStoreNotificationMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
