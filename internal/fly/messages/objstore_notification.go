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
	"time"

	"github.com/google/uuid"
)

// ObjStoreNotificationMessage represents an object storage file notification event
// Using short JSON keys to minimize message size for high-volume Kafka usage
type ObjStoreNotificationMessage struct {
	OrganizationID uuid.UUID `json:"o"` // org_id
	InstanceNum    int16     `json:"i"` // instance_num
	Bucket         string    `json:"b"` // bucket
	ObjectID       string    `json:"k"` // key/object_id
	FileSize       int64     `json:"s"` // size
	QueuedAt       time.Time `json:"t"` // timestamp
}

// Marshal converts the message to JSON bytes
func (m *ObjStoreNotificationMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to ObjStoreNotificationMessage
func (m *ObjStoreNotificationMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// ObjStoreNotificationBatch represents a batch of object storage notifications for efficient processing
type ObjStoreNotificationBatch struct {
	Messages  []ObjStoreNotificationMessage `json:"messages"`
	BatchID   uuid.UUID                     `json:"batch_id"`
	CreatedAt time.Time                     `json:"created_at"`
}

// Marshal converts the batch to JSON bytes
func (b *ObjStoreNotificationBatch) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// Unmarshal converts JSON bytes to ObjStoreNotificationBatch
func (b *ObjStoreNotificationBatch) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}
