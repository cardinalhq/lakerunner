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

// LogCompactionKey represents the key for grouping log messages for compaction
type LogCompactionKey struct {
	OrganizationID uuid.UUID
	DateInt        int32
	InstanceNum    int16
}

// GetOrgID returns the organization ID from the key
func (k LogCompactionKey) GetOrgID() uuid.UUID {
	return k.OrganizationID
}

// GetInstanceNum returns the instance number from the key
func (k LogCompactionKey) GetInstanceNum() int16 {
	return k.InstanceNum
}

// LogCompactionMessage represents a log segment processing notification for compaction
// This implements GroupableMessage interface
type LogCompactionMessage struct {
	Version        int16     `json:"v"`  // message version, current version is 1
	OrganizationID uuid.UUID `json:"o"`  // organization_id
	DateInt        int32     `json:"d"`  // dateint
	SegmentID      int64     `json:"s"`  // segment_id
	InstanceNum    int16     `json:"i"`  // instance_num
	SlotID         int32     `json:"si"` // slot_id (always 0 for logs)
	Records        int64     `json:"rc"` // record_count for early filtering
	FileSize       int64     `json:"fs"` // file_size for early filtering
	StartTs        int64     `json:"st"` // start timestamp
	EndTs          int64     `json:"et"` // end timestamp
	QueuedAt       time.Time `json:"t"`  // queued_at timestamp for lag tracking
}

// GroupingKey returns the key used to group messages for compaction
func (m *LogCompactionMessage) GroupingKey() any {
	return LogCompactionKey{
		OrganizationID: m.OrganizationID,
		DateInt:        m.DateInt,
		InstanceNum:    m.InstanceNum,
	}
}

// RecordCount returns the record count for this message
func (m *LogCompactionMessage) RecordCount() int64 {
	return m.Records
}

// Marshal converts the message to JSON bytes
func (m *LogCompactionMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to LogCompactionMessage
func (m *LogCompactionMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
