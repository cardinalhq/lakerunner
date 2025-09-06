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

// MetricSegmentNotificationMessage represents a metric segment processing notification
// Using short JSON keys to minimize message size for high-volume Kafka usage
type MetricSegmentNotificationMessage struct {
	OrganizationID uuid.UUID `json:"o"`  // organization_id
	DateInt        int32     `json:"d"`  // dateint
	FrequencyMs    int64     `json:"f"`  // frequency_ms
	SegmentID      uuid.UUID `json:"s"`  // segment_id
	InstanceNum    int16     `json:"i"`  // instance_num
	SlotID         int32     `json:"si"` // slot_id
	SlotCount      int32     `json:"sc"` // slot_count
	QueuedAt       time.Time `json:"t"`  // queued_at timestamp for lag tracking
}

// Marshal converts the message to JSON bytes
func (m *MetricSegmentNotificationMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to MetricSegmentNotificationMessage
func (m *MetricSegmentNotificationMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
