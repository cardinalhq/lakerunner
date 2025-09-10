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

// RollupKey represents the key for grouping messages for metric rollups
type RollupKey struct {
	OrganizationID    uuid.UUID
	DateInt           int32
	SourceFrequencyMs int32
	TargetFrequencyMs int32
	InstanceNum       int16
	SlotID            int32
	SlotCount         int32
	TruncatedTimebox  int64 // Start time of target frequency window
}

// GetOrgID returns the organization ID from the key
func (k RollupKey) GetOrgID() uuid.UUID {
	return k.OrganizationID
}

// GetInstanceNum returns the instance number from the key
func (k RollupKey) GetInstanceNum() int16 {
	return k.InstanceNum
}

// MetricRollupMessage represents a metric segment processing notification for rollups
// This implements GroupableMessage interface
type MetricRollupMessage struct {
	Version           int16     `json:"v"`  // message version, current version is 1
	OrganizationID    uuid.UUID `json:"o"`  // organization_id
	DateInt           int32     `json:"d"`  // dateint
	SourceFrequencyMs int32     `json:"sf"` // source frequency_ms
	TargetFrequencyMs int32     `json:"tf"` // target frequency_ms
	SegmentID         int64     `json:"s"`  // segment_id
	InstanceNum       int16     `json:"i"`  // instance_num
	SlotID            int32     `json:"si"` // slot_id
	SlotCount         int32     `json:"sc"` // slot_count
	Records           int64     `json:"rc"` // record_count for early filtering
	FileSize          int64     `json:"fs"` // file_size for early filtering
	SegmentStartTime  time.Time `json:"st"` // segment start time for truncated timebox calculation
	QueuedAt          time.Time `json:"t"`  // queued_at timestamp for lag tracking
}

// GroupingKey returns the key used to group messages for rollups
func (m *MetricRollupMessage) GroupingKey() any {
	// Calculate truncated timebox - align segment start time to target frequency window
	targetFrequencyDuration := time.Duration(m.TargetFrequencyMs) * time.Millisecond
	truncatedTimebox := m.SegmentStartTime.Truncate(targetFrequencyDuration).Unix()

	return RollupKey{
		OrganizationID:    m.OrganizationID,
		DateInt:           m.DateInt,
		SourceFrequencyMs: m.SourceFrequencyMs,
		TargetFrequencyMs: m.TargetFrequencyMs,
		InstanceNum:       m.InstanceNum,
		SlotID:            m.SlotID,
		SlotCount:         m.SlotCount,
		TruncatedTimebox:  truncatedTimebox,
	}
}

// RecordCount returns the record count for this message
func (m *MetricRollupMessage) RecordCount() int64 {
	return m.Records
}

// Marshal converts the message to JSON bytes
func (m *MetricRollupMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to MetricRollupMessage
func (m *MetricRollupMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}
