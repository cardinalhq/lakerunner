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

// TraceIngestMessage represents a trace file to be ingested from object storage
type TraceIngestMessage struct {
	ID             uuid.UUID `json:"id"`
	OrganizationID uuid.UUID `json:"organization_id"`
	CollectorName  string    `json:"collector_name"`
	InstanceNum    int16     `json:"instance_num"`
	Bucket         string    `json:"bucket"`
	ObjectID       string    `json:"object_id"`
	FileSize       int64     `json:"file_size"`
	QueuedAt       time.Time `json:"queued_at"`

	// Retry tracking
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error,omitempty"`
	LastAttempt time.Time `json:"last_attempt"`
}

// Marshal converts the message to JSON bytes
func (m *TraceIngestMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes to TraceIngestMessage
func (m *TraceIngestMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// GetPartitionKey returns a key suitable for partitioning
func (m *TraceIngestMessage) GetPartitionKey() string {
	// Use organization ID for partitioning to keep org data together
	return m.OrganizationID.String()
}
