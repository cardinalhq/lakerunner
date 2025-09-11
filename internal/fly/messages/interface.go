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

import "github.com/google/uuid"

// GroupableMessage interface for messages that can be grouped by the Gatherer
type GroupableMessage interface {
	GroupingKey() any
	RecordCount() int64
}

// Unmarshaler defines the interface for messages that can be unmarshaled from bytes
type Unmarshaler interface {
	Unmarshal([]byte) error
}

// CompactionKeyProvider defines the interface for grouping keys that provide organization and instance info
type CompactionKeyProvider interface {
	GetOrgID() uuid.UUID
	GetInstanceNum() int16
}

// CompactionMessage defines the complete interface for compaction messages
type CompactionMessage interface {
	GroupableMessage
	Unmarshaler
}

// CompactionKeyInterface defines the complete interface for compaction keys
type CompactionKeyInterface interface {
	comparable
	CompactionKeyProvider
}
