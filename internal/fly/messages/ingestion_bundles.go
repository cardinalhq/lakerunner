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
)

// MetricIngestBundle represents a bundled collection of metric ingestion messages for boxer processing
type MetricIngestBundle struct {
	Version  int16                          `json:"v"` // message version, current version is 1
	Messages []*ObjStoreNotificationMessage `json:"m"` // the bundled ingestion messages
	QueuedAt time.Time                      `json:"q"` // when the bundle was queued to boxer topic
}

// Marshal converts the bundle to JSON bytes
func (b *MetricIngestBundle) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// Unmarshal converts JSON bytes to MetricIngestBundle
func (b *MetricIngestBundle) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}

// GroupingKey returns the IngestKey based on the first message in the bundle
func (b *MetricIngestBundle) GroupingKey() any {
	if len(b.Messages) == 0 {
		return IngestKey{}
	}
	return b.Messages[0].GroupingKey()
}

// RecordCount returns the total record count for all messages in the bundle
func (b *MetricIngestBundle) RecordCount() int64 {
	var total int64
	for _, msg := range b.Messages {
		total += msg.RecordCount()
	}
	return total
}

// LogIngestBundle represents a bundled collection of log ingestion messages for boxer processing
type LogIngestBundle struct {
	Version  int16                          `json:"v"` // message version, current version is 1
	Messages []*ObjStoreNotificationMessage `json:"m"` // the bundled ingestion messages
	QueuedAt time.Time                      `json:"q"` // when the bundle was queued to boxer topic
}

// Marshal converts the bundle to JSON bytes
func (b *LogIngestBundle) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// Unmarshal converts JSON bytes to LogIngestBundle
func (b *LogIngestBundle) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}

// GroupingKey returns the IngestKey based on the first message in the bundle
func (b *LogIngestBundle) GroupingKey() any {
	if len(b.Messages) == 0 {
		return IngestKey{}
	}
	return b.Messages[0].GroupingKey()
}

// RecordCount returns the total record count for all messages in the bundle
func (b *LogIngestBundle) RecordCount() int64 {
	var total int64
	for _, msg := range b.Messages {
		total += msg.RecordCount()
	}
	return total
}

// TraceIngestBundle represents a bundled collection of trace ingestion messages for boxer processing
type TraceIngestBundle struct {
	Version  int16                          `json:"v"` // message version, current version is 1
	Messages []*ObjStoreNotificationMessage `json:"m"` // the bundled ingestion messages
	QueuedAt time.Time                      `json:"q"` // when the bundle was queued to boxer topic
}

// Marshal converts the bundle to JSON bytes
func (b *TraceIngestBundle) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// Unmarshal converts JSON bytes to TraceIngestBundle
func (b *TraceIngestBundle) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}

// GroupingKey returns the IngestKey based on the first message in the bundle
func (b *TraceIngestBundle) GroupingKey() any {
	if len(b.Messages) == 0 {
		return IngestKey{}
	}
	return b.Messages[0].GroupingKey()
}

// RecordCount returns the total record count for all messages in the bundle
func (b *TraceIngestBundle) RecordCount() int64 {
	var total int64
	for _, msg := range b.Messages {
		total += msg.RecordCount()
	}
	return total
}
