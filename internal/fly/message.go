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

package fly

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Message represents a Kafka message with headers
type Message struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// ConsumedMessage represents a message consumed from Kafka with metadata
type ConsumedMessage struct {
	Message
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
}

// ToKafkaMessage converts to kafka-go message format
func (m *Message) ToKafkaMessage() kafka.Message {
	headers := make([]kafka.Header, 0, len(m.Headers))
	for k, v := range m.Headers {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}
	return kafka.Message{
		Key:     m.Key,
		Value:   m.Value,
		Headers: headers,
	}
}

// FromKafkaMessage converts from kafka-go message format
func FromKafkaMessage(km kafka.Message) ConsumedMessage {
	headers := make(map[string]string)
	for _, h := range km.Headers {
		headers[h.Key] = string(h.Value)
	}
	return ConsumedMessage{
		Message: Message{
			Key:     km.Key,
			Value:   km.Value,
			Headers: headers,
		},
		Topic:     km.Topic,
		Partition: km.Partition,
		Offset:    km.Offset,
		Timestamp: km.Time,
	}
}
