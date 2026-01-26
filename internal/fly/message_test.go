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
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage_ToKafkaMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want kafka.Message
	}{
		{
			name: "message with headers",
			msg: Message{
				Key:   []byte("test-key"),
				Value: []byte("test-value"),
				Headers: map[string]string{
					"header1": "value1",
					"header2": "value2",
				},
			},
			want: kafka.Message{
				Key:   []byte("test-key"),
				Value: []byte("test-value"),
				Headers: []kafka.Header{
					{Key: "header1", Value: []byte("value1")},
					{Key: "header2", Value: []byte("value2")},
				},
			},
		},
		{
			name: "message without headers",
			msg: Message{
				Key:     []byte("test-key"),
				Value:   []byte("test-value"),
				Headers: nil,
			},
			want: kafka.Message{
				Key:     []byte("test-key"),
				Value:   []byte("test-value"),
				Headers: []kafka.Header{},
			},
		},
		{
			name: "empty message",
			msg:  Message{},
			want: kafka.Message{
				Headers: []kafka.Header{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.msg.ToKafkaMessage()
			assert.Equal(t, tt.want.Key, got.Key)
			assert.Equal(t, tt.want.Value, got.Value)

			// Check headers separately since order might vary
			assert.Equal(t, len(tt.want.Headers), len(got.Headers))
			expectedHeaders := make(map[string][]byte)
			for _, h := range tt.want.Headers {
				expectedHeaders[h.Key] = h.Value
			}
			for _, h := range got.Headers {
				expected, ok := expectedHeaders[h.Key]
				require.True(t, ok, "unexpected header key: %s", h.Key)
				assert.Equal(t, expected, h.Value)
			}
		})
	}
}

func TestFromKafkaMessage(t *testing.T) {
	timestamp := time.Now()
	tests := []struct {
		name string
		km   kafka.Message
		want ConsumedMessage
	}{
		{
			name: "full message",
			km: kafka.Message{
				Topic:     "test-topic",
				Partition: 1,
				Offset:    100,
				Key:       []byte("test-key"),
				Value:     []byte("test-value"),
				Headers: []kafka.Header{
					{Key: "header1", Value: []byte("value1")},
					{Key: "header2", Value: []byte("value2")},
				},
				Time: timestamp,
			},
			want: ConsumedMessage{
				Message: Message{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
					Headers: map[string]string{
						"header1": "value1",
						"header2": "value2",
					},
				},
				Topic:     "test-topic",
				Partition: 1,
				Offset:    100,
				Timestamp: timestamp,
			},
		},
		{
			name: "message without headers",
			km: kafka.Message{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
				Key:       []byte("key"),
				Value:     []byte("value"),
				Time:      timestamp,
			},
			want: ConsumedMessage{
				Message: Message{
					Key:     []byte("key"),
					Value:   []byte("value"),
					Headers: map[string]string{},
				},
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
				Timestamp: timestamp,
			},
		},
		{
			name: "empty message",
			km:   kafka.Message{},
			want: ConsumedMessage{
				Message: Message{
					Headers: map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FromKafkaMessage(tt.km)
			assert.Equal(t, tt.want, got)
		})
	}
}
