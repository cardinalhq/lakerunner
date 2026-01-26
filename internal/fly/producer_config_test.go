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

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestProducerCompressionConfig(t *testing.T) {
	cfg := ProducerConfig{
		Brokers:     []string{"localhost:9092"},
		Compression: kafka.Snappy,
	}
	p := NewProducer(cfg)
	defer func() { _ = p.Close() }()

	kp := p.(*kafkaProducer)
	assert.Equal(t, kafka.Snappy, kp.config.Compression)
}
