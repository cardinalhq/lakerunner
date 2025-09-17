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

package fly

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/config"
)

func TestNewFactory(t *testing.T) {
	config := &config.KafkaConfig{
		Brokers: []string{"broker1:9092", "broker2:9092"},
	}

	factory := NewFactory(config)
	assert.NotNil(t, factory)
	assert.Equal(t, config, factory.GetConfig())
}

func TestFactory_CreateProducer(t *testing.T) {
	tests := []struct {
		name    string
		config  *config.KafkaConfig
		wantErr bool
	}{
		{
			name: "basic producer",
			config: &config.KafkaConfig{
				Brokers:              []string{"localhost:9092"},
				ProducerBatchSize:    100,
				ProducerBatchTimeout: 1000000000, // 1 second in nanoseconds
				ProducerCompression:  "snappy",
				SASLEnabled:          false,
				TLSEnabled:           false,
			},
			wantErr: false,
		},
		{
			name: "producer with unsupported compression",
			config: &config.KafkaConfig{
				Brokers:             []string{"localhost:9092"},
				ProducerCompression: "invalid",
			},
			wantErr: true,
		},
		{
			name: "producer with SASL SCRAM-SHA-256",
			config: &config.KafkaConfig{
				Brokers:              []string{"localhost:9092"},
				ProducerBatchSize:    50,
				ProducerBatchTimeout: 500000000, // 500ms
				SASLEnabled:          true,
				SASLMechanism:        "SCRAM-SHA-256",
				SASLUsername:         "user",
				SASLPassword:         "pass",
			},
			wantErr: false,
		},
		{
			name: "producer with SASL SCRAM-SHA-512",
			config: &config.KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				SASLEnabled:   true,
				SASLMechanism: "SCRAM-SHA-512",
				SASLUsername:  "user",
				SASLPassword:  "pass",
			},
			wantErr: false,
		},
		{
			name: "producer with TLS",
			config: &config.KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				TLSEnabled:    true,
				TLSSkipVerify: true,
			},
			wantErr: false,
		},
		{
			name: "producer with PLAIN SASL mechanism",
			config: &config.KafkaConfig{
				Brokers:       []string{"localhost:9092"},
				SASLEnabled:   true,
				SASLMechanism: "PLAIN", // Now supported
				SASLUsername:  "user",
				SASLPassword:  "pass",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory(tt.config)
			producer, err := factory.CreateProducer()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, producer)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, producer)
				if producer != nil {
					kp := producer.(*kafkaProducer)
					if tt.config.ProducerCompression == "snappy" {
						assert.Equal(t, kafka.Snappy, kp.config.Compression)
					}
					_ = producer.Close()
				}
			}
		})
	}
}

// Tests that require actual Kafka connection have been moved to factory_integration_test.go
// with the kafkatest build tag to only run with "make test-kafka"

func TestFactory_SASLMechanismCreation(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		wantErr   bool
	}{
		{"SCRAM-SHA-256", "SCRAM-SHA-256", false},
		{"SCRAM-SHA-512", "SCRAM-SHA-512", false},
		{"PLAIN", "PLAIN", false},
		{"unsupported", "INVALID", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &Factory{
				config: &config.KafkaConfig{
					SASLMechanism: tt.mechanism,
					SASLUsername:  "user",
					SASLPassword:  "pass",
				},
			}

			mechanism, err := factory.createSASLMechanism()
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, mechanism)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, mechanism)
			}
		})
	}
}
