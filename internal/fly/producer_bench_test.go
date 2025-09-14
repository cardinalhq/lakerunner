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

//go:build kafkatest
// +build kafkatest

package fly

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkProducerSend(b *testing.B) {
	// Get shared Kafka container (using TB interface for benchmarks)
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized - TestMain should have set it up")
	}
	k := sharedKafkaContainer
	k.CreateTopics(b, "benchmark-topic")

	// Setup Kafka container and producer
	config := &Config{
		Brokers:              []string{k.Broker()},
		ProducerBatchSize:    100,
		ProducerBatchTimeout: 100 * time.Millisecond,
		ProducerCompression:  "snappy",
	}

	factory := NewFactory(config)
	producer, err := factory.CreateProducer()
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	topic := "benchmark-topic"

	// Create test messages of different sizes
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			// Create a message of the specified size
			value := make([]byte, sz.size)
			for i := range value {
				value[i] = byte(i % 256)
			}

			msg := Message{
				Key:   []byte(fmt.Sprintf("key-%d", time.Now().UnixNano())),
				Value: value,
			}

			// Reset timer to exclude setup
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(sz.size))

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				if err := producer.Send(ctx, topic, msg); err != nil {
					b.Fatalf("Failed to send message: %v", err)
				}
			}
		})
	}
}

func BenchmarkProducerBatchSend(b *testing.B) {
	// Get shared Kafka container (using TB interface for benchmarks)
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized - TestMain should have set it up")
	}
	k := sharedKafkaContainer
	k.CreateTopics(b, "benchmark-batch-topic")

	// Setup Kafka container and producer
	config := &Config{
		Brokers:              []string{k.Broker()},
		ProducerBatchSize:    1000,
		ProducerBatchTimeout: 500 * time.Millisecond,
		ProducerCompression:  "snappy",
	}

	factory := NewFactory(config)
	producer, err := factory.CreateProducer()
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	topic := "benchmark-batch-topic"

	// Batch sizes to test
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch-%d", batchSize), func(b *testing.B) {
			// Create batch of messages (1KB each)
			messages := make([]Message, batchSize)
			for i := range messages {
				value := make([]byte, 1024)
				for j := range value {
					value[j] = byte(j % 256)
				}
				messages[i] = Message{
					Key:   []byte(fmt.Sprintf("key-%d-%d", time.Now().UnixNano(), i)),
					Value: value,
				}
			}

			// Reset timer to exclude setup
			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * 1024))

			// Run the benchmark
			for i := 0; i < b.N; i++ {
				for _, msg := range messages {
					if err := producer.Send(ctx, topic, msg); err != nil {
						b.Fatalf("Failed to send message: %v", err)
					}
				}
			}
		})
	}
}

// BenchmarkProducerThroughput measures messages per second throughput
func BenchmarkProducerThroughput(b *testing.B) {
	// Get shared Kafka container (using TB interface for benchmarks)
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized - TestMain should have set it up")
	}
	k := sharedKafkaContainer
	k.CreateTopics(b, "benchmark-throughput-topic")

	config := &Config{
		Brokers:              []string{k.Broker()},
		ProducerBatchSize:    500,
		ProducerBatchTimeout: 100 * time.Millisecond,
		ProducerCompression:  "snappy",
	}

	factory := NewFactory(config)
	producer, err := factory.CreateProducer()
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	topic := "benchmark-throughput-topic"

	// 1KB message
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Send messages for the duration of the benchmark
	start := time.Now()
	messagesSent := 0

	for i := 0; i < b.N; i++ {
		msg := Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: value,
		}
		if err := producer.Send(ctx, topic, msg); err != nil {
			b.Fatalf("Failed to send message: %v", err)
		}
		messagesSent++
	}

	elapsed := time.Since(start)
	messagesPerSecond := float64(messagesSent) / elapsed.Seconds()
	b.ReportMetric(messagesPerSecond, "msgs/sec")
	b.ReportMetric(elapsed.Seconds()/float64(messagesSent)*1000, "ms/msg")
}
