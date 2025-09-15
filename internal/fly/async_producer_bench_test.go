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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/config"
)

// BenchmarkAsyncProducerThroughput measures async producer performance
func BenchmarkAsyncProducerThroughput(b *testing.B) {
	// Get shared Kafka container
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized - TestMain should have set it up")
	}
	k := sharedKafkaContainer
	k.CreateTopics(b, "async-benchmark-topic")

	// Create async producer with optimized settings
	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         500,
			BatchTimeout:      50 * time.Millisecond,
			RequiredAcks:      0, // Fire and forget
			Compression:       2, // Snappy
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           4,
		ChannelSize:       10000,
		AsyncBatchSize:    100,
		AsyncBatchTimeout: 20 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-benchmark-topic"

	// Track callbacks
	var callbacksReceived atomic.Int64
	var errorsReceived atomic.Int64

	callback := func(msg Message, partition int, offset int64, err error) {
		callbacksReceived.Add(1)
		if err != nil {
			errorsReceived.Add(1)
		}
	}

	// 1KB message
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Send messages
	start := time.Now()
	for i := 0; i < b.N; i++ {
		msg := Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: value,
		}
		if err := producer.SendAsync(ctx, topic, msg, callback); err != nil {
			b.Fatalf("Failed to send async message: %v", err)
		}
	}

	// Flush to ensure all messages are sent
	if err := producer.Flush(ctx); err != nil {
		b.Fatalf("Failed to flush: %v", err)
	}

	// Wait for all callbacks (with timeout)
	deadline := time.Now().Add(30 * time.Second)
	for callbacksReceived.Load() < int64(b.N) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(start)

	// Report metrics
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
	b.ReportMetric(elapsed.Seconds()/float64(b.N)*1000, "ms/msg")
	b.ReportMetric(float64(callbacksReceived.Load()), "callbacks")
	b.ReportMetric(float64(errorsReceived.Load()), "errors")

	// Get final stats
	stats := producer.Stats()
	b.ReportMetric(float64(stats.BatchesSent), "batches")
	b.ReportMetric(float64(stats.MessagesSent)/float64(stats.BatchesSent), "msgs/batch")

	if callbacksReceived.Load() != int64(b.N) {
		b.Errorf("Expected %d callbacks, got %d", b.N, callbacksReceived.Load())
	}
}

// BenchmarkSyncVsAsyncComparison directly compares sync and async performance
func BenchmarkSyncVsAsyncComparison(b *testing.B) {
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized")
	}
	k := sharedKafkaContainer

	// Test message
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.Run("Sync", func(b *testing.B) {
		k.CreateTopics(b, "sync-comparison-topic")

		cfg := &config.KafkaConfig{
			Brokers:              []string{k.Broker()},
			ProducerBatchSize:    100,
			ProducerBatchTimeout: 100 * time.Millisecond,
			ProducerCompression:  "snappy",
		}

		factory := NewFactory(cfg)
		producer, err := factory.CreateProducer()
		if err != nil {
			b.Fatalf("Failed to create producer: %v", err)
		}
		defer producer.Close()

		ctx := context.Background()
		topic := "sync-comparison-topic"

		b.ResetTimer()
		b.ReportAllocs()

		start := time.Now()
		for i := 0; i < b.N; i++ {
			msg := Message{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: value,
			}
			if err := producer.Send(ctx, topic, msg); err != nil {
				b.Fatalf("Failed to send message: %v", err)
			}
		}
		elapsed := time.Since(start)

		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
		b.ReportMetric(elapsed.Seconds()/float64(b.N)*1000, "ms/msg")
	})

	b.Run("Async", func(b *testing.B) {
		k.CreateTopics(b, "async-comparison-topic")

		config := AsyncProducerConfig{
			ProducerConfig: ProducerConfig{
				Brokers:           []string{k.Broker()},
				BatchSize:         100,
				BatchTimeout:      100 * time.Millisecond,
				RequiredAcks:      0,
				Compression:       2, // Snappy
				ConnectionTimeout: 10 * time.Second,
			},
			Workers:           4,
			ChannelSize:       1000,
			AsyncBatchSize:    50,
			AsyncBatchTimeout: 50 * time.Millisecond,
		}

		producer := NewAsyncProducer(config)
		defer producer.Close()

		ctx := context.Background()
		topic := "async-comparison-topic"

		var callbacksReceived atomic.Int64
		callback := func(msg Message, partition int, offset int64, err error) {
			callbacksReceived.Add(1)
		}

		b.ResetTimer()
		b.ReportAllocs()

		start := time.Now()
		for i := 0; i < b.N; i++ {
			msg := Message{
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: value,
			}
			if err := producer.SendAsync(ctx, topic, msg, callback); err != nil {
				b.Fatalf("Failed to send async message: %v", err)
			}
		}

		// Flush and wait for callbacks
		if err := producer.Flush(ctx); err != nil {
			b.Fatalf("Failed to flush: %v", err)
		}

		deadline := time.Now().Add(30 * time.Second)
		for callbacksReceived.Load() < int64(b.N) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}

		elapsed := time.Since(start)

		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
		b.ReportMetric(elapsed.Seconds()/float64(b.N)*1000, "ms/msg")
		b.ReportMetric(float64(callbacksReceived.Load()), "callbacks")
	})
}

// BenchmarkAsyncProducerConcurrent tests async producer under concurrent load
func BenchmarkAsyncProducerConcurrent(b *testing.B) {
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized")
	}
	k := sharedKafkaContainer
	k.CreateTopics(b, "async-concurrent-topic")

	config := AsyncProducerConfig{
		ProducerConfig: ProducerConfig{
			Brokers:           []string{k.Broker()},
			BatchSize:         500,
			BatchTimeout:      50 * time.Millisecond,
			RequiredAcks:      0,
			Compression:       2,
			ConnectionTimeout: 10 * time.Second,
		},
		Workers:           8,
		ChannelSize:       10000,
		AsyncBatchSize:    200,
		AsyncBatchTimeout: 25 * time.Millisecond,
	}

	producer := NewAsyncProducer(config)
	defer producer.Close()

	ctx := context.Background()
	topic := "async-concurrent-topic"

	// Message
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	// Track callbacks per goroutine
	var totalCallbacks atomic.Int64
	var totalErrors atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	// Run with increasing concurrency
	for _, numGoroutines := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines-%d", numGoroutines), func(b *testing.B) {
			totalCallbacks.Store(0)
			totalErrors.Store(0)

			messagesPerGoroutine := b.N / numGoroutines
			if messagesPerGoroutine == 0 {
				messagesPerGoroutine = 1
			}

			var wg sync.WaitGroup
			start := time.Now()

			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					callback := func(msg Message, partition int, offset int64, err error) {
						totalCallbacks.Add(1)
						if err != nil {
							totalErrors.Add(1)
						}
					}

					for i := 0; i < messagesPerGoroutine; i++ {
						msg := Message{
							Key:   []byte(fmt.Sprintf("key-%d-%d", goroutineID, i)),
							Value: value,
						}
						for {
							err := producer.SendAsync(ctx, topic, msg, callback)
							if err == nil {
								break
							}
							// Channel full, retry
							time.Sleep(time.Millisecond)
						}
					}
				}(g)
			}

			wg.Wait()

			// Flush all messages
			if err := producer.Flush(ctx); err != nil {
				b.Fatalf("Failed to flush: %v", err)
			}

			// Wait for callbacks
			expectedCallbacks := int64(numGoroutines * messagesPerGoroutine)
			deadline := time.Now().Add(30 * time.Second)
			for totalCallbacks.Load() < expectedCallbacks && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}

			elapsed := time.Since(start)
			totalMessages := numGoroutines * messagesPerGoroutine

			b.ReportMetric(float64(totalMessages)/elapsed.Seconds(), "msgs/sec")
			b.ReportMetric(elapsed.Seconds()/float64(totalMessages)*1000, "ms/msg")
			b.ReportMetric(float64(totalCallbacks.Load()), "callbacks")
			b.ReportMetric(float64(totalErrors.Load()), "errors")
			b.ReportMetric(float64(producer.Stats().BatchesSent), "batches")
		})
	}
}

// BenchmarkAsyncProducerBatchSizes tests different batch sizes
func BenchmarkAsyncProducerBatchSizes(b *testing.B) {
	if sharedKafkaContainer == nil {
		b.Fatal("Shared Kafka container not initialized")
	}
	k := sharedKafkaContainer
	k.CreateTopics(b, "async-batch-size-topic")

	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	for _, batchSize := range []int{10, 50, 100, 500, 1000} {
		b.Run(fmt.Sprintf("batch-%d", batchSize), func(b *testing.B) {
			config := AsyncProducerConfig{
				ProducerConfig: ProducerConfig{
					Brokers:           []string{k.Broker()},
					BatchSize:         batchSize,
					BatchTimeout:      100 * time.Millisecond,
					RequiredAcks:      0,
					Compression:       2,
					ConnectionTimeout: 10 * time.Second,
				},
				Workers:           4,
				ChannelSize:       10000,
				AsyncBatchSize:    batchSize,
				AsyncBatchTimeout: 50 * time.Millisecond,
			}

			producer := NewAsyncProducer(config)
			defer producer.Close()

			ctx := context.Background()
			topic := "async-batch-size-topic"

			var callbacksReceived atomic.Int64
			callback := func(msg Message, partition int, offset int64, err error) {
				callbacksReceived.Add(1)
			}

			b.ResetTimer()
			b.ReportAllocs()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				msg := Message{
					Key:   []byte(fmt.Sprintf("key-%d", i)),
					Value: value,
				}
				if err := producer.SendAsync(ctx, topic, msg, callback); err != nil {
					b.Fatalf("Failed to send message: %v", err)
				}
			}

			producer.Flush(ctx)

			// Wait for callbacks
			deadline := time.Now().Add(30 * time.Second)
			for callbacksReceived.Load() < int64(b.N) && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}

			elapsed := time.Since(start)
			stats := producer.Stats()

			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msgs/sec")
			b.ReportMetric(float64(stats.BatchesSent), "batches")
			b.ReportMetric(float64(stats.MessagesSent)/float64(stats.BatchesSent), "msgs/batch")
		})
	}
}
