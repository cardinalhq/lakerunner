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

package externalscaler

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KafkaMetricsExporter exports Kafka consumer lag metrics as OpenTelemetry metrics
type KafkaMetricsExporter struct {
	monitor          LagMonitorInterface
	meter            metric.Meter
	offsetGauge      metric.Int64ObservableGauge
	consumerLagGauge metric.Int64ObservableGauge
	hwmGauge         metric.Int64ObservableGauge
}

// NewKafkaMetricsExporter creates a new Kafka metrics exporter
func NewKafkaMetricsExporter(monitor LagMonitorInterface) (*KafkaMetricsExporter, error) {
	exporter := &KafkaMetricsExporter{
		monitor: monitor,
		meter:   otel.Meter("github.com/cardinalhq/lakerunner/externalscaler"),
	}

	if err := exporter.registerMetrics(); err != nil {
		return nil, fmt.Errorf("failed to register metrics: %w", err)
	}

	return exporter, nil
}

// registerMetrics registers the OpenTelemetry metrics with callback functions
func (e *KafkaMetricsExporter) registerMetrics() error {
	var err error

	// Register consumer group offset gauge
	e.offsetGauge, err = e.meter.Int64ObservableGauge(
		"lakerunner.kafka.consumer_group.offset",
		metric.WithDescription("Current offset for consumer group"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(e.observeConsumerOffsets),
	)
	if err != nil {
		return fmt.Errorf("failed to create consumer offset gauge: %w", err)
	}

	// Register consumer lag gauge
	e.consumerLagGauge, err = e.meter.Int64ObservableGauge(
		"lakerunner.kafka.consumer_group.lag",
		metric.WithDescription("Consumer group lag (high water mark - committed offset)"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(e.observeConsumerLag),
	)
	if err != nil {
		return fmt.Errorf("failed to create consumer lag gauge: %w", err)
	}

	// Register high water mark gauge
	e.hwmGauge, err = e.meter.Int64ObservableGauge(
		"lakerunner.kafka.partition.high_water_mark",
		metric.WithDescription("High water mark for topic partition"),
		metric.WithUnit("1"),
		metric.WithInt64Callback(e.observeHighWaterMark),
	)
	if err != nil {
		return fmt.Errorf("failed to create high water mark gauge: %w", err)
	}

	return nil
}

// observeConsumerOffsets is the callback for consumer offset metrics
func (e *KafkaMetricsExporter) observeConsumerOffsets(ctx context.Context, observer metric.Int64Observer) error {
	metrics := e.monitor.GetDetailedMetrics()

	for _, info := range metrics {
		if info.CommittedOffset >= 0 {
			observer.Observe(info.CommittedOffset,
				metric.WithAttributes(
					attribute.String("topic", info.Topic),
					attribute.String("consumer_group", info.GroupID),
					attribute.Int("partition", info.Partition),
				),
			)
		}
	}
	return nil
}

// observeConsumerLag is the callback for consumer lag metrics
func (e *KafkaMetricsExporter) observeConsumerLag(ctx context.Context, observer metric.Int64Observer) error {
	metrics := e.monitor.GetDetailedMetrics()

	for _, info := range metrics {
		observer.Observe(info.Lag,
			metric.WithAttributes(
				attribute.String("topic", info.Topic),
				attribute.String("consumer_group", info.GroupID),
				attribute.Int("partition", info.Partition),
			),
		)
	}
	return nil
}

// observeHighWaterMark is the callback for high water mark metrics
func (e *KafkaMetricsExporter) observeHighWaterMark(ctx context.Context, observer metric.Int64Observer) error {
	metrics := e.monitor.GetDetailedMetrics()

	// Deduplicate HWM by topic/partition (multiple consumer groups may read same topic)
	seen := make(map[string]bool)
	for _, info := range metrics {
		key := fmt.Sprintf("%s:%d", info.Topic, info.Partition)
		if !seen[key] {
			observer.Observe(info.HighWaterMark,
				metric.WithAttributes(
					attribute.String("topic", info.Topic),
					attribute.Int("partition", info.Partition),
				),
			)
			seen[key] = true
		}
	}
	return nil
}
