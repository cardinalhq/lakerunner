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

package externalscaler

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
)

// KafkaMetricsExporter exports Kafka consumer lag metrics as OpenTelemetry metrics
type KafkaMetricsExporter struct {
	monitor          LagMonitorInterface
	topicRegistry    *config.TopicRegistry
	meter            metric.Meter
	consumerLagGauge metric.Int64ObservableGauge
}

// NewKafkaMetricsExporter creates a new Kafka metrics exporter
func NewKafkaMetricsExporter(monitor LagMonitorInterface, topicRegistry *config.TopicRegistry) (*KafkaMetricsExporter, error) {
	if monitor == nil {
		return nil, fmt.Errorf("monitor cannot be nil")
	}
	if topicRegistry == nil {
		return nil, fmt.Errorf("topicRegistry cannot be nil")
	}

	exporter := &KafkaMetricsExporter{
		monitor:       monitor,
		topicRegistry: topicRegistry,
		meter:         otel.Meter("github.com/cardinalhq/lakerunner/externalscaler"),
	}

	if err := exporter.registerMetrics(); err != nil {
		return nil, fmt.Errorf("failed to register metrics: %w", err)
	}

	return exporter, nil
}

// registerMetrics registers the OpenTelemetry metrics with callback functions
func (e *KafkaMetricsExporter) registerMetrics() error {
	var err error

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

	return nil
}

// observeConsumerLag is the callback for consumer lag metrics
func (e *KafkaMetricsExporter) observeConsumerLag(ctx context.Context, observer metric.Int64Observer) error {
	metrics := e.monitor.GetDetailedMetrics()

	for _, info := range metrics {
		serviceName := "unknown"
		if e.topicRegistry != nil {
			serviceName = e.topicRegistry.GetServiceNameByConsumerGroup(info.GroupID)
		}

		observer.Observe(info.Lag,
			metric.WithAttributes(
				attribute.String("topic", info.Topic),
				attribute.String("consumer_group", info.GroupID),
				attribute.Int("partition", info.Partition),
				attribute.String("service", serviceName),
			),
		)
	}
	return nil
}
