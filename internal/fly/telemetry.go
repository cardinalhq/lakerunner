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
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var (
	messagesSentCounter  otelmetric.Int64Counter
	messagesErrorCounter otelmetric.Int64Counter
	bytesSentCounter     otelmetric.Int64Counter
	pendingMessagesGauge otelmetric.Int64UpDownCounter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/fly")

	var err error
	messagesSentCounter, err = meter.Int64Counter(
		"lakerunner.fly.producer.messages.sent",
		otelmetric.WithDescription("Number of Kafka messages successfully sent"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create messages.sent counter: %w", err))
	}

	messagesErrorCounter, err = meter.Int64Counter(
		"lakerunner.fly.producer.messages.errors",
		otelmetric.WithDescription("Number of Kafka message send errors"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create messages.errors counter: %w", err))
	}

	bytesSentCounter, err = meter.Int64Counter(
		"lakerunner.fly.producer.bytes.sent",
		otelmetric.WithDescription("Total bytes sent to Kafka"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create bytes.sent counter: %w", err))
	}

	pendingMessagesGauge, err = meter.Int64UpDownCounter(
		"lakerunner.fly.producer.messages.pending",
		otelmetric.WithDescription("Number of Kafka messages pending to be sent"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create messages.pending counter: %w", err))
	}
}

// recordSentMetrics updates counters for a batch of messages.
func recordSentMetrics(ctx context.Context, topic string, msgs []Message, err error) {
	attrs := otelmetric.WithAttributes(attribute.String("topic", topic))
	if err != nil {
		messagesErrorCounter.Add(ctx, int64(len(msgs)), attrs)
		return
	}
	messagesSentCounter.Add(ctx, int64(len(msgs)), attrs)
	var totalBytes int64
	for _, m := range msgs {
		totalBytes += int64(len(m.Value))
	}
	bytesSentCounter.Add(ctx, totalBytes, attrs)
}

// recordPendingDelta updates the pending messages gauge.
func recordPendingDelta(ctx context.Context, topic string, delta int64) {
	pendingMessagesGauge.Add(ctx, delta, otelmetric.WithAttributes(attribute.String("topic", topic)))
}
