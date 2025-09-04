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

package pubsub

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	itemsProcessed metric.Int64Counter
	itemsSkipped   metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/pubsub")

	var err error
	itemsProcessed, err = meter.Int64Counter(
		"pubsub_items_processed_total",
		metric.WithDescription("Total number of pubsub items processed successfully"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create itemsProcessed counter: %w", err))
	}

	itemsSkipped, err = meter.Int64Counter(
		"pubsub_items_skipped_total",
		metric.WithDescription("Total number of pubsub items skipped"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create itemsSkipped counter: %w", err))
	}
}

// handleMessage has been removed - all messages now go through Kafka
// See kafka_handler.go for the new implementation
