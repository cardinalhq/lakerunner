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

package promql

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	logAggregationSourceCounter metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/promql")

	var err error

	logAggregationSourceCounter, err = meter.Int64Counter(
		"lakerunner.promql.log.aggregation.source",
		metric.WithDescription("Number of log aggregations by source (tbl or agg)"),
	)
	if err != nil {
		log.Fatalf("failed to create query.log.aggregation.source counter: %v", err)
	}
}

func RecordLogAggregationSource(ctx context.Context, organization_id, source string, instance_num int16) {
	logAggregationSourceCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("organization_id", organization_id),
		attribute.String("source", source),
		attribute.Int("instance_num", int(instance_num)),
	))
}
