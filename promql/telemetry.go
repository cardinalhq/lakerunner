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

package promql

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	logQuerySimpleCounter  metric.Int64Counter
	logQueryComplexCounter metric.Int64Counter
	logQueryAggCounter     metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/promql")

	var err error

	logQuerySimpleCounter, err = meter.Int64Counter(
		"lakerunner.query.log.simple",
		metric.WithDescription("Number of log queries using the simple flat SQL path"),
	)
	if err != nil {
		log.Fatalf("failed to create query.log.simple counter: %v", err)
	}

	logQueryComplexCounter, err = meter.Int64Counter(
		"lakerunner.query.log.complex",
		metric.WithDescription("Number of log queries using the complex CTE pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create query.log.complex counter: %v", err)
	}

	logQueryAggCounter, err = meter.Int64Counter(
		"lakerunner.query.log.agg",
		metric.WithDescription("Number of log segments queried using pre-aggregated agg_ files"),
	)
	if err != nil {
		log.Fatalf("failed to create query.log.agg counter: %v", err)
	}
}

func recordLogQuerySimple() {
	logQuerySimpleCounter.Add(context.Background(), 1)
}

func recordLogQueryComplex() {
	logQueryComplexCounter.Add(context.Background(), 1)
}

// RecordLogQueryAgg records the number of segments queried using agg_ files.
func RecordLogQueryAgg(segmentCount int) {
	logQueryAggCounter.Add(context.Background(), int64(segmentCount))
}
