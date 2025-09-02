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

package metricsprocessing

import (
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	// Metrics for processing pipeline (shared between compaction and rollup)
	processingSegmentsIn  metric.Int64Counter
	processingSegmentsOut metric.Int64Counter
	processingRecordsIn   metric.Int64Counter
	processingRecordsOut  metric.Int64Counter
	processingBytesIn     metric.Int64Counter
	processingBytesOut    metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing")

	var err error

	processingSegmentsIn, err = meter.Int64Counter(
		"lakerunner.processing.segments.in",
		metric.WithDescription("Number of segments input to processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.segments.in counter: %v", err)
	}

	processingSegmentsOut, err = meter.Int64Counter(
		"lakerunner.processing.segments.out",
		metric.WithDescription("Number of segments output from processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.segments.out counter: %v", err)
	}

	processingRecordsIn, err = meter.Int64Counter(
		"lakerunner.processing.records.in",
		metric.WithDescription("Number of records input to processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.records.in counter: %v", err)
	}

	processingRecordsOut, err = meter.Int64Counter(
		"lakerunner.processing.records.out",
		metric.WithDescription("Number of records output from processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.records.out counter: %v", err)
	}

	processingBytesIn, err = meter.Int64Counter(
		"lakerunner.processing.bytes.in",
		metric.WithDescription("Number of bytes input to processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.bytes.in counter: %v", err)
	}

	processingBytesOut, err = meter.Int64Counter(
		"lakerunner.processing.bytes.out",
		metric.WithDescription("Number of bytes output from processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.bytes.out counter: %v", err)
	}
}
