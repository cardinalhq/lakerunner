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
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	// Metrics for processing pipeline (shared between compaction and rollup)
	processingSegmentsIn   metric.Int64Counter
	processingSegmentsOut  metric.Int64Counter
	processingFilesIn      metric.Int64Counter
	processingFilesOut     metric.Int64Counter
	processingRecordsIn    metric.Int64Counter
	processingRecordsOut   metric.Int64Counter
	processingBytesIn      metric.Int64Counter
	processingBytesOut     metric.Int64Counter
	processingBytesInHist  metric.Int64Histogram
	processingBytesOutHist metric.Int64Histogram
	processingDropped      metric.Int64Counter
)

func reportTelemetry(ctx context.Context, action string, segmentsIn, segmentsOut, recordsIn, recordsOut, bytesIn, bytesOut int64) {
	attrset := attribute.NewSet(
		attribute.String("action", action),
		attribute.String("signal", "metrics"),
	)
	processingSegmentsIn.Add(ctx, segmentsIn, metric.WithAttributeSet(attrset))
	processingSegmentsOut.Add(ctx, segmentsOut, metric.WithAttributeSet(attrset))
	processingFilesIn.Add(ctx, segmentsIn, metric.WithAttributeSet(attrset))
	processingFilesOut.Add(ctx, segmentsOut, metric.WithAttributeSet(attrset))
	processingRecordsIn.Add(ctx, recordsIn, metric.WithAttributeSet(attrset))
	processingRecordsOut.Add(ctx, recordsOut, metric.WithAttributeSet(attrset))
	processingBytesIn.Add(ctx, bytesIn, metric.WithAttributeSet(attrset))
	processingBytesOut.Add(ctx, bytesOut, metric.WithAttributeSet(attrset))
	processingBytesInHist.Record(ctx, bytesIn, metric.WithAttributeSet(attrset))
	processingBytesOutHist.Record(ctx, bytesOut, metric.WithAttributeSet(attrset))
}

func reportDrop(ctx context.Context, reason string, count int64) {
	attrset := attribute.NewSet(
		attribute.String("reason", reason),
		attribute.String("signal", "metrics"),
	)
	processingDropped.Add(ctx, count, metric.WithAttributeSet(attrset))
}

func initTelemetry() {
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

	processingFilesIn, err = meter.Int64Counter(
		"lakerunner.processing.files.in",
		metric.WithDescription("Number of files input to processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.files.in counter: %v", err)
	}

	processingFilesOut, err = meter.Int64Counter(
		"lakerunner.processing.files.out",
		metric.WithDescription("Number of files output from processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.files.out counter: %v", err)
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

	processingBytesInHist, err = meter.Int64Histogram(
		"lakerunner.processing.bytes.in.size",
		metric.WithDescription("Histogram of input bytes to processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.bytes.in.size histogram: %v", err)
	}

	processingBytesOutHist, err = meter.Int64Histogram(
		"lakerunner.processing.bytes.out.size",
		metric.WithDescription("Histogram of output bytes from processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.bytes.out.size histogram: %v", err)
	}

	processingDropped, err = meter.Int64Counter(
		"lakerunner.processing.dropped",
		metric.WithDescription("Number of drop decisions in processing pipeline"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.dropped counter: %v", err)
	}
}

func init() {
	initTelemetry()
}
