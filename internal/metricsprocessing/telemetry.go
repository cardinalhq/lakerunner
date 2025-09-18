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

	"github.com/google/uuid"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	// Metrics for processing pipeline (shared between compaction and rollup)
	processingSegmentsIn            metric.Int64Counter
	processingSegmentsOut           metric.Int64Counter
	processingRecordsIn             metric.Int64Counter
	processingRecordsOut            metric.Int64Counter
	processingBytesIn               metric.Int64Counter
	processingBytesOut              metric.Int64Counter
	processingSegmentDownloadMisses metric.Int64Counter
)

func reportTelemetry(ctx context.Context, signal, action string, segmentsIn, segmentsOut, recordsIn, recordsOut, bytesIn, bytesOut int64) {
	attrset := attribute.NewSet(
		attribute.String("action", action),
		attribute.String("signal", signal),
	)
	processingSegmentsIn.Add(ctx, segmentsIn, metric.WithAttributeSet(attrset))
	processingSegmentsOut.Add(ctx, segmentsOut, metric.WithAttributeSet(attrset))
	processingRecordsIn.Add(ctx, recordsIn, metric.WithAttributeSet(attrset))
	processingRecordsOut.Add(ctx, recordsOut, metric.WithAttributeSet(attrset))
	processingBytesIn.Add(ctx, bytesIn, metric.WithAttributeSet(attrset))
	processingBytesOut.Add(ctx, bytesOut, metric.WithAttributeSet(attrset))
}

func recordMetricSegmentDownload404(
	ctx context.Context,
	bucket string,
	organizationID uuid.UUID,
	instanceNum int16,
	createdBy lrdb.CreatedBy,
	frequencyMs int32,
) {
	recordSegmentDownload404(
		ctx,
		"metrics",
		bucket,
		organizationID,
		instanceNum,
		createdBy,
		attribute.Int("frequency_ms", int(frequencyMs)),
	)
}

func recordLogSegmentDownload404(
	ctx context.Context,
	bucket string,
	organizationID uuid.UUID,
	instanceNum int16,
	createdBy lrdb.CreatedBy,
) {
	recordSegmentDownload404(ctx, "logs", bucket, organizationID, instanceNum, createdBy)
}

func recordTraceSegmentDownload404(
	ctx context.Context,
	bucket string,
	organizationID uuid.UUID,
	instanceNum int16,
	createdBy lrdb.CreatedBy,
) {
	recordSegmentDownload404(ctx, "traces", bucket, organizationID, instanceNum, createdBy)
}

func recordSegmentDownload404(
	ctx context.Context,
	signal string,
	bucket string,
	organizationID uuid.UUID,
	instanceNum int16,
	createdBy lrdb.CreatedBy,
	extraAttrs ...attribute.KeyValue,
) {
	attrs := []attribute.KeyValue{
		attribute.String("signal", signal),
		attribute.String("bucket", bucket),
		attribute.String("organization_id", organizationID.String()),
		attribute.Int("instance_num", int(instanceNum)),
		attribute.String("created_by", createdBy.String()),
	}
	attrs = append(attrs, extraAttrs...)

	attrset := attribute.NewSet(attrs...)
	processingSegmentDownloadMisses.Add(ctx, 1, metric.WithAttributeSet(attrset))
}

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

	processingSegmentDownloadMisses, err = meter.Int64Counter(
		"lakerunner.processing.segment_downloads.not_found",
		metric.WithDescription("Number of segment download attempts that returned 404"),
	)
	if err != nil {
		log.Fatalf("failed to create processing.segment_downloads.not_found counter: %v", err)
	}

}
