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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/fileconv/proto"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-metrics",
		Short: "Ingest metrics from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

			servicename := "lakerunner-ingest-metrics"
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "ingest"),
			)
			iter := attribute.NewMergeIterator(&commonAttributes, &addlAttrs)
			attrs := []attribute.KeyValue{}
			for iter.Next() {
				attrs = append(attrs, iter.Attribute())
			}
			commonAttributes = attribute.NewSet(attrs...)

			go diskUsageLoop(doneCtx)

			sp, err := storageprofile.SetupStorageProfiles()
			if err != nil {
				return fmt.Errorf("failed to setup storage profiles: %w", err)
			}

			return IngestLoop(doneCtx, sp, "metrics", servicename, metricIngestItem)
		},
	}

	rootCmd.AddCommand(cmd)
}

func metricIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64) error {
	profile, err := sp.Get(ctx, inf.OrganizationID, inf.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}
	if profile.Role == "" {
		if !profile.Hosted {
			ll.Error("No role on non-hosted profile")
			return err
		}
	}
	if profile.Bucket != inf.Bucket {
		ll.Error("Bucket ID mismatch", slog.String("expected", profile.Bucket), slog.String("actual", inf.Bucket))
		return errors.New("bucket ID mismatch")
	}
	if profile.Role == "" {
		if !profile.Hosted {
			ll.Info("No role on non-hosted profile")
			return err
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, inf.Bucket, inf.ObjectID)
	if err != nil {
		ll.Error("Failed to download S3 object", slog.Any("error", err))
		return err
	}
	if is404 {
		ll.Info("S3 object not found, skipping", slog.String("bucket", inf.Bucket), slog.String("objectID", inf.ObjectID))
		return nil
	}

	filenames := []string{tmpfilename}

	// If the file is not in our `otel-raw` prefix, check if we can convert it
	if !strings.HasPrefix(inf.ObjectID, "otel-raw/") {
		// Skip database files (these are processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			return nil
		}

		// Check file type and convert if supported
		if fnames, err := convertMetricsFileIfSupported(ll, tmpfilename, tmpdir, inf.Bucket, inf.ObjectID, rpfEstimate); err != nil {
			ll.Error("Failed to convert file", slog.Any("error", err))
			return err
		} else if fnames != nil {
			filenames = fnames
			ll.Info("Converted file", slog.String("filename", tmpfilename), slog.String("objectID", inf.ObjectID))
		}
	}

	for _, fname := range filenames {
		fh, err := filecrunch.LoadSchemaForFile(fname)
		if err != nil {
			ll.Error("Failed to load schema for file", slog.Any("error", err))
			return err
		}
		defer func() {
			_ = fh.Close()
		}()

		if err := crunchMetricFile(ctx, ll, tmpdir, fh, inf, s3client, mdb, ingest_dateint, rpfEstimate); err != nil {
			ll.Error("Failed to crunch metric file", slog.Any("error", err), slog.String("file", fname))
			return err
		}
	}

	return nil
}

type TimeBlock struct {
	Block       int64
	FrequencyMS int32
	Sketches    *map[int64]TagSketch
	nodebuilder *buffet.NodeMapBuilder
}

type TagSketch struct {
	MetricName string
	MetricType string
	Tags       map[string]any
	Sketch     *ddsketch.DDSketch
}

// crunchMetricFile processes the metric file and generates sketches or other
func crunchMetricFile(ctx context.Context, ll *slog.Logger, tmpdir string, fh *filecrunch.FileHandle, inf lrdb.Inqueue, s3client *awsclient.S3Client, mdb lrdb.StoreFull, ingest_dateint int32, rpfEstimate int64) error {
	reader := parquet.NewReader(fh.File, fh.Schema)
	defer reader.Close()

	blocks := map[int64]*TimeBlock{}

	for {
		rec := map[string]any{}
		if err := reader.Read(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading parquet: %w", err)
		}
		delete(rec, "_cardinalhq.id")

		ts, ok := helpers.GetInt64Value(rec, "_cardinalhq.timestamp")
		if !ok {
			ll.Warn("Skipping record without timestamp", slog.Any("record", rec))
			continue
		}
		delete(rec, "_cardinalhq.timestamp")

		metricType, ok := helpers.GetStringValue(rec, "_cardinalhq.metric_type")
		if !ok {
			ll.Warn("Skipping record without metric type", slog.Any("record", rec))
			continue
		}
		if metricType == "count" || metricType == "gauge" {
			delete(rec, "_cardinalhq.bucket_bounds")
			delete(rec, "_cardinalhq.counts")
			delete(rec, "_cardinalhq.negative_counts")
			delete(rec, "_cardinalhq.positive_counts")
		} else {
			delete(rec, "_cardinalhq.value")
		}

		metricName, ok := helpers.GetStringValue(rec, "_cardinalhq.name")
		if !ok {
			ll.Warn("Skipping record without metric name", slog.Any("record", rec))
			continue
		}

		const frequencyMS = 10000 // 10 seconds

		blocknum := ts / frequencyMS
		rec["_cardinalhq.timestamp"] = blocknum * frequencyMS

		block, exists := blocks[blocknum]
		if !exists {
			block = &TimeBlock{
				Block:       blocknum,
				FrequencyMS: frequencyMS,
				Sketches:    &map[int64]TagSketch{},
				nodebuilder: buffet.NewNodeMapBuilder(),
			}
			blocks[blocknum] = block
		}

		tags := helpers.MakeTags(rec)
		if err := block.nodebuilder.Add(tags); err != nil {
			return fmt.Errorf("adding tags to node builder: %w", err)
		}

		tid := helpers.ComputeTID(metricName, tags)
		tags["_cardinalhq.tid"] = fmt.Sprintf("%d", tid)

		sketch, exists := (*block.Sketches)[tid]
		if !exists {
			sketch = TagSketch{
				MetricName: metricName,
				MetricType: metricType,
				Tags:       tags,
				Sketch:     nil,
			}
			s, err := ddsketch.NewDefaultDDSketch(0.01)
			if err != nil {
				return fmt.Errorf("creating sketch: %w", err)
			}
			sketch.Sketch = s
			(*block.Sketches)[tid] = sketch
		} else {
			if sketch.MetricName != metricName {
				return fmt.Errorf("metric name mismatch for TID %d: existing %s, new %s", tid, sketch.MetricName, metricName)
			}
			if sketch.MetricType != metricType {
				return fmt.Errorf("metric type mismatch for TID %d: existing %s, new %s", tid, sketch.MetricType, metricType)
			}
			diff := helpers.MatchTags(sketch.Tags, tags)
			if len(diff) > 0 {
				return fmt.Errorf("tag mismatch for TID %d: diff %v", tid, diff)
			}
		}

		switch metricType {
		case "count", "gauge":
			value, ok := helpers.GetFloat64Value(rec, "_cardinalhq.value")
			if !ok {
				ll.Warn("Skipping record without value", slog.Any("record", rec))
				continue
			}
			rec["_cardinalhq.value"] = -1
			if err := sketch.Sketch.Add(value); err != nil {
				return fmt.Errorf("adding value to sketch: %w", err)
			}
		case "histogram":
			bucketCounts, ok := helpers.GetFloat64SliceJSON(rec, "_cardinalhq.counts")
			if !ok {
				ll.Warn("Skipping histogram record without counts", slog.Any("record", rec))
				continue
			}
			delete(rec, "_cardinalhq.counts")
			bucketBounds, ok := helpers.GetFloat64SliceJSON(rec, "_cardinalhq.bucket_bounds")
			if !ok {
				ll.Warn("Skipping histogram record without bucket bounds", slog.Any("record", rec))
				continue
			}
			delete(rec, "_cardinalhq.bucket_bounds")
			counts, values := handleHistogram(bucketCounts, bucketBounds)
			if len(counts) == 0 {
				// if err := sketch.Sketch.Add(0); err != nil {
				// 	return fmt.Errorf("adding zero to sketch: %w", err)
				// }
				continue
			}
			for i, count := range counts {
				if err := sketch.Sketch.AddWithCount(values[i], count); err != nil {
					return fmt.Errorf("adding histogram value to sketch: %w", err)
				}
			}

		default:
			ll.Info("Skipping unsupported metric type", slog.String("metricType", metricType))
			continue
		}
	}

	ll.Info("Finished processing metric file", slog.String("file", fh.File.Name()), slog.Int("blocks", len(blocks)))

	// print out all blocks, and the metric names and sketch value
	for blocknum, block := range blocks {
		if len(*block.Sketches) == 0 {
			ll.Info("Skipping empty block", slog.Int64("blocknum", blocknum))
			continue
		}

		ll.Info("Processing block",
			slog.Int64("blocknum", blocknum),
			slog.Int("nSketches", len(*block.Sketches)),
			slog.Int64("frequencyMS", int64(block.FrequencyMS)),
			slog.Int64("startTS", block.Block*int64(block.FrequencyMS)),
			slog.Int64("endTS", (block.Block+1)*int64(block.FrequencyMS)-1),
		)

		err := writeMetricSketchParquet(ctx, tmpdir, blocknum, block, inf, s3client, ll, mdb, ingest_dateint, rpfEstimate)
		if err != nil {
			return fmt.Errorf("writing metric sketch parquet: %w", err)
		}
	}

	return nil
}

func writeMetricSketchParquet(ctx context.Context, tmpdir string, blocknum int64, block *TimeBlock, inf lrdb.Inqueue, s3client *awsclient.S3Client, ll *slog.Logger, mdb lrdb.StoreFull, ingest_dateint int32, rpfEstimate int64) error {
	addedNodes := map[string]any{
		"_cardinalhq.timestamp":      int64(1),
		"_cardinalhq.name":           "x",
		"_cardinalhq.customer_id":    "x",
		"_cardinalhq.collector_id":   "x",
		"_cardinalhq.metric_type":    "x",
		"_cardinalhq.tid":            int64(1),
		"_cardinalhq.value":          float64(1),
		"_cardinalhq.telemetry_type": "metrics",
		"sketch":                     []byte{},
		"rollup_avg":                 float64(1),
		"rollup_max":                 float64(1),
		"rollup_min":                 float64(1),
		"rollup_count":               float64(1),
		"rollup_sum":                 float64(1),
		"rollup_p25":                 float64(1),
		"rollup_p50":                 float64(1),
		"rollup_p75":                 float64(1),
		"rollup_p90":                 float64(1),
		"rollup_p95":                 float64(1),
		"rollup_p99":                 float64(1),
	}
	if err := block.nodebuilder.Add(addedNodes); err != nil {
		return fmt.Errorf("adding nodes to node builder: %w", err)
	}
	nodes := block.nodebuilder.Build()
	pw, err := buffet.NewWriter("metrics", tmpdir, nodes, rpfEstimate)
	if err != nil {
		return fmt.Errorf("creating buffet writer: %w", err)
	}

	startTS := block.Block * int64(block.FrequencyMS)
	endTS := startTS + int64(block.FrequencyMS)

	sortedTIDs := make([]int64, 0, len(*block.Sketches))
	for tid := range *block.Sketches {
		sortedTIDs = append(sortedTIDs, tid)
	}
	slices.Sort(sortedTIDs)

	for _, tid := range sortedTIDs {
		sketch := (*block.Sketches)[tid]
		if sketch.Sketch == nil || sketch.Sketch.GetCount() == 0 {
			continue
		}
		maxvalue, err := sketch.Sketch.GetMaxValue()
		if err != nil {
			return fmt.Errorf("getting max value from sketch: %w", err)
		}

		minvalue, err := sketch.Sketch.GetMinValue()
		if err != nil {
			return fmt.Errorf("getting min value from sketch: %w", err)
		}

		quantiles, err := sketch.Sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})
		if err != nil {
			return fmt.Errorf("getting quantiles from sketch: %w", err)
		}

		count := sketch.Sketch.GetCount()
		sum := sketch.Sketch.GetSum()
		avg := sum / count

		addToRec := map[string]any{
			"_cardinalhq.timestamp":      startTS,
			"_cardinalhq.name":           sketch.MetricName,
			"_cardinalhq.customer_id":    inf.OrganizationID.String(),
			"_cardinalhq.collector_id":   inf.CollectorName,
			"_cardinalhq.metric_type":    sketch.MetricType,
			"_cardinalhq.tid":            tid,
			"_cardinalhq.value":          float64(-1),
			"_cardinalhq.telemetry_type": "metrics",
			"sketch":                     EncodeSketch(sketch.Sketch),
			"rollup_avg":                 avg,
			"rollup_max":                 maxvalue,
			"rollup_min":                 minvalue,
			"rollup_count":               count,
			"rollup_sum":                 sum,
			"rollup_p25":                 quantiles[0],
			"rollup_p50":                 quantiles[1],
			"rollup_p75":                 quantiles[2],
			"rollup_p90":                 quantiles[3],
			"rollup_p95":                 quantiles[4],
			"rollup_p99":                 quantiles[5],
		}
		rec := map[string]any{}
		maps.Copy(rec, sketch.Tags)
		maps.Copy(rec, addToRec)

		if err := pw.Write(rec); err != nil {
			_, _ = pw.Close()
			return fmt.Errorf("writing record to parquet: %w", err)
		}
	}

	stats, err := pw.Close()
	if err != nil {
		return fmt.Errorf("closing parquet writer: %w", err)
	}
	if stats == nil {
		ll.Info("No records written for block", slog.Int64("blocknum", blocknum))
		return nil
	}

	dateint, hour := helpers.MSToDateintHour(startTS)

	for _, stat := range stats {
		ll.Info("Wrote metric sketch parquet",
			slog.String("organizationID", inf.OrganizationID.String()),
			slog.String("collectorName", inf.CollectorName),
			slog.Int64("blocknum", blocknum),
			slog.String("file", stat.FileName),
			slog.Int64("recordcount", stat.RecordCount),
			slog.Int64("filesize", stat.FileSize),
		)

		// Upload the file to S3
		segmentID := s3helper.GenerateID()
		objID := helpers.MakeDBObjectID(inf.OrganizationID, inf.CollectorName, dateint, hour, segmentID, "metrics")
		if err := s3helper.UploadS3Object(ctx, s3client, inf.Bucket, objID, stat.FileName); err != nil {
			return fmt.Errorf("uploading file to S3: %w", err)
		}

		t0 := time.Now()
		err = mdb.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: inf.OrganizationID,
			FrequencyMs:    block.FrequencyMS,
			Dateint:        dateint,
			IngestDateint:  ingest_dateint,
			TidPartition:   0,
			SegmentID:      segmentID,
			InstanceNum:    inf.InstanceNum,
			StartTs:        startTS,
			EndTs:          endTS,
			RecordCount:    stat.RecordCount,
			FileSize:       stat.FileSize,
			Published:      true,
		})
		dbExecDuration.Record(ctx, time.Since(t0).Seconds(),
			metric.WithAttributeSet(commonAttributes),
			metric.WithAttributes(
				attribute.Bool("hasError", err != nil),
				attribute.String("queryName", "InsertMetricSegment"),
			))
		if err != nil {
			ll.Error("Failed to insert metric segment", slog.Any("error", err))
			// Clean up the uploaded file if insertion fails
			if err2 := s3helper.DeleteS3Object(ctx, s3client, inf.Bucket, objID); err2 != nil {
				ll.Error("Failed to delete S3 object after insertion failure", slog.Any("error", err2), slog.String("objectID", objID))
				return fmt.Errorf("failed to delete S3 object after insertion failure: %w", err2)
			}
			ll.Info("Deleted S3 object after insertion failure", slog.String("objectID", objID))
			return fmt.Errorf("inserting metric segment: %w", err)
		}

		ll.Info("Inserted metric segment and uploaded to S3",
			slog.String("organizationID", inf.OrganizationID.String()),
			slog.String("collectorName", inf.CollectorName),
			slog.Int64("blocknum", blocknum),
			slog.String("objectID", objID),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", stat.RecordCount),
			slog.Int64("fileSize", stat.FileSize),
			slog.String("bucket", inf.Bucket),
			slog.Int64("startTs", startTS),
			slog.Int64("endTs", endTS),
		)
	}

	if err := queueMetricCompaction(ctx, mdb, qmcFromInqueue(inf, block.FrequencyMS, startTS)); err != nil {
		return fmt.Errorf("queueing metric compaction: %w", err)
	}
	if err := queueMetricRollup(ctx, mdb, qmcFromInqueue(inf, block.FrequencyMS, startTS)); err != nil {
		return fmt.Errorf("queueing metric rollup: %w", err)
	}

	return nil
}

// handleHistogram fills the sketch with representative values for each bucket count.
// If bucketCounts[i] > 0, it inserts the midpoint of the bucket that bucketCounts[i] represents.
func handleHistogram(bucketCounts []float64, bucketBounds []float64) (counts, values []float64) {
	const maxTrackableValue = 1e9

	counts = []float64{}
	values = []float64{}

	if len(bucketCounts) == 0 || len(bucketBounds) == 0 {
		return counts, values
	}
	if len(bucketCounts) > len(bucketBounds)+1 {
		return counts, values
	}

	for i, count := range bucketCounts {
		if count <= 0 {
			continue
		}
		var value float64
		if i < len(bucketBounds) {
			var lowerBound float64
			if i == 0 {
				lowerBound = 1e-10 // very small lower bound
			} else {
				lowerBound = bucketBounds[i-1]
			}
			upperBound := bucketBounds[i]
			value = (lowerBound + upperBound) / 2.0
		} else {
			value = min(bucketBounds[len(bucketBounds)-1]+1, maxTrackableValue)
		}

		if value <= maxTrackableValue {
			counts = append(counts, count)
			values = append(values, value)
		}
	}
	return counts, values
}

// convertMetricsFileIfSupported checks the file type and converts it if supported.
// Returns nil if the file type is not supported (file will be skipped).
func convertMetricsFileIfSupported(ll *slog.Logger, tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64) ([]string, error) {
	switch {
	case strings.HasSuffix(objectID, ".binpb"):
		return convertMetricsProtoFile(tmpfilename, tmpdir, bucket, objectID, rpfEstimate)
	default:
		ll.Warn("Unsupported file type for metrics, skipping", slog.String("objectID", objectID))
		return nil, nil
	}
}

// convertMetricsProtoFile converts a protobuf file to the standardized format
func convertMetricsProtoFile(tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64) ([]string, error) {
	// Create a mapper for protobuf files
	mapper := translate.NewMapper()

	r, err := proto.NewMetricsProtoReader(tmpfilename, mapper, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	nmb := buffet.NewNodeMapBuilder()

	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   ingestlogs.GetFileType(objectID),
	}

	// First pass: read all rows to build complete schema
	allRows := make([]map[string]any, 0)
	for {
		row, done, err := r.GetRow()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}

		// Add base items to the row
		for k, v := range baseitems {
			row[k] = v
		}

		// Add row to schema builder
		if err := nmb.Add(row); err != nil {
			return nil, fmt.Errorf("failed to add row to schema: %w", err)
		}

		allRows = append(allRows, row)
	}

	if len(allRows) == 0 {
		return nil, fmt.Errorf("no rows processed")
	}

	// Create writer with complete schema
	w, err := buffet.NewWriter("fileconv", tmpdir, nmb.Build(), rpfEstimate)
	if err != nil {
		return nil, err
	}

	defer func() {
		_, err := w.Close()
		if err != buffet.ErrAlreadyClosed && err != nil {
			slog.Error("Failed to close writer", slog.Any("error", err))
		}
	}()

	// Second pass: write all rows
	for _, row := range allRows {
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write row: %w", err)
		}
	}

	result, err := w.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no records written to file")
	}

	var fnames []string
	for _, res := range result {
		fnames = append(fnames, res.FileName)
	}
	return fnames, nil
}
