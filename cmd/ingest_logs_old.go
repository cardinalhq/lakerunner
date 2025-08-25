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
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/fileconv/jsongz"
	protoconv "github.com/cardinalhq/lakerunner/fileconv/proto"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// oldSlotHourBoundary combines slot information with hour boundary for old log processing
type oldSlotHourBoundary struct {
	SlotID       int
	HourBoundary helpers.HourBoundary
}

func runOldLogIngestion(ctx context.Context, ll *slog.Logger, loop *IngestLoopContext) error {
	ll.Info("Using old log ingestion path (LAKERUNNER_LOG_OLDPATH is set)")
	return IngestLoopWithBatch(loop, oldLogIngestItem, oldLogIngestBatch)
}

func oldLogIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	// Extract collector name from object path for proper storage profile lookup
	var profile storageprofile.StorageProfile
	var err error

	if collectorName := helpers.ExtractCollectorName(inf.ObjectID); collectorName != "" {
		// Use collector-specific storage profile
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, inf.OrganizationID, collectorName)
		if err != nil {
			ll.Error("Failed to get storage profile for collector",
				slog.String("collectorName", collectorName), slog.Any("error", err))
			return err
		}
	} else {
		// Use instance-specific storage profile
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, inf.OrganizationID, inf.InstanceNum)
		if err != nil {
			ll.Error("Failed to get storage profile", slog.Any("error", err))
			return err
		}
	}
	if profile.Bucket != inf.Bucket {
		ll.Error("Bucket ID mismatch", slog.String("expected", profile.Bucket), slog.String("actual", inf.Bucket))
		return errors.New("bucket ID mismatch")
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
		ll.Info("S3 object not found, deleting inqueue work item", slog.String("bucket", inf.Bucket), slog.String("objectID", inf.ObjectID))
		return nil
	}

	filenames := []string{tmpfilename}

	// If the file is not in our `otel-raw` prefix, check if we can convert it
	if !strings.HasPrefix(inf.ObjectID, "otel-raw/") {
		// Skip database files (these are processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			// TODO add counter for skipped files in the db prefix
			return nil
		}

		// Check file type and convert if supported
		if fnames, err := oldConvertFileIfSupported(ll, tmpfilename, tmpdir, inf.Bucket, inf.ObjectID, rpfEstimate); err != nil {
			ll.Error("Failed to convert file", slog.Any("error", err))
			// TODO add counter for failure to convert, probably in each convert function
			return err
		} else if len(fnames) == 0 {
			ll.Info("Empty source file, skipping", slog.String("objectID", inf.ObjectID))
			return nil
		} else if fnames != nil {
			filenames = fnames
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
		splitResults, err := buffet.ProcessAndSplit(ll, fh, tmpdir, ingest_dateint, rpfEstimate)
		if err != nil {
			ll.Error("Failed to fingerprint file", slog.Any("error", err))
			return err
		}

		// First, validate that all split results have proper hour boundaries
		// and collect unique slot+hour combinations for work queue creation
		slotHourTriggers := make(map[oldSlotHourBoundary]int64) // maps slot+hour boundary to earliest FirstTS

		for key, split := range splitResults {
			// Validate that this split doesn't cross hour boundaries - this would be a bug in split logic
			splitTimeRange := helpers.TimeRange{
				Start: helpers.UnixMillisToTime(split.FirstTS),
				End:   helpers.UnixMillisToTime(split.LastTS + 1), // +1 because end is exclusive in our ranges
			}

			if !helpers.IsSameDateintHour(splitTimeRange) {
				ll.Error("Split result crosses hour boundaries - this is a bug in split logic",
					slog.Any("key", key),
					slog.Int64("firstTS", split.FirstTS),
					slog.Int64("lastTS", split.LastTS),
					slog.Time("firstTime", splitTimeRange.Start),
					slog.Time("lastTime", splitTimeRange.End))
				return fmt.Errorf("split result for key %v crosses hour boundaries: %d to %d", key, split.FirstTS, split.LastTS)
			}

			// Verify the key's dateint/hour matches the actual time range
			expectedDateint, expectedHour := helpers.MSToDateintHour(split.FirstTS)
			if key.DateInt != expectedDateint || key.Hour != expectedHour {
				ll.Error("Split key doesn't match actual time range - this is a bug in split logic",
					slog.Any("key", key),
					slog.Int("expectedDateint", int(expectedDateint)),
					slog.Int("expectedHour", int(expectedHour)),
					slog.Int64("firstTS", split.FirstTS))
				return fmt.Errorf("split key dateint/hour (%d/%d) doesn't match time range dateint/hour (%d/%d)",
					key.DateInt, key.Hour, expectedDateint, expectedHour)
			}

			// Calculate slot_id using the first fingerprint (or 0 if none)
			fps := split.Fingerprints.ToSlice()
			var firstFingerprint int64
			if len(fps) > 0 {
				firstFingerprint = fps[0]
			}
			slotID := ingestlogs.DetermineLogSlot(firstFingerprint, key.DateInt, inf.OrganizationID.String())

			// Collect unique slot+hour boundaries for work queue creation
			hourBoundary := helpers.HourBoundary{DateInt: key.DateInt, Hour: key.Hour}
			slotHourKey := oldSlotHourBoundary{SlotID: slotID, HourBoundary: hourBoundary}
			if existingTS, exists := slotHourTriggers[slotHourKey]; !exists || split.FirstTS < existingTS {
				slotHourTriggers[slotHourKey] = split.FirstTS
			}
		}

		// Process each split result - upload files and insert into database
		for key, split := range splitResults {
			segmentID := s3helper.GenerateID()
			dbObjectID := helpers.MakeDBObjectID(inf.OrganizationID, inf.CollectorName, key.DateInt, s3helper.HourFromMillis(split.FirstTS), segmentID, "logs")

			if err := s3helper.UploadS3Object(ctx, s3client, inf.Bucket, dbObjectID, split.FileName); err != nil {
				ll.Error("Failed to upload S3 object", slog.Any("error", err))
				return err
			}
			_ = os.Remove(split.FileName)

			fps := split.Fingerprints.ToSlice()
			t0 := time.Now()
			split.LastTS++ // end is exclusive, so we need to increment it by 1ms
			// Calculate slot_id using the first fingerprint (or 0 if none)
			var firstFingerprint int64
			if len(fps) > 0 {
				firstFingerprint = fps[0]
			}
			slotID := ingestlogs.DetermineLogSlot(firstFingerprint, key.DateInt, inf.OrganizationID.String())

			err = mdb.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
				OrganizationID: inf.OrganizationID,
				Dateint:        key.DateInt,
				IngestDateint:  ingest_dateint,
				SegmentID:      segmentID,
				InstanceNum:    inf.InstanceNum,
				SlotID:         int32(slotID),
				StartTs:        split.FirstTS,
				EndTs:          split.LastTS,
				RecordCount:    split.RecordCount,
				FileSize:       split.FileSize,
				CreatedBy:      lrdb.CreatedByIngest,
				Fingerprints:   fps,
			})
			dbExecDuration.Record(ctx, time.Since(t0).Seconds(),
				metric.WithAttributeSet(commonAttributes),
				metric.WithAttributes(
					attribute.Bool("hasError", err != nil),
					attribute.String("queryName", "InsertLogSegment"),
				))
			if err != nil {
				ll.Error("Failed to insert log segments", slog.Any("error", err))
				return err
			}

		}

		// Create work queue items for log compaction - one per slot+hour combination
		// This follows the same pattern as traces but with slot-based grouping
		for slotHourKey, earliestTS := range slotHourTriggers {
			ll.Info("Queueing log compaction for slot+hour",
				slog.Int("slotID", slotHourKey.SlotID),
				slog.Int("dateint", int(slotHourKey.HourBoundary.DateInt)),
				slog.Int("hour", int(slotHourKey.HourBoundary.Hour)),
				slog.Int64("triggerTS", earliestTS))

			// Create hour-aligned timestamp for the work queue (same as logs)
			hourAlignedTS := helpers.TruncateToHour(helpers.UnixMillisToTime(earliestTS)).UnixMilli()

			if err := queueOldLogCompactionForSlot(ctx, mdb, inf, slotHourKey.SlotID, slotHourKey.HourBoundary.DateInt, hourAlignedTS); err != nil {
				ll.Error("Failed to queue log compaction",
					slog.Any("slotHourKey", slotHourKey),
					slog.Int64("triggerTS", earliestTS),
					slog.Any("error", err))
				return err
			}
		}
	}

	return nil
}

// queueOldLogCompactionForSlot queues a log compaction job for a specific slot
func queueOldLogCompactionForSlot(ctx context.Context, mdb lrdb.StoreFull, inf lrdb.Inqueue, slotID int, dateint int32, hourAlignedTS int64) error {

	return mdb.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		OrgID:      inf.OrganizationID,
		Instance:   inf.InstanceNum,
		Signal:     lrdb.SignalEnumLogs,
		Action:     lrdb.ActionEnumCompact,
		Dateint:    dateint,
		Frequency:  -1,
		SlotID:     int32(slotID),
		TsRange:    qmcFromInqueue(inf, 3600000, hourAlignedTS).TsRange,
		RunnableAt: time.Now().UTC().Add(5 * time.Minute),
	})
}

// oldConvertFileIfSupported checks the file type and converts it if supported.
// Returns nil if the file type is not supported (file will be skipped).
func oldConvertFileIfSupported(ll *slog.Logger, tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64) ([]string, error) {
	// TODO add a counter for each type we process, and a counter for unsupported types
	// Include the signal type in the attributes, as well as the converter used, and the extension found.
	switch {
	case strings.HasSuffix(objectID, ".parquet"):
		return ingestlogs.ConvertRawParquet(tmpfilename, tmpdir, bucket, objectID, rpfEstimate)
	case strings.HasSuffix(objectID, ".json.gz"):
		return oldConvertJSONGzFile(tmpfilename, tmpdir, bucket, objectID, rpfEstimate)
	case strings.HasSuffix(objectID, ".binpb"):
		return oldConvertProtoFile(tmpfilename, tmpdir, bucket, objectID, rpfEstimate)
	default:
		ll.Warn("Unsupported file type, skipping", slog.String("objectID", objectID))
		return nil, nil
	}
}

// oldConvertJSONGzFile converts a JSON.gz file to the standardized format
func oldConvertJSONGzFile(tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64) ([]string, error) {
	// Create a mapper that recognizes "date" as a timestamp field
	mapper := translate.NewMapper(
		translate.WithTimestampColumn("date"),
		translate.WithTimeFormat(time.RFC3339Nano),
	)

	r, err := jsongz.NewJSONGzReader(tmpfilename, mapper, nil)
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

	var closed bool
	defer func() {
		if !closed {
			_, err := w.Close()
			if err != buffet.ErrAlreadyClosed && err != nil {
				slog.Error("Failed to close writer", slog.Any("error", err))
			}
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
	closed = true
	if len(result) == 0 {
		return nil, fmt.Errorf("no records written to file")
	}

	var fnames []string
	for _, res := range result {
		fnames = append(fnames, res.FileName)
	}
	return fnames, nil
}

// oldConvertProtoFile converts a protobuf file to the standardized format
func oldConvertProtoFile(tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64) ([]string, error) {
	// Create a mapper for protobuf files
	mapper := translate.NewMapper()

	r, err := protoconv.NewLogsProtoReader(tmpfilename, mapper, nil)
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

func oldLogIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	if len(items) == 0 {
		return fmt.Errorf("empty batch")
	}

	ll.Info("Processing log batch (old path)", slog.Int("batchSize", len(items)))

	var profile storageprofile.StorageProfile
	var err error

	firstItem := items[0]
	if collectorName := helpers.ExtractCollectorName(firstItem.ObjectID); collectorName != "" {
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, firstItem.OrganizationID, collectorName)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for collector %s: %w", collectorName, err)
		}
	} else {
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for organization and instance: %w", err)
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	allParquetFiles := make([]string, 0)

	for _, inf := range items {
		ll.Info("Processing batch item",
			slog.String("itemID", inf.ID.String()),
			slog.String("objectID", inf.ObjectID),
			slog.Int64("fileSize", inf.FileSize))

		itemTmpdir := fmt.Sprintf("%s/item_%s", tmpdir, inf.ID.String())
		if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
			return fmt.Errorf("creating item tmpdir: %w", err)
		}

		tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, itemTmpdir, s3client, inf.Bucket, inf.ObjectID)
		if err != nil {
			return fmt.Errorf("failed to download file %s: %w", inf.ObjectID, err)
		}
		if is404 {
			ll.Warn("S3 object not found, skipping", slog.String("itemID", inf.ID.String()), slog.String("objectID", inf.ObjectID))
			continue
		}

		convertedFiles, err := oldConvertFileIfSupported(ll, tmpfilename, itemTmpdir, inf.Bucket, inf.ObjectID, rpfEstimate)
		if err != nil {
			return fmt.Errorf("failed to convert logs file %s: %w", inf.ObjectID, err)
		}
		if convertedFiles == nil {
			ll.Warn("Unsupported file type, skipping", slog.String("objectID", inf.ObjectID))
			continue
		}

		allParquetFiles = append(allParquetFiles, convertedFiles...)
	}

	if len(allParquetFiles) == 0 {
		ll.Info("No files to process in batch")
		return nil
	}

	batchTmpdir := fmt.Sprintf("%s/batch_output", tmpdir)
	if err := os.MkdirAll(batchTmpdir, 0755); err != nil {
		return fmt.Errorf("creating batch tmpdir: %w", err)
	}

	finalResults := make(map[buffet.SplitKey]buffet.HourlyResult)

	for _, fname := range allParquetFiles {
		fh, err := filecrunch.LoadSchemaForFile(fname)
		if err != nil {
			return fmt.Errorf("failed to open converted file %s: %w", fname, err)
		}

		fileResults, err := buffet.ProcessAndSplit(ll, fh, batchTmpdir, ingest_dateint, rpfEstimate)
		fh.Close()
		if err != nil {
			return fmt.Errorf("failed to process and split file %s: %w", fname, err)
		}

		for key, result := range fileResults {
			if existing, exists := finalResults[key]; exists {
				existing.Fingerprints = existing.Fingerprints.Union(result.Fingerprints)
				if result.FirstTS < existing.FirstTS || existing.FirstTS == 0 {
					existing.FirstTS = result.FirstTS
				}
				if result.LastTS > existing.LastTS {
					existing.LastTS = result.LastTS
				}
				finalResults[key] = existing
			} else {
				finalResults[key] = result
			}
		}
	}

	for key, result := range finalResults {
		segmentID := s3helper.GenerateID()
		dbObjectID := helpers.MakeDBObjectID(firstItem.OrganizationID, firstItem.CollectorName, key.DateInt, s3helper.HourFromMillis(result.FirstTS), segmentID, "logs")

		if err := s3helper.UploadS3Object(ctx, s3client, firstItem.Bucket, dbObjectID, result.FileName); err != nil {
			return fmt.Errorf("failed to upload file to S3: %w", err)
		}
		_ = os.Remove(result.FileName)

		fps := result.Fingerprints.ToSlice()
		resultLastTS := result.LastTS + 1 // end is exclusive, so we need to increment it by 1ms
		err := mdb.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: firstItem.OrganizationID,
			Dateint:        key.DateInt,
			IngestDateint:  key.IngestDateint,
			SegmentID:      segmentID,
			InstanceNum:    firstItem.InstanceNum,
			StartTs:        result.FirstTS,
			EndTs:          resultLastTS,
			RecordCount:    result.RecordCount,
			FileSize:       result.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   fps,
		})
		if err != nil {
			ll.Error("Failed to insert log segments", slog.Any("error", err))
			return err
		}

		ll.Info("Inserted log segment from batch",
			slog.String("organizationID", firstItem.OrganizationID.String()),
			slog.Int("instanceNum", int(firstItem.InstanceNum)),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("hour", int(key.Hour)),
			slog.String("dbObjectID", dbObjectID),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", result.RecordCount),
			slog.Int64("fileSize", result.FileSize),
		)
	}

	// Create work queue items for compaction - one per unique dateint/hour combination
	hourlyTriggers := make(map[helpers.HourBoundary]int64) // maps hour boundary to earliest FirstTS

	for key, result := range finalResults {
		hourBoundary := helpers.HourBoundary{DateInt: key.DateInt, Hour: key.Hour}
		if existingTS, exists := hourlyTriggers[hourBoundary]; !exists || result.FirstTS < existingTS {
			hourlyTriggers[hourBoundary] = result.FirstTS
		}
	}

	for hourBoundary, earliestTS := range hourlyTriggers {
		ll.Info("Queueing log compaction for hour from batch",
			slog.Int("dateint", int(hourBoundary.DateInt)),
			slog.Int("hour", int(hourBoundary.Hour)),
			slog.Int64("triggerTS", earliestTS))

		// Create hour-aligned timestamp for the work queue
		hourAlignedTS := helpers.TruncateToHour(helpers.UnixMillisToTime(earliestTS)).UnixMilli()
		if err := queueLogCompaction(ctx, mdb, qmcFromInqueue(firstItem, 3600000, hourAlignedTS)); err != nil {
			ll.Error("Failed to queue log compaction from batch",
				slog.Any("hourBoundary", hourBoundary),
				slog.Int64("triggerTS", earliestTS),
				slog.Any("error", err))
			return err
		}
	}

	return nil
}
