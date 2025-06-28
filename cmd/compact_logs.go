// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/pkg/lockmgr"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-logs",
		Short: "Compact logs into optimally sized files",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "lakerunner-compact-logs"
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			compactLogsDoneCtx = doneCtx

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			sp, err := storageprofile.SetupStorageProfiles()
			if err != nil {
				return fmt.Errorf("failed to setup storage profiles: %w", err)
			}

			return RunqueueLoop(doneCtx, sp, "logs", "compact", servicename, compactLogsFor)
		},
	}
	rootCmd.AddCommand(cmd)
}

var compactLogsDoneCtx context.Context

func compactLogsFor(
	ctx context.Context,
	ll *slog.Logger,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	estBytesPerRecord float64,
) (WorkResult, error) {
	profile, err := sp.Get(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}
	if profile.Role == "" {
		if !profile.Hosted {
			ll.Error("No role on non-hosted profile")
			return WorkResultTryAgainLater, err
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	ll.Info("Processing log compression item", slog.Any("workItem", inf.AsMap()))
	return logCompactItemDo(ctx, ll, mdb, inf, profile, s3client, estBytesPerRecord)
}

func logCompactItemDo(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	sp storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	estBytesPerRecord float64,
) (WorkResult, error) {
	st, et, ok := RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, errors.New("error getting range bounds")
	}
	stdi := timeToDateint(st.Time)
	etdi := timeToDateint(et.Time.Add(-time.Millisecond)) // end dateint is inclusive, so subtract 1ms
	if stdi != etdi {
		ll.Error("Range bounds are not the same dateint",
			slog.Int("startDateint", int(stdi)),
			slog.Time("st", st.Time),
			slog.Int("endDateint", int(etdi)),
			slog.Time("et", et.Time),
		)
		return WorkResultTryAgainLater, errors.New("range bounds are not the same dateint")
	}

	segments, err := mdb.GetLogSegmentsForCompaction(ctx, lrdb.GetLogSegmentsForCompactionParams{
		OrganizationID: sp.OrganizationID,
		Dateint:        stdi,
		InstanceNum:    sp.InstanceNum,
	})
	if err != nil {
		ll.Error("Error getting log segments for compaction", slog.String("error", err.Error()))
		return WorkResultTryAgainLater, err
	}
	ll.Info("Got log segments for compaction",
		slog.Int("segmentCount", len(segments)))

	// Remove any segments that are within the last 5 minutes.  The returned list
	// should be sorted by timestamp, so use the StartTs value here.
	ignoredSegments := 0
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].StartTs > time.Now().UTC().Add(-5*time.Minute).UnixMilli() {
			segments = segments[:i]
			ignoredSegments++
			continue
		}
	}

	if len(segments) == 0 {
		ll.Info("No segments to compact", slog.Int("ignoredSegments", ignoredSegments))
		return WorkResultSuccess, nil
	}

	packed, err := logcrunch.PackSegments(segments, targetFileSize, estBytesPerRecord)
	if err != nil {
		ll.Error("Error packing segments", slog.String("error", err.Error()))
		return WorkResultTryAgainLater, err
	}

	lastGroupSmall := false
	if len(packed) > 0 {
		// if the last packed segment is smaller than our target size, drop it.
		bytecount := int64(0)
		lastGroup := packed[len(packed)-1]
		for _, segment := range lastGroup {
			bytecount += segment.FileSize
		}
		if bytecount < targetFileSize {
			packed = packed[:len(packed)-1]
			lastGroupSmall = true
		}
	}

	if len(packed) == 0 {
		ll.Info("No segments to compact", slog.Int("ignoredSegments", ignoredSegments))
		return WorkResultSuccess, nil
	}

	ll.Info("counts", slog.Int("currentSegments", len(segments)), slog.Int("packGroups", len(packed)), slog.Int("ignoredSegments", ignoredSegments), slog.Bool("lastGroupSmall", lastGroupSmall))

	for i, group := range packed {
		ll := ll.With(slog.Int("groupIndex", i))
		err = packSegment(ctx, ll, s3client, mdb, group, sp, stdi)
		if err != nil {
			break
		}
		select {
		case <-compactLogsDoneCtx.Done():
			return WorkResultTryAgainLater, errors.New("Asked to shut down, will retry work")
		default:
		}
	}

	if err != nil {
		return WorkResultTryAgainLater, err
	}
	ll.Info("Successfully packed segments", slog.Int("groupCount", len(packed)))
	return WorkResultSuccess, nil
}

// timeToDateint computes the dateint for the current time.  This is YYYYMMDD as an int32.
func timeToDateint(t time.Time) int32 {
	return int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
}

var dropFieldNames = []string{
	"minute",
	"hour",
	"day",
	"month",
	"year",
}

func packSegment(ctx context.Context, ll *slog.Logger, s3Client *awsclient.S3Client, mdb lrdb.StoreFull, group []lrdb.GetLogSegmentsForCompactionRow, sp storageprofile.StorageProfile, dateint int32) error {
	groupSize := int64(0)
	recordCount := int64(0)
	for _, segment := range group {
		groupSize += segment.FileSize
		recordCount += segment.RecordCount
	}

	if len(group) < 2 {
		return nil
	}

	tmpdir, err := os.MkdirTemp("", "lakerunner-compact-logs")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Error removing temporary directory", slog.String("error", err.Error()))
		}
	}()

	ll.Info("Packing segment group",
		slog.Int("groupSize", int(groupSize)),
		slog.Int("segmentCount", len(group)),
		slog.Int("recordCount", int(recordCount)))

	// downlaod each segment from S3.
	handles := make([]*filecrunch.FileHandle, 0, len(group))
	defer func() {
		for _, handle := range handles {
			if err := handle.Close(); err != nil {
				ll.Error("Error closing file handle", slog.String("error", err.Error()))
			}
			if err := os.Remove(handle.File.Name()); err != nil {
				ll.Error("Error removing temp file", slog.String("error", err.Error()))
			}
		}
	}()

	objectIDs := make([]string, 0, len(group))
	for _, segment := range group {
		objectID := helpers.MakeDBObjectID(sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(segment.StartTs), segment.SegmentID, "logs")
		tmpname, _, err := s3helper.DownloadS3Object(ctx, tmpdir, s3Client, sp.Bucket, objectID)
		if err != nil {
			if s3helper.S3ErrorIs404(err) {
				objectID = helpers.MakeDBObjectIDbad(sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(segment.StartTs), segment.SegmentID, "logs")
				tmpname, _, err = s3helper.DownloadS3Object(ctx, tmpdir, s3Client, sp.Bucket, objectID)
				if err != nil {
					if s3helper.S3ErrorIs404(err) {
						ll.Info("S3 object not found with BAD name either, skipping", slog.String("objectID", objectID))
						continue
					}
					ll.Error("Error downloading S3 object with BAD name", slog.String("objectID", objectID), slog.String("error", err.Error()))
					return err
				}
			}
			if err != nil {
				ll.Error("Error downloading S3 object", slog.String("objectID", objectID), slog.String("error", err.Error()))
				return err
			}
		}
		objectIDs = append(objectIDs, objectID)
		fh, err := filecrunch.LoadSchemaForFile(tmpname)
		if err != nil {
			ll.Error("Error loading schema for file", slog.String("error", err.Error()))
			return err
		}
		fh.Nodes["_cardinalhq.timestamp"] = filecrunch.NodeTypeMap["INT64"]
		for _, fieldName := range dropFieldNames {
			delete(fh.Nodes, fieldName)
		}
		handles = append(handles, fh)
		ll.Debug("Downloaded file", slog.String("file", tmpname), slog.String("objectID", objectID))
	}

	nodes := map[string]parquet.Node{}
	for _, handle := range handles {
		if err := filecrunch.MergeNodes(handle, nodes); err != nil {
			ll.Error("Error merging nodes", slog.String("error", err.Error()))
			return err
		}
	}

	w, err := buffet.NewWriter("logcompact", tmpdir, nodes, 0, 0)
	if err != nil {
		ll.Error("Error creating buffet writer", slog.String("error", err.Error()))
		return err
	}
	defer func() {
		_, _ = w.Close()
	}()

	for i, handle := range handles {
		group := group[i]
		reader := parquet.NewReader(handle.File, handle.Schema)
		defer reader.Close()
		rcount := 0
		for {
			rec := map[string]any{}
			if err := reader.Read(&rec); err != nil {
				if err == io.EOF {
					break
				}
				ll.Error("Error reading parquet", slog.String("error", err.Error()))
				return err
			}
			for _, fieldName := range dropFieldNames {
				delete(rec, fieldName)
			}
			tsrec, ok := rec["_cardinalhq.timestamp"]
			if !ok {
				return errors.New("missing _cardinalhq.timestamp field in record")
			}
			switch ts := tsrec.(type) {
			case int64:
				// ok
			case float64:
				// convert float64 to int64
				tsrec = int64(ts)
			default:
				return errors.New("unexpected type for _cardinalhq.timestamp field: " + tsrec.(string))
			}
			rec["_cardinalhq.timestamp"] = tsrec
			if err := w.Write(rec); err != nil {
				ll.Error("Error writing parquet", slog.String("error", err.Error()))
				return err
			}
			rcount++
		}
		ll.Debug("Wrote records from", slog.String("file", handle.File.Name()), slog.Int("recordCount", rcount), slog.Int("expectedCount", int(group.RecordCount)))
	}

	writeResults, err := w.Close()
	if err != nil {
		ll.Error("Error closing buffet writer", slog.String("error", err.Error()))
		return err
	}
	if len(writeResults) == 0 {
		ll.Info("No records written, skipping upload")
		return nil
	}
	writeResult := writeResults[0]

	if writeResult.RecordCount != recordCount {
		ll.Error("Record count mismatch", slog.Int64("expected", recordCount), slog.Int64("actual", writeResult.RecordCount))
		return err
	}

	s, err := os.Stat(writeResult.FileName)
	if err != nil {
		slog.Error("Failed to stat file", slog.Any("error", err))
		return err
	}

	firstTS := firstFromGroup(group)
	lastTS := lastFromGroup(group)
	ingest_dateint := ingestDateintFromGroup(group)

	// upload the new file to S3
	newsegmentID := s3helper.GenerateID()
	newObjectID := helpers.MakeDBObjectID(sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(firstTS), newsegmentID, "logs")

	ll.Info("Uploading new file to S3",
		slog.Int64("size", s.Size()),
		slog.String("objectID", newObjectID),
		slog.Int64("segmentID", newsegmentID),
		slog.Int64("firstTS", firstTS),
		slog.Int64("lastTS", lastTS),
		slog.Int64("recordCount", writeResult.RecordCount),
		slog.Int64("fileSize", s.Size()))

	if err := s3helper.UploadS3Object(ctx, s3Client, sp.Bucket, newObjectID, writeResult.FileName); err != nil {
		ll.Error("Error uploading S3 object", slog.String("error", err.Error()))
		return err
	}

	ll.Info("Successfully uploaded new file to S3")

	// update the database
	if err := mdb.CompactLogSegments(ctx, lrdb.CompactLogSegmentsParams{
		OrganizationID: sp.OrganizationID,
		Dateint:        dateint,
		IngestDateint:  ingest_dateint,
		InstanceNum:    sp.InstanceNum,
		NewStartTs:     firstTS,
		NewEndTs:       lastTS,
		NewSegmentID:   newsegmentID,
		NewFileSize:    s.Size(),
		NewRecordCount: writeResult.RecordCount,
		OldSegmentIds:  segmentIDsFrom(group),
	}); err != nil {
		ll.Error("Error updating database", slog.String("error", err.Error()))
		return err
	}

	ll.Info("Successfully updated database")

	for _, objectID := range objectIDs {
		if err := s3helper.ScheduleS3Delete(ctx, mdb, sp.OrganizationID, sp.InstanceNum, sp.Bucket, objectID); err != nil {
			ll.Error("scheduleS3Delete", slog.String("error", err.Error()))
			continue
		}
	}

	ll.Info("Scheduled old segments for deletion", slog.Int("count", len(objectIDs)))

	return nil
}

func firstFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int64 {
	if len(group) == 0 {
		return 0
	}
	first := group[0].StartTs
	for _, segment := range group {
		first = min(first, segment.StartTs)
	}
	return first
}

func lastFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int64 {
	last := int64(0)
	for _, segment := range group {
		last = max(last, segment.EndTs)
	}
	return last + 1
}

func segmentIDsFrom(segments []lrdb.GetLogSegmentsForCompactionRow) []int64 {
	ids := make([]int64, len(segments))
	for i, segment := range segments {
		ids[i] = segment.SegmentID
	}
	return ids
}

func ingestDateintFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int32 {
	if len(group) == 0 {
		return 0
	}
	first := int32(0)
	for _, segment := range group {
		first = max(first, segment.IngestDateint)
	}
	return first
}
