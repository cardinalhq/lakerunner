// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the GNU Affero General Public License, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR ANY PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// openedTraceSegment represents a trace segment that has been downloaded and opened
type openedTraceSegment struct {
	Seg      lrdb.GetTraceSegmentsForCompactionRow
	Handle   *filecrunch.FileHandle
	ObjectID string
}

// downloadAndOpenTraceSegments downloads and opens trace segments; returns only those that exist and opened successfully.
func downloadAndOpenTraceSegments(
	ctx context.Context,
	sp storageprofile.StorageProfile,
	dateint int32,
	slotID int32,
	group []lrdb.GetTraceSegmentsForCompactionRow,
	tmpdir string,
	bucket string,
	fetcher ObjectFetcher,
	open FileOpener,
) ([]openedTraceSegment, error) {
	out := make([]openedTraceSegment, 0, len(group))
	for _, seg := range group {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Create S3 object ID for traces using slot-based path
		// Format: db/<org-id>/<collector-id>/<dateint>/traces/<slot_number>/<segment_id>.parquet
		objectID := fmt.Sprintf("db/%s/%s/%d/traces/%d/%d.parquet",
			sp.OrganizationID, sp.CollectorName, dateint, slotID, seg.SegmentID)

		// Download the trace segment
		tmpfile, _, is404, err := fetcher.Download(ctx, bucket, objectID, tmpdir)
		if err != nil {
			return nil, fmt.Errorf("download: %w", err)
		}
		if is404 {
			// Skip segments that don't exist in S3
			continue
		}

		fh, err := open.LoadSchemaForFile(tmpfile)
		if err != nil {
			return nil, fmt.Errorf("open schema: %w", err)
		}

		// For traces, we don't need to normalize timestamps or drop fields like logs do
		// Traces have their own schema structure

		out = append(out, openedTraceSegment{Seg: seg, Handle: fh, ObjectID: objectID})
	}
	return out, nil
}

// statsForTraceSegments calculates statistics for a group of trace segments
func statsForTraceSegments(segments []lrdb.GetTraceSegmentsForCompactionRow) struct {
	CountRecords int64
	FirstTS      int64
	LastTS       int64
	IngestDate   int32
} {
	if len(segments) == 0 {
		return struct {
			CountRecords int64
			FirstTS      int64
			LastTS       int64
			IngestDate   int32
		}{}
	}

	firstTS := segments[0].StartTs
	lastTS := segments[0].EndTs
	ingestDate := segments[0].IngestDateint
	var totalRecords int64

	for _, seg := range segments {
		if seg.StartTs < firstTS {
			firstTS = seg.StartTs
		}
		if seg.EndTs > lastTS {
			lastTS = seg.EndTs
		}
		if seg.IngestDateint > ingestDate {
			ingestDate = seg.IngestDateint
		}
		totalRecords += seg.RecordCount
	}

	return struct {
		CountRecords int64
		FirstTS      int64
		LastTS       int64
		IngestDate   int32
	}{
		CountRecords: totalRecords,
		FirstTS:      firstTS,
		LastTS:       lastTS,
		IngestDate:   ingestDate,
	}
}

// segmentIDsFromTraceSegments extracts segment IDs from trace segments
func segmentIDsFromTraceSegments(segments []lrdb.GetTraceSegmentsForCompactionRow) []int64 {
	ids := make([]int64, len(segments))
	for i, segment := range segments {
		ids[i] = segment.SegmentID
	}
	return ids
}

// packTraceSegment packs a group of trace segments into a single consolidated file
func packTraceSegment(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	s3Client *awsclient.S3Client,
	mdb lrdb.StoreFull,
	group []lrdb.GetTraceSegmentsForCompactionRow,
	sp storageprofile.StorageProfile,
	dateint int32,
	slotID int32,
) error {
	ll.Info("DEBUG: packTraceSegment started",
		slog.Int("groupSize", len(group)),
		slog.String("tmpdir", tmpdir),
		slog.Int("slotID", int(slotID)),
		slog.Int("dateint", int(dateint)))

	if len(group) < 2 {
		ll.Info("DEBUG: Group too small, skipping", slog.Int("groupSize", len(group)))
		return nil
	}

	ll.Info("DEBUG: Creating object fetcher and file opener")
	fetcher := objectFetcherAdapter{s3Client: s3Client}
	open := fileOpenerAdapter{}
	wf := writerFactoryAdapter{}

	ll.Info("DEBUG: About to download and open trace segments",
		slog.Int("segmentCount", len(group)),
		slog.String("bucket", sp.Bucket))

	opened, err := downloadAndOpenTraceSegments(ctx, sp, dateint, slotID, group, tmpdir, sp.Bucket, fetcher, open)
	if err != nil {
		ll.Error("DEBUG: Failed to download and open trace segments", slog.String("error", err.Error()))
		return err
	}

	ll.Info("DEBUG: Downloaded and opened segments",
		slog.Int("requestedCount", len(group)),
		slog.Int("openedCount", len(opened)))

	if len(opened) < 2 {
		ll.Info("DEBUG: Group has fewer than 2 usable segments; skipping",
			slog.Int("requestedGroupSize", len(group)),
			slog.Int("usableSegments", len(opened)))
		return nil
	}

	handles := make([]*filecrunch.FileHandle, 0, len(opened))
	usedSegs := make([]lrdb.GetTraceSegmentsForCompactionRow, 0, len(opened))
	objectIDs := make([]string, 0, len(opened))
	for _, o := range opened {
		handles = append(handles, o.Handle)
		usedSegs = append(usedSegs, o.Seg)
		objectIDs = append(objectIDs, o.ObjectID)
	}

	defer func() {
		for _, h := range handles {
			_ = h.Close()
			_ = os.Remove(h.File.Name())
		}
	}()

	ll.Info("DEBUG: About to merge nodes from opened files", slog.Int("fileCount", len(handles)))
	nodes, err := mergeNodes(handles)
	if err != nil {
		ll.Error("DEBUG: Error merging nodes", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: Successfully merged nodes", slog.Int("nodeCount", len(nodes)))

	ll.Info("DEBUG: Creating new writer", slog.String("tmpdir", tmpdir))
	w, err := wf.NewWriter("tracecompact", tmpdir, nodes, 0)
	if err != nil {
		ll.Error("DEBUG: Failed to create writer", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: Successfully created writer")

	writerClosed := false
	defer func() {
		if !writerClosed {
			ll.Info("DEBUG: Closing writer in defer")
			_, _ = w.Close()
		}
	}()

	ll.Info("DEBUG: About to copy data from handles to writer", slog.Int("handleCount", len(handles)))
	_, err = copyAll(ctx, open, w, handles)
	if err != nil {
		ll.Error("DEBUG: Failed to copy data", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: Successfully copied data to writer")

	ll.Info("DEBUG: About to close writer and get results")
	writeResults, err := w.Close()
	writerClosed = true
	if err != nil {
		ll.Error("DEBUG: Failed to close writer", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: Writer closed successfully", slog.Int("resultCount", len(writeResults)))

	if len(writeResults) == 0 {
		ll.Info("DEBUG: No records written, skipping upload")
		return nil
	}
	writeResult := writeResults[0]
	ll.Info("DEBUG: Got write result",
		slog.String("fileName", writeResult.FileName),
		slog.Int64("recordCount", writeResult.RecordCount))

	stats := statsForTraceSegments(usedSegs)
	ll.Info("DEBUG: Calculated stats for trace segments",
		slog.Int64("expectedRecords", stats.CountRecords),
		slog.Int64("actualRecords", writeResult.RecordCount),
		slog.Int64("firstTS", stats.FirstTS),
		slog.Int64("lastTS", stats.LastTS))

	if writeResult.RecordCount != stats.CountRecords {
		ll.Error("DEBUG: Record count mismatch",
			slog.Int64("expected", stats.CountRecords),
			slog.Int64("actual", writeResult.RecordCount))
		return fmt.Errorf("record count mismatch: expected=%d actual=%d", stats.CountRecords, writeResult.RecordCount)
	}

	ll.Info("DEBUG: About to get file stats", slog.String("fileName", writeResult.FileName))
	fi, err := os.Stat(writeResult.FileName)
	if err != nil {
		ll.Error("DEBUG: Failed to get file stats", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: Got file stats",
		slog.Int64("fileSize", fi.Size()),
		slog.String("fileName", fi.Name()))

	newSegmentID := s3helper.GenerateID()
	ll.Info("DEBUG: Generated new segment ID", slog.Int64("segmentID", newSegmentID))

	// Create S3 object ID for the new consolidated trace file
	newObjectID := fmt.Sprintf("db/%s/%s/%d/traces/%d/%d.parquet",
		sp.OrganizationID, sp.CollectorName, dateint, slotID, newSegmentID)

	ll.Info("DEBUG: Created S3 object ID", slog.String("objectID", newObjectID))

	ll.Info("DEBUG: About to upload to S3",
		slog.String("bucket", sp.Bucket),
		slog.String("objectID", newObjectID),
		slog.String("localFile", writeResult.FileName),
		slog.Int64("fileSize", fi.Size()))

	if err := s3helper.UploadS3Object(ctx, s3Client, sp.Bucket, newObjectID, writeResult.FileName); err != nil {
		ll.Error("DEBUG: S3 upload failed", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: S3 upload completed successfully")

	ll.Info("DEBUG: About to update database with new segment info")
	// Update the database to replace old segments with the new consolidated one
	if err := mdb.CompactTraceSegments(ctx, lrdb.CompactTraceSegmentsParams{
		OrganizationID: sp.OrganizationID,
		Dateint:        dateint,
		IngestDateint:  stats.IngestDate,
		NewSegmentID:   newSegmentID,
		InstanceNum:    sp.InstanceNum,
		SlotID:         slotID,
		NewRecordCount: writeResult.RecordCount,
		NewFileSize:    fi.Size(),
		NewStartTs:     stats.FirstTS,
		NewEndTs:       stats.LastTS,
		CreatedBy:      lrdb.CreatedByCompact,
		OldSegmentIds:  segmentIDsFromTraceSegments(usedSegs),
	}); err != nil {
		ll.Error("DEBUG: Database update failed", slog.String("error", err.Error()))
		return err
	}
	ll.Info("DEBUG: Database updated successfully")

	// Schedule old segments for deletion
	ll.Info("DEBUG: About to schedule old segments for deletion", slog.Int("segmentCount", len(objectIDs)))
	for _, oid := range objectIDs {
		if err := s3helper.ScheduleS3Delete(ctx, mdb, sp.OrganizationID, sp.InstanceNum, sp.Bucket, oid); err != nil {
			ll.Error("DEBUG: Failed to schedule segment deletion",
				slog.String("objectID", oid),
				slog.String("error", err.Error()))
		}
	}
	ll.Info("DEBUG: Successfully scheduled old trace segments for deletion", slog.Int("count", len(objectIDs)))

	ll.Info("DEBUG: packTraceSegment completed successfully",
		slog.Int64("newSegmentID", newSegmentID),
		slog.Int64("fileSize", fi.Size()),
		slog.Int64("recordCount", writeResult.RecordCount))
	return nil
}
