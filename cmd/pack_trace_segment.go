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
	"github.com/cardinalhq/lakerunner/internal/helpers"
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

		hour := s3helper.HourFromMillis(seg.StartTs)
		objectID := helpers.MakeDBObjectID(sp.OrganizationID, sp.CollectorName, dateint, hour, seg.SegmentID, "traces")

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
	instanceNum int16,
) error {

	if len(group) < 2 {
		return nil
	}

	fetcher := objectFetcherAdapter{s3Client: s3Client}
	open := fileOpenerAdapter{}
	wf := writerFactoryAdapter{}

	opened, err := downloadAndOpenTraceSegments(ctx, sp, dateint, slotID, group, tmpdir, sp.Bucket, fetcher, open)
	if err != nil {
		ll.Error("Failed to download and open trace segments", slog.String("error", err.Error()))
		return err
	}

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

	nodes, err := mergeNodes(handles)
	if err != nil {
		ll.Error("Error merging nodes", slog.String("error", err.Error()))
		return err
	}

	w, err := wf.NewWriter("tracecompact", tmpdir, nodes, 0)
	if err != nil {
		ll.Error("Failed to create writer", slog.String("error", err.Error()))
		return err
	}

	writerClosed := false
	defer func() {
		if !writerClosed {
			_, _ = w.Close()
		}
	}()

	_, err = copyAll(ctx, open, w, handles)
	if err != nil {
		ll.Error("Failed to copy data", slog.String("error", err.Error()))
		return err
	}

	writeResults, err := w.Close()
	writerClosed = true
	if err != nil {
		ll.Error("Failed to close writer", slog.String("error", err.Error()))
		return err
	}

	if len(writeResults) == 0 {
		return nil
	}
	writeResult := writeResults[0]

	stats := statsForTraceSegments(usedSegs)

	if writeResult.RecordCount != stats.CountRecords {
		ll.Error("Record count mismatch",
			slog.Int64("expected", stats.CountRecords),
			slog.Int64("actual", writeResult.RecordCount))
		return fmt.Errorf("record count mismatch: expected=%d actual=%d", stats.CountRecords, writeResult.RecordCount)
	}

	fi, err := os.Stat(writeResult.FileName)
	if err != nil {
		ll.Error("Failed to get file stats", slog.String("error", err.Error()))
		return err
	}

	newSegmentID := s3helper.GenerateID()

	// Create S3 object ID for the new consolidated trace file
	newObjectID := fmt.Sprintf("db/%s/%d/traces/%d.parquet",
		sp.OrganizationID, dateint, newSegmentID)

	if err := s3helper.UploadS3Object(ctx, s3Client, sp.Bucket, newObjectID, writeResult.FileName); err != nil {
		ll.Error("S3 upload failed", slog.String("error", err.Error()))
		return err
	}

	// Update the database to replace old segments with the new consolidated one
	if err := mdb.CompactTraceSegments(ctx, lrdb.CompactTraceSegmentsParams{
		OrganizationID: sp.OrganizationID,
		Dateint:        dateint,
		InstanceNum:    instanceNum,
		IngestDateint:  stats.IngestDate,
		NewSegmentID:   newSegmentID,
		SlotID:         slotID,
		NewRecordCount: writeResult.RecordCount,
		NewFileSize:    fi.Size(),
		NewStartTs:     stats.FirstTS,
		NewEndTs:       stats.LastTS,
		CreatedBy:      lrdb.CreatedByCompact,
		OldSegmentIds:  segmentIDsFromTraceSegments(usedSegs),
	}); err != nil {
		ll.Error("Database update failed", slog.String("error", err.Error()))
		return err
	}

	// Schedule old segments for deletion
	for _, oid := range objectIDs {
		if err := s3helper.ScheduleS3Delete(ctx, mdb, sp.OrganizationID, sp.InstanceNum, sp.Bucket, oid); err != nil {
			ll.Error("Failed to schedule segment deletion",
				slog.String("objectID", oid),
				slog.String("error", err.Error()))
		}
	}

	return nil
}
