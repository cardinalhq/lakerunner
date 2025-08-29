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
	"os"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// chooseObjectID returns the preferred object key (good or bad) that exists, or "" if neither.
func chooseObjectID(ctx context.Context, fetcher ObjectFetcher, bucket, good, bad, tmpdir string) (string, string, int64, error) {
	if tmp, sz, nf, err := fetcher.Download(ctx, bucket, good, tmpdir); err != nil {
		return "", "", 0, err
	} else if !nf {
		return good, tmp, sz, nil
	}
	if tmp, sz, nf, err := fetcher.Download(ctx, bucket, bad, tmpdir); err != nil {
		return "", "", 0, err
	} else if !nf {
		return bad, tmp, sz, nil
	}
	return "", "", 0, nil
}

type openedSegment struct {
	Seg      lrdb.GetLogSegmentsForCompactionRow
	Handle   *filecrunch.FileHandle
	ObjectID string
}

// downloadAndOpen segments; returns only those that exist and opened successfully.
func downloadAndOpen(
	ctx context.Context,
	sp storageprofile.StorageProfile,
	dateint int32,
	group []lrdb.GetLogSegmentsForCompactionRow,
	tmpdir string,
	bucket string,
	fetcher ObjectFetcher,
	open FileOpener,
) ([]openedSegment, error) {
	out := make([]openedSegment, 0, len(group))
	for _, seg := range group {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		good := helpers.MakeDBObjectID(sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(seg.StartTs), seg.SegmentID, "logs")
		bad := helpers.MakeDBObjectIDbad(sp.OrganizationID, dateint, s3helper.HourFromMillis(seg.StartTs), seg.SegmentID, "logs")

		objectID, tmpfile, _, err := chooseObjectID(ctx, fetcher, bucket, good, bad, tmpdir)
		if err != nil {
			return nil, fmt.Errorf("download: %w", err)
		}
		if objectID == "" {
			continue
		}

		fh, err := open.LoadSchemaForFile(tmpfile)
		if err != nil {
			return nil, fmt.Errorf("open schema: %w", err)
		}
		// Normalize timestamp
		fh.Nodes["_cardinalhq.timestamp"] = filecrunch.NodeTypeMap["INT64"]
		for _, fn := range dropFieldNames {
			delete(fh.Nodes, fn)
		}

		out = append(out, openedSegment{Seg: seg, Handle: fh, ObjectID: objectID})
	}
	return out, nil
}

// mergeNodes merges schemas from opened handles.
func mergeNodes(handles []*filecrunch.FileHandle) (map[string]parquet.Node, error) {
	cap := 0
	for _, h := range handles {
		if h != nil {
			cap += len(h.Nodes)
		}
	}
	nodes := make(map[string]parquet.Node, cap)
	for _, h := range handles {
		if h == nil || h.Nodes == nil {
			continue
		}
		if err := filecrunch.MergeNodes(h, nodes); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func computeDropSet(nodes map[string]parquet.Node) map[string]struct{} {
	drop := map[string]struct{}{}
	for _, name := range dropFieldNames {
		if _, ok := nodes[name]; ok {
			drop[name] = struct{}{}
		}
	}
	return drop
}

// normalizeRecord drops fields and coerces _cardinalhq.timestamp to int64.
func normalizeRecord(rec map[string]any, drop map[string]struct{}) (map[string]any, error) {
	if len(drop) != 0 {
		for k := range drop {
			delete(rec, k)
		}
	}
	v, ok := rec["_cardinalhq.timestamp"]
	if !ok {
		return nil, errors.New("missing _cardinalhq.timestamp")
	}
	switch t := v.(type) {
	case int64:
		// ok
	case int32:
		rec["_cardinalhq.timestamp"] = int64(t)
	case float64:
		rec["_cardinalhq.timestamp"] = int64(t)
	default:
		return nil, fmt.Errorf("unexpected _cardinalhq.timestamp type %T", v)
	}
	return rec, nil
}

// copyAll writes all rows from each handle to writer; returns total written rows.
func copyAll(
	ctx context.Context,
	open FileOpener,
	writer Writer,
	handles []*filecrunch.FileHandle,
) (int64, error) {
	var total int64
	const batchSize = 4096
	batch := make([]map[string]any, batchSize)
	for i := range batch {
		batch[i] = mapPool.Get().(map[string]any)
	}
	defer func() {
		for i := range batch {
			mapPool.Put(batch[i])
		}
	}()

	for _, h := range handles {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		dropSet := computeDropSet(h.Nodes)

		r, err := open.NewGenericMapReader(h.File, h.Schema)
		if err != nil {
			return total, err
		}

		for {
			if err := ctx.Err(); err != nil {
				_ = r.Close()
				return total, err
			}
			n, err := r.Read(batch)
			if n > 0 {
				// Normalize and write each record in the batch.
				for i := range n {
					rec, nerr := normalizeRecord(batch[i], dropSet)
					if nerr != nil {
						_ = r.Close()
						return total, nerr
					}
					if werr := writer.Write(rec); werr != nil {
						_ = r.Close()
						return total, werr
					}
					for k := range batch[i] {
						delete(batch[i], k)
					}
				}
				total += int64(n)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = r.Close()
				return total, err
			}
		}
		// non-fatal close
		_ = r.Close()
	}
	return total, nil
}

// basic stats from used segments
type usedStats struct {
	CountRecords int64
	SizeBytes    int64
	FirstTS      int64
	LastTS       int64
	IngestDate   int32
}

func statsFor(used []lrdb.GetLogSegmentsForCompactionRow) usedStats {
	var s usedStats
	if len(used) == 0 {
		return s
	}
	s.FirstTS = used[0].StartTs
	for _, seg := range used {
		s.CountRecords += seg.RecordCount
		s.SizeBytes += seg.FileSize
		if seg.StartTs < s.FirstTS {
			s.FirstTS = seg.StartTs
		}
		if seg.EndTs > s.LastTS {
			s.LastTS = seg.EndTs
		}
		if seg.IngestDateint > s.IngestDate {
			s.IngestDate = seg.IngestDateint
		}
	}
	return s
}

// executeCriticalSection executes S3 upload followed by database update with a timeout.
// This ensures the critical section completes atomically or fails completely.
func executeCriticalSection(
	ctx context.Context,
	ll *slog.Logger,
	s3Client *awsclient.S3Client,
	mdb lrdb.StoreFull,
	bucket, objectID, fileName string,
	dbParams lrdb.CompactLogSegmentsParams,
	timeout time.Duration,
) error {
	// Create a context with timeout for the critical section
	criticalCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ll.Info("Entering critical section - S3 upload + DB update must complete atomically",
		slog.Duration("timeout", timeout),
		slog.String("objectID", objectID))

	// Step 1: Upload to S3
	ll.Debug("Critical section: uploading to S3",
		slog.String("objectID", objectID))

	if err := s3helper.UploadS3Object(criticalCtx, s3Client, bucket, objectID, fileName); err != nil {
		ll.Error("Critical section failed during S3 upload - no changes made",
			slog.Any("error", err),
			slog.String("objectID", objectID))
		return err
	}

	ll.Debug("Critical section: S3 upload successful, updating database")

	// Step 2: Update database
	if err := mdb.CompactLogSegments(criticalCtx, dbParams); err != nil {
		ll.Error("Critical section: database update failed after S3 upload",
			slog.Any("error", err),
			slog.String("objectID", objectID))

		// Best effort cleanup - try to delete the uploaded file
		ll.Debug("Critical section: attempting to cleanup orphaned S3 object")
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		if cleanupErr := s3helper.DeleteS3Object(cleanupCtx, s3Client, bucket, objectID); cleanupErr != nil {
			ll.Error("Critical section: failed to cleanup orphaned S3 object - manual cleanup required",
				slog.Any("error", cleanupErr),
				slog.String("objectID", objectID),
				slog.String("bucket", bucket))
		} else {
			ll.Info("Critical section: successfully cleaned up orphaned S3 object")
		}
		return err
	}

	ll.Info("Critical section completed successfully - S3 upload and database update committed",
		slog.String("objectID", objectID))

	return nil
}

func packSegment(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	s3Client *awsclient.S3Client,
	mdb lrdb.StoreFull,
	group []lrdb.GetLogSegmentsForCompactionRow,
	sp storageprofile.StorageProfile,
	dateint int32,
	instanceNum int16,
) error {
	// ll already has operationID from caller
	if len(group) < 2 {
		ll.Info("Atomic operation skipped - insufficient segments for compaction",
			slog.Int("segmentCount", len(group)),
			slog.String("reason", "need at least 2 segments"))
		return nil
	}

	ll.Debug("Starting atomic compaction - downloading and opening segments",
		slog.Int("segmentCount", len(group)))

	fetcher := objectFetcherAdapter{s3Client: s3Client}
	open := fileOpenerAdapter{}
	wf := writerFactoryAdapter{}

	opened, err := downloadAndOpen(ctx, sp, dateint, group, tmpdir, sp.Bucket, fetcher, open)
	if err != nil {
		ll.Error("Atomic operation failed during segment download",
			slog.Any("error", err),
			slog.Int("segmentCount", len(group)))
		return err
	}

	if len(opened) < 2 {
		ll.Info("Atomic operation skipped - insufficient usable segments after download",
			slog.Int("requestedSegmentCount", len(group)),
			slog.Int("usableSegmentCount", len(opened)),
			slog.String("reason", "some segments may be missing from S3"))
		return nil
	}

	ll.Debug("Successfully downloaded segments, beginning compaction",
		slog.Int("segmentCount", len(opened)))

	handles := make([]*filecrunch.FileHandle, 0, len(opened))
	usedSegs := make([]lrdb.GetLogSegmentsForCompactionRow, 0, len(opened))
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

	ll.Debug("Merging schemas from downloaded segments")
	nodes, err := mergeNodes(handles)
	if err != nil {
		ll.Error("Atomic operation failed during schema merge",
			slog.Any("error", err))
		return err
	}

	ll.Debug("Creating writer for compacted output")
	w, err := wf.NewWriter("logcompact", tmpdir, nodes, 0)
	if err != nil {
		ll.Error("Atomic operation failed during writer creation",
			slog.Any("error", err))
		return err
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			_, _ = w.Close()
		}
	}()

	ll.Debug("Copying and compacting data from all segments")
	_, err = copyAll(ctx, open, w, handles)
	if err != nil {
		ll.Error("Atomic operation failed during data copying",
			slog.Any("error", err))
		return err
	}

	writeResults, err := w.Close()
	writerClosed = true
	if err != nil {
		ll.Error("Atomic operation failed during file finalization",
			slog.Any("error", err))
		return err
	}
	if len(writeResults) == 0 {
		ll.Info("Atomic operation skipped - no records to write",
			slog.String("reason", "all input segments were empty"))
		return nil
	}
	writeResult := writeResults[0]

	ll.Debug("Validating compacted file before upload")
	stats := statsFor(usedSegs)
	if writeResult.RecordCount != stats.CountRecords {
		ll.Error("Atomic operation failed during validation",
			slog.String("error", "record count mismatch"),
			slog.Int64("expected", stats.CountRecords),
			slog.Int64("actual", writeResult.RecordCount))
		return fmt.Errorf("record count mismatch: expected=%d actual=%d", stats.CountRecords, writeResult.RecordCount)
	}

	fi, err := os.Stat(writeResult.FileName)
	if err != nil {
		ll.Error("Atomic operation failed during file stat",
			slog.Any("error", err))
		return err
	}

	newSegmentID := s3helper.GenerateID()
	newObjectID := helpers.MakeDBObjectID(
		sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(stats.FirstTS), newSegmentID, "logs",
	)

	// Final interruption check before entering critical section
	if err := ctx.Err(); err != nil {
		ll.Info("Context cancelled before critical section - safe to abort",
			slog.String("objectID", newObjectID),
			slog.Any("error", err))
		return err
	}

	ll.Info("Preparing for critical section - S3 upload + DB update",
		slog.Int64("fileSize", fi.Size()),
		slog.String("objectID", newObjectID),
		slog.Int64("segmentID", newSegmentID),
		slog.Int64("recordCount", writeResult.RecordCount),
		slog.Int("segmentCount", len(usedSegs)))

	// Prepare database parameters for critical section
	dbParams := lrdb.CompactLogSegmentsParams{
		OrganizationID: sp.OrganizationID,
		Dateint:        dateint,
		InstanceNum:    instanceNum,
		SlotID:         usedSegs[0].SlotID, // Use slot ID from the first segment (all should be the same)
		IngestDateint:  stats.IngestDate,
		NewStartTs:     stats.FirstTS,
		NewEndTs:       stats.LastTS, // already half-open
		NewSegmentID:   newSegmentID,
		NewFileSize:    fi.Size(),
		NewRecordCount: writeResult.RecordCount,
		OldSegmentIds:  segmentIDsFrom(usedSegs),
		CreatedBy:      lrdb.CreatedByCompact,
	}

	// Execute critical section with 30 second timeout
	if err := executeCriticalSection(ctx, ll, s3Client, mdb, sp.Bucket, newObjectID, writeResult.FileName, dbParams, 30*time.Second); err != nil {
		return err
	}

	ll.Info("Atomic operation committed successfully - database updated, segments swapped",
		slog.Int("segmentCount", len(usedSegs)),
		slog.Int64("segmentID", newSegmentID),
		slog.Int64("recordCount", writeResult.RecordCount),
		slog.Int64("fileSize", fi.Size()),
		slog.String("objectID", newObjectID))

	// Schedule old files for deletion (best effort)
	ll.Debug("Scheduling old segment files for background deletion",
		slog.Int("fileCount", len(objectIDs)))

	scheduledCount := 0
	for _, oid := range objectIDs {
		if err := s3helper.ScheduleS3Delete(ctx, mdb, sp.OrganizationID, sp.InstanceNum, sp.Bucket, oid); err != nil {
			ll.Warn("Failed to schedule deletion for old segment file",
				slog.Any("error", err),
				slog.String("objectID", oid))
		} else {
			scheduledCount++
		}
	}

	ll.Info("Atomic operation completed successfully",
		slog.Int("scheduledDeletions", scheduledCount),
		slog.Int("totalFiles", len(objectIDs)))

	return nil
}
