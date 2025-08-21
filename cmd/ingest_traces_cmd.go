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
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/ingesttraces"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-traces",
		Short: "Ingest traces from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-traces"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "traces"),
				attribute.String("action", "ingest"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewIngestLoopContext(doneCtx, "traces", servicename)
			if err != nil {
				return fmt.Errorf("failed to create ingest loop context: %w", err)
			}

			for {
				select {
				case <-doneCtx.Done():
					slog.Info("Ingest traces command done")
					return nil
				default:
				}

				err := IngestLoop(loop, traceIngestItem)
				if err != nil {
					slog.Error("Error in ingest loop", slog.Any("error", err))
				}
			}
		},
	}

	rootCmd.AddCommand(cmd)
}

func traceIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	profile, err := sp.GetStorageProfileForBucket(ctx, inf.OrganizationID, inf.Bucket)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}
	if profile.Bucket != inf.Bucket {
		ll.Error("Bucket ID mismatch", slog.String("expected", profile.Bucket), slog.String("actual", inf.Bucket))
		return fmt.Errorf("bucket ID mismatch")
	}
	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	// Download the trace file from S3
	tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, inf.Bucket, inf.ObjectID)
	if err != nil {
		ll.Error("Failed to download S3 object", slog.Any("error", err))
		return err
	}
	if is404 {
		ll.Info("S3 object not found, deleting inqueue work item", slog.String("bucket", inf.Bucket), slog.String("objectID", inf.ObjectID))
		return nil
	}

	ll.Info("Downloaded source trace file")

	// If the file is not in our `otel-raw` prefix, check if we can convert it
	if !strings.HasPrefix(inf.ObjectID, "otel-raw/") {
		// Skip database files (these are processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			// TODO add counter for skipped files in the db prefix
			return nil
		}

		// Check file type and convert if supported
		if traceResults, err := convertTracesFileIfSupported(ll, tmpfilename, tmpdir, inf.Bucket, inf.ObjectID, rpfEstimate, ingest_dateint, inf.OrganizationID.String()); err != nil {
			ll.Error("Failed to convert file", slog.Any("error", err))
			// TODO add counter for failure to convert, probably in each convert function
			return err
		} else if len(traceResults) == 0 {
			ll.Info("Empty source file, skipping", slog.String("objectID", inf.ObjectID))
			return nil
		} else if traceResults != nil {
			ll.Info("Converted file", slog.String("filename", tmpfilename), slog.String("objectID", inf.ObjectID))

			// Process each converted file - upload to S3 and insert into database
			for _, result := range traceResults {
				segmentID := s3helper.GenerateID()

				// Create S3 object ID for traces using the standard helper
				hour := int16(0) // Hour doesn't matter for slot-based traces
				dbObjectID := helpers.MakeDBObjectID(inf.OrganizationID, inf.CollectorName, ingest_dateint, hour, segmentID, "traces")

				// Upload to S3
				if err := s3helper.UploadS3Object(ctx, s3client, inf.Bucket, dbObjectID, result.FileName); err != nil {
					ll.Error("Failed to upload S3 object", slog.Any("error", err))
					return err
				}

				ll.Info("Uploaded trace segment",
					slog.String("bucket", inf.Bucket),
					slog.String("objectID", dbObjectID),
					slog.Int64("segmentID", segmentID),
					slog.Int("slotID", result.SlotID),
					slog.String("dateint", fmt.Sprintf("%d", ingest_dateint)))

				// Clean up local file
				_ = os.Remove(result.FileName)

				// Insert trace segment into database
				err = mdb.InsertTraceSegment(ctx, lrdb.InsertTraceSegmentDirectParams{
					OrganizationID: inf.OrganizationID,
					Dateint:        ingest_dateint,
					IngestDateint:  ingest_dateint,
					SegmentID:      segmentID,
					InstanceNum:    inf.InstanceNum,
					SlotID:         int32(result.SlotID),
					StartTs:        0, // Time doesn't matter for slot-based traces compaction
					EndTs:          1, // Time doesn't matter for slot-based traces compaction
					RecordCount:    result.RecordCount,
					FileSize:       result.FileSize,
					CreatedBy:      lrdb.CreatedByIngest,
					Fingerprints:   []int64{}, // TODO: Extract fingerprints
				})
				if err != nil {
					ll.Error("Failed to insert trace segment", slog.Any("error", err))
					return err
				}

				ll.Info("Inserted trace segment",
					slog.Int64("segmentID", segmentID),
					slog.Int("slotID", result.SlotID),
					slog.Int64("recordCount", result.RecordCount),
					slog.Int64("fileSize", result.FileSize))

				// Queue trace compaction work for this specific slot
				if err := queueTraceCompactionForSlot(ctx, mdb, inf, result.SlotID, ingest_dateint); err != nil {
					ll.Error("Failed to queue trace compaction for slot", slog.Int("slotID", result.SlotID), slog.Any("error", err))
					return err
				}
			}
		}
	}

	// Trace ingestion logic is now handled in the convertTracesFileIfSupported function
	// which processes the converted files, uploads them to S3, and prepares for database insertion
	return nil
}

func convertTracesFileIfSupported(ll *slog.Logger, tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64, ingest_dateint int32, orgID string) ([]ingesttraces.TraceFileResult, error) {
	// TODO add a counter for each type we process, and a counter for unsupported types
	// Include the signal type in the attributes, as well as the converter used, and the extension found.
	switch {
	case strings.HasSuffix(objectID, ".binpb"):
		return ingesttraces.ConvertProtoFile(tmpfilename, tmpdir, bucket, objectID, rpfEstimate, ingest_dateint, orgID)
	default:
		ll.Warn("Unsupported file type, skipping", slog.String("objectID", objectID))
		return nil, nil
	}
}

// queueTraceCompactionForSlot queues a trace compaction job for a specific slot
func queueTraceCompactionForSlot(ctx context.Context, mdb lrdb.StoreFull, inf lrdb.Inqueue, slotID int, dateint int32) error {
	return mdb.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		OrgID:     inf.OrganizationID,
		Signal:    lrdb.SignalEnumTraces,
		Action:    lrdb.ActionEnumCompact,
		Dateint:   dateint,
		Frequency: -1,
		SlotID:    int32(slotID),
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}, // Time doesn't matter for slot-based traces compaction
			Upper:     pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), Valid: true}, // Time doesn't matter for slot-based traces compaction
			Valid:     true,
		},
		RunnableAt: time.Now().UTC().Add(5 * time.Minute),
	})
}
