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
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-logs",
		Short: "Ingest logs from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "lakerunner-ingest-logs"
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

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

			return IngestLoop(doneCtx, sp, "logs", servicename, logIngestItem)
		},
	}

	rootCmd.AddCommand(cmd)
}

func logIngestItem(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32) error {
	tmpdir, err := os.MkdirTemp("", "lakerunner-ingest-logs-*")
	if err != nil {
		ll.Error("Failed to create temp directory", slog.Any("error", err))
		return err
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temp directory", slog.Any("error", err))
		}
	}()

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

	tmpfilename, _, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, inf.Bucket, inf.ObjectID)
	if err != nil {
		if s3helper.S3ErrorIs404(err) {
			ll.Info("S3 object not found, deleting inqueue work item", slog.String("bucket", inf.Bucket), slog.String("objectID", inf.ObjectID))
			return nil
		}
		ll.Error("Failed to download S3 object", slog.Any("error", err))
		return err
	}
	ll.Info("Downloaded source file")

	fh, err := filecrunch.LoadSchemaForFile(tmpfilename)
	if err != nil {
		ll.Error("Failed to load schema for file", slog.Any("error", err))
		return err
	}
	defer func() {
		_ = fh.Close()
	}()
	splitResults, err := logcrunch.ProcessAndSplit(ll, fh, tmpdir, ingest_dateint)
	if err != nil {
		ll.Error("Failed to fingerprint file", slog.Any("error", err))
		return err
	}

	for key, split := range splitResults {
		segmentID := s3helper.GenerateID()
		dbObjectID := helpers.MakeDBObjectID(inf.OrganizationID, inf.CollectorName, key.DateInt, s3helper.HourFromMillis(split.FirstTS), segmentID, "logs")

		if err := s3helper.UploadS3Object(ctx, s3client, inf.Bucket, dbObjectID, split.FileName); err != nil {
			ll.Error("Failed to upload S3 object", slog.Any("error", err))
			return err
		}
		ll.Info("Uploaded log segment",
			slog.String("bucket", inf.Bucket),
			slog.String("objectID", dbObjectID),
			slog.Int64("segmentID", segmentID),
			slog.Any("key", key),
			slog.Int64("firstTS", split.FirstTS),
			slog.Int64("lastTS", split.LastTS),
			slog.Int64("recordCount", split.RecordCount),
			slog.Int64("fileSize", split.FileSize))
		_ = os.Remove(split.FileName)

		attrs := attribute.NewSet(
			attribute.String("signal", "logs"),
			attribute.String("action", "ingest"),
		)

		fps := split.Fingerprints.ToSlice()
		t0 := time.Now()
		split.LastTS++ // end is exclusive, so we need to increment it by 1ms
		err = mdb.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: inf.OrganizationID,
			Dateint:        key.DateInt,
			IngestDateint:  ingest_dateint,
			SegmentID:      segmentID,
			InstanceNum:    inf.InstanceNum,
			StartTs:        split.FirstTS,
			EndTs:          split.LastTS,
			RecordCount:    split.RecordCount,
			FileSize:       split.FileSize,
			Fingerprints:   fps,
		})
		dbExecDuration.Record(ctx, time.Since(t0).Milliseconds(),
			metric.WithAttributeSet(commonAttributes),
			metric.WithAttributeSet(attrs),
			metric.WithAttributes(
				attribute.Bool("hasError", err != nil),
				attribute.String("queryName", "InsertLogSegment"),
			))
		if err != nil {
			ll.Error("Failed to insert log segments", slog.Any("error", err))
			return err
		}

		ll.Info("Inserted log segment",
			slog.Int64("segmentID", segmentID),
			slog.Any("key", key),
			slog.Int("fingerprintCount", split.Fingerprints.Cardinality()),
			slog.Int64("recordCount", split.RecordCount),
			slog.Int64("fileSize", split.FileSize))

		// TODO this can be done just once per dateint.
		if err := queueLogCompaction(ctx, mdb, qmcFromInqueue(inf, -1, split.FirstTS)); err != nil {
			return err
		}
	}

	return nil
}
