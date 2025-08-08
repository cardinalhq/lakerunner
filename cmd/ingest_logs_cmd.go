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

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/fileconv/jsongz"
	protoconv "github.com/cardinalhq/lakerunner/fileconv/proto"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-logs",
		Short: "Ingest logs from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

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

			addlAttrs := attribute.NewSet(
				attribute.String("signal", "logs"),
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

			for {
				select {
				case <-doneCtx.Done():
					slog.Info("Ingest logs command done")
					return nil
				default:
				}

				err := IngestLoop(doneCtx, sp, "logs", servicename, logIngestItem)
				if err != nil {
					slog.Error("Error in ingest loop", slog.Any("error", err))
				}
			}
		},
	}

	rootCmd.AddCommand(cmd)
}

func logIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32) error {
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
			// TODO add counter for missing files
			ll.Info("S3 object not found, deleting inqueue work item", slog.String("bucket", inf.Bucket), slog.String("objectID", inf.ObjectID))
			return nil
		}
		// TODO add counter for download errors
		ll.Error("Failed to download S3 object", slog.Any("error", err))
		return err
	}
	ll.Info("Downloaded source file")

	filenames := []string{tmpfilename}

	// If the file is not in our `otel-raw` prefix, check if we can convert it
	if !strings.HasPrefix(inf.ObjectID, "otel-raw/") {
		// Skip database files (these are processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			// TODO add counter for skipped files in the db prefix
			return nil
		}

		// Check file type and convert if supported
		if fnames, err := convertFileIfSupported(ll, tmpfilename, tmpdir, inf.Bucket, inf.ObjectID); err != nil {
			ll.Error("Failed to convert file", slog.Any("error", err))
			// TODO add counter for failure to convert, probably in each convert function
			return err
		} else if len(fnames) == 0 {
			ll.Info("Empty source file, skipping", slog.String("objectID", inf.ObjectID))
			return nil
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
	}

	return nil
}

// convertFileIfSupported checks the file type and converts it if supported.
// Returns nil if the file type is not supported (file will be skipped).
func convertFileIfSupported(ll *slog.Logger, tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
	// TODO add a counter for each type we process, and a counter for unsupported types
	// Include the signal type in the attributes, as well as the converter used, and the extension found.
	switch {
	case strings.HasSuffix(objectID, ".parquet"):
		return ingestlogs.ConvertRawParquet(tmpfilename, tmpdir, bucket, objectID)
	case strings.HasSuffix(objectID, ".json.gz"):
		return convertJSONGzFile(tmpfilename, tmpdir, bucket, objectID)
	case strings.HasSuffix(objectID, ".binpb"):
		return convertProtoFile(tmpfilename, tmpdir, bucket, objectID)
	default:
		ll.Warn("Unsupported file type, skipping", slog.String("objectID", objectID))
		return nil, nil
	}
}

// convertJSONGzFile converts a JSON.gz file to the standardized format
func convertJSONGzFile(tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
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
	w, err := buffet.NewWriter("fileconv", tmpdir, nmb.Build(), 0, 0)
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

// convertProtoFile converts a protobuf file to the standardized format
func convertProtoFile(tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
	// Create a mapper for protobuf files
	mapper := translate.NewMapper()

	r, err := protoconv.NewProtoReader(tmpfilename, mapper, nil)
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
	w, err := buffet.NewWriter("fileconv", tmpdir, nmb.Build(), 0, 0)
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
