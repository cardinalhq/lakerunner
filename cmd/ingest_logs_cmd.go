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
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/jsongz"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/rawparquet"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
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

	filenames := []string{tmpfilename}

	// If the file is not in our `otel-raw` prefix, check if we can convert it
	if !strings.HasPrefix(inf.ObjectID, "otel-raw/") {
		// Skip database files (these are processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			return nil
		}

		// Check file type and convert if supported
		if fnames, err := convertFileIfSupported(ll, tmpfilename, tmpdir, inf.Bucket, inf.ObjectID); err != nil {
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
	}

	return nil
}

func convertRawParquet(tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
	r, err := rawparquet.NewRawParquetReader(tmpfilename, translate.NewMapper(), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	nodes, err := r.Nodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}
	slog.Info("nodes", slog.Any("nodes", nodes))

	// add our new nodes to the list of nodes we will write out
	nmb := buffet.NewNodeMapBuilder()
	if err := nmb.AddNodes(nodes); err != nil {
		return nil, fmt.Errorf("failed to add nodes: %w", err)
	}
	if err := nmb.Add(map[string]any{
		"resource.bucket.name": "bucket",
		"resource.file.name":   "object",
		"resource.file.type":   "filetype",
	}); err != nil {
		return nil, fmt.Errorf("failed to add resource nodes: %w", err)
	}

	w, err := buffet.NewWriter("fileconv", tmpdir, nmb.Build(), 0, 0)
	defer func() {
		_, err := w.Close()
		if errors.Is(err, buffet.ErrAlreadyClosed) {
			if err != nil {
				slog.Error("Failed to close writer", slog.Any("error", err))
			}
		}
	}()

	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   getFileType(objectID),
	}

	for {
		row, done, err := r.GetRow()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		for k, v := range baseitems {
			row[k] = v
		}
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

var nonLetter = regexp.MustCompile(`[^a-zA-Z]`)

// getFileType extracts the “base” of the filename (everything before the last dot),
// then strips out any non‑letter characters.
func getFileType(p string) string {
	// equivalent of Scala’s path.split("/").lastOption.getOrElse("")
	fileName := path.Base(p)

	// find last “.”; if none, use whole filename
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		fileName = fileName[:idx]
	}

	// strip out anything that isn’t A–Z or a–z
	return nonLetter.ReplaceAllString(fileName, "")
}

// convertFileIfSupported checks the file type and converts it if supported.
// Returns nil if the file type is not supported (file will be skipped).
func convertFileIfSupported(ll *slog.Logger, tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
	switch {
	case strings.HasSuffix(objectID, ".parquet"):
		return convertRawParquet(tmpfilename, tmpdir, bucket, objectID)
	case strings.HasSuffix(objectID, ".json.gz"):
		return convertJSONGzFile(tmpfilename, tmpdir, bucket, objectID)
	default:
		ll.Warn("Unsupported file type, skipping", slog.String("objectID", objectID))
		return nil, nil
	}
}

// convertJSONGzFile converts a JSON.gz file to the standardized format
func convertJSONGzFile(tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
	r, err := jsongz.NewJSONGzReader(tmpfilename, translate.NewMapper(), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	nmb := buffet.NewNodeMapBuilder()

	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   getFileType(objectID),
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
