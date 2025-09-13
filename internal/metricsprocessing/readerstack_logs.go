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

package metricsprocessing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type logReaderStackResult struct {
	Readers         []filereader.Reader
	Files           []*os.File
	DownloadedFiles []string
	HeadReader      filereader.Reader
}

func createLogReaderStack(
	ctx context.Context,
	tmpdir string,
	blobclient cloudstorage.Client,
	orgID uuid.UUID,
	profile storageprofile.StorageProfile,
	rows []lrdb.LogSeg,
) (*logReaderStackResult, error) {
	ll := logctx.FromContext(ctx)

	var readers []filereader.Reader
	var files []*os.File
	var downloadedFiles []string

	if len(rows) == 0 {
		return nil, errors.New("no log segments provided to create reader stack")
	}

	for _, row := range rows {
		if ctx.Err() != nil {
			ll.Info("Context cancelled during segment download",
				slog.Int64("segmentID", row.SegmentID),
				slog.Any("error", ctx.Err()))
			return nil, fmt.Errorf("context cancelled during segment download: %w", ctx.Err())
		}

		dateint, hour := helpers.MSToDateintHour(row.TsRange.Lower.Int64)
		objectID := helpers.MakeDBObjectID(orgID, profile.CollectorName, dateint, hour, row.SegmentID, "logs")

		fn, _, is404, err := blobclient.DownloadObject(ctx, tmpdir, profile.Bucket, objectID)
		if err != nil {
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return nil, err
		}
		if is404 {
			ll.Info("S3 object not found, skipping",
				slog.String("bucket", profile.Bucket),
				slog.String("objectID", objectID),
				slog.Int64("segmentID", row.SegmentID))
			continue
		}

		file, err := os.Open(fn)
		if err != nil {
			ll.Error("Failed to open parquet file", slog.String("file", fn), slog.Any("error", err))
			return nil, fmt.Errorf("opening parquet file %s: %w", fn, err)
		}

		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			ll.Error("Failed to stat parquet file", slog.String("file", fn), slog.Any("error", err))
			return nil, fmt.Errorf("statting parquet file %s: %w", fn, err)
		}

		reader, err := filereader.NewCookedLogParquetReader(file, stat.Size(), 1000)
		if err != nil {
			_ = file.Close()
			ll.Error("Failed to create parquet reader", slog.String("file", fn), slog.Any("error", err))
			return nil, fmt.Errorf("creating parquet reader for %s: %w", fn, err)
		}

		finalReader := reader

		readers = append(readers, finalReader)
		files = append(files, file)
		downloadedFiles = append(downloadedFiles, fn)
	}

	// Always use merge sort reader for consistency
	keyProvider := &filereader.TimestampSortKeyProvider{}
	mergedReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	if err != nil {
		return nil, fmt.Errorf("creating mergesort reader: %w", err)
	}

	return &logReaderStackResult{
		Readers:         readers,
		Files:           files,
		DownloadedFiles: downloadedFiles,
		HeadReader:      mergedReader,
	}, nil
}

func (result *logReaderStackResult) Close(ctx context.Context) {
	ll := logctx.FromContext(ctx)

	if result.HeadReader != nil {
		if err := result.HeadReader.Close(); err != nil {
			ll.Error("Failed to close head reader", slog.Any("error", err))
		}
	}
	for _, reader := range result.Readers {
		if err := reader.Close(); err != nil {
			ll.Error("Failed to close reader", slog.Any("error", err))
		}
	}
	for _, file := range result.Files {
		if err := file.Close(); err != nil {
			ll.Error("Failed to close file", slog.String("file", file.Name()), slog.Any("error", err))
		}
	}
}
