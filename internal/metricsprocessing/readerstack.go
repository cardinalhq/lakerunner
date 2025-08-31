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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type ReaderStackConfig struct {
	FileSortedCounter metric.Int64Counter
	CommonAttributes  attribute.Set
}

type ReaderStackResult struct {
	Readers         []filereader.Reader
	Files           []*os.File
	DownloadedFiles []string
	FinalReader     filereader.Reader
	MergedReader    filereader.Reader
}

func CreateReaderStack(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	s3client *awsclient.S3Client,
	orgID uuid.UUID,
	profile storageprofile.StorageProfile,
	startTimeMs int64,
	rows []lrdb.MetricSeg,
	config ReaderStackConfig,
	savepath string,
) (*ReaderStackResult, error) {
	var readers []filereader.Reader
	var files []*os.File
	var downloadedFiles []string

	for _, row := range rows {
		if ctx.Err() != nil {
			ll.Info("Context cancelled during segment download - safe interruption point",
				slog.Int64("segmentID", row.SegmentID),
				slog.Any("error", ctx.Err()))
			return nil, fmt.Errorf("context cancelled during segment download: %w", ctx.Err())
		}

		dateint, hour := helpers.MSToDateintHour(startTimeMs)
		objectID := helpers.MakeDBObjectID(orgID, profile.CollectorName, dateint, hour, row.SegmentID, "metrics")

		fn, _, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, profile.Bucket, objectID)
		if err != nil {
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return nil, err
		}
		if is404 {
			ll.Info("S3 object not found, skipping", slog.String("bucket", profile.Bucket), slog.String("objectID", objectID))
			continue
		}

		if savepath != "" {
			basename := filepath.Base(objectID)
			outpath := filepath.Join(savepath, "inputs", basename)
			out, err := os.Create(outpath)
			ll.Info("Saving downloaded file to save path", slog.String("file", outpath))
			if err != nil {
				ll.Error("Failed to create save file", slog.String("file", outpath), slog.Any("error", err))
				return nil, fmt.Errorf("creating save file %s: %w", filepath.Join(savepath, basename), err)
			}
			defer out.Close()
			in, err := os.Open(fn)
			if err != nil {
				ll.Error("Failed to open temp file for saving", slog.String("file", fn), slog.Any("error", err))
				return nil, fmt.Errorf("opening temp file %s for saving: %w", fn, err)
			}
			defer in.Close()
			_, err = io.Copy(out, in)
			if err != nil {
				ll.Error("Failed to copy to save file", slog.String("file", outpath), slog.Any("error", err))
				return nil, fmt.Errorf("copying to save file %s: %w", filepath.Join(savepath, basename), err)
			}
		}

		file, err := os.Open(fn)
		if err != nil {
			ll.Error("Failed to open parquet file", slog.String("file", fn), slog.Any("error", err))
			return nil, fmt.Errorf("opening parquet file %s: %w", fn, err)
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			ll.Error("Failed to stat parquet file", slog.String("file", fn), slog.Any("error", err))
			return nil, fmt.Errorf("statting parquet file %s: %w", fn, err)
		}

		reader, err := filereader.NewParquetRawReader(file, stat.Size(), 1000)
		if err != nil {
			file.Close()
			ll.Error("Failed to create parquet reader", slog.String("file", fn), slog.Any("error", err))
			return nil, fmt.Errorf("creating parquet reader for %s: %w", fn, err)
		}

		var finalReader filereader.Reader = reader
		sourceSortedWithCompatibleKey := row.SortVersion == lrdb.CurrentMetricSortVersion

		if !sourceSortedWithCompatibleKey {
			keyProvider := GetCurrentMetricSortKeyProvider()
			sortingReader, err := filereader.NewDiskSortingReader(reader, keyProvider, 1000)
			if err != nil {
				reader.Close()
				file.Close()
				ll.Error("Failed to create disk sorting reader", slog.String("file", fn), slog.Any("error", err))
				return nil, fmt.Errorf("creating disk sorting reader for %s: %w", fn, err)
			}
			finalReader = sortingReader
		}

		if config.FileSortedCounter != nil {
			fileFormat := getFileFormat(fn)
			inputSorted := sourceSortedWithCompatibleKey

			attrs := append(config.CommonAttributes.ToSlice(),
				attribute.String("format", fileFormat),
				attribute.Bool("input_sorted", inputSorted),
			)
			config.FileSortedCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
		}

		readers = append(readers, finalReader)
		files = append(files, file)
		downloadedFiles = append(downloadedFiles, fn)
	}

	var finalReader filereader.Reader
	var mergedReader filereader.Reader

	if len(readers) == 1 {
		finalReader = readers[0]
	} else if len(readers) > 1 {
		selector := MetricsOrderedSelector()
		multiReader, err := filereader.NewMergesortReader(readers, selector, 1000)
		if err != nil {
			return nil, fmt.Errorf("creating mergesort reader: %w", err)
		}
		finalReader = multiReader
		mergedReader = multiReader
	}

	return &ReaderStackResult{
		Readers:         readers,
		Files:           files,
		DownloadedFiles: downloadedFiles,
		FinalReader:     finalReader,
		MergedReader:    mergedReader,
	}, nil
}

func CloseReaderStack(ll *slog.Logger, result *ReaderStackResult) {
	if result.MergedReader != nil {
		if err := result.MergedReader.Close(); err != nil {
			ll.Error("Failed to close merged reader", slog.Any("error", err))
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

func getFileFormat(filename string) string {
	switch {
	case strings.HasSuffix(filename, ".binpb.gz"):
		return "binpb.gz"
	case strings.HasSuffix(filename, ".binpb"):
		return "binpb"
	case strings.HasSuffix(filename, ".parquet"):
		return "parquet"
	case strings.HasSuffix(filename, ".json.gz"):
		return "json.gz"
	case strings.HasSuffix(filename, ".json"):
		return "json"
	default:
		return "unknown"
	}
}
