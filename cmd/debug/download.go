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

package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func GetDownloadCmd() *cobra.Command {
	var (
		organization string
		signal       string
		hours        int
		endpoint     string
		frequency    time.Duration
	)

	cmd := &cobra.Command{
		Use:   "download",
		Short: "Download Parquet files for debugging",
		Long:  `Download the last hour's worth of Parquet files from the database for a specific organization and signal type.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDownload(cmd.Context(), organization, signal, hours, endpoint, frequency)
		},
	}

	cmd.Flags().StringVar(&organization, "organization", "", "Organization ID (required)")
	cmd.Flags().StringVar(&signal, "signal", "", "Signal type: metrics, logs, or traces (required)")
	cmd.Flags().IntVar(&hours, "hours", 1, "Number of hours to download (default: 1)")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "Override storage endpoint")
	cmd.Flags().DurationVar(&frequency, "frequency", 10*time.Second, "Metric frequency: 10s, 60s, 5m, 20m, or 60m (default: 10s)")
	_ = cmd.MarkFlagRequired("organization")
	_ = cmd.MarkFlagRequired("signal")

	return cmd
}

func runDownload(ctx context.Context, orgStr, signal string, hours int, endpoint string, frequency time.Duration) error {
	orgID, err := uuid.Parse(orgStr)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	if signal != "metrics" && signal != "logs" && signal != "traces" {
		return fmt.Errorf("invalid signal type: %s (must be metrics, logs, or traces)", signal)
	}

	// Convert frequency to milliseconds for metrics (ignored for other signals)
	var frequencyMs int32
	if signal == "metrics" {
		// Validate frequency is one of the allowed values
		validFrequencies := map[time.Duration]bool{
			10 * time.Second: true, // 10s
			60 * time.Second: true, // 60s
			5 * time.Minute:  true, // 5m
			20 * time.Minute: true, // 20m
			60 * time.Minute: true, // 60m
		}

		if !validFrequencies[frequency] {
			return fmt.Errorf("invalid frequency %v: must be one of 10s, 60s, 5m, 20m, or 60m", frequency)
		}

		frequencyMs = int32(frequency.Milliseconds())
	}

	lrStore, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to open lrdb: %w", err)
	}
	defer lrStore.Close()

	configPool, err := dbopen.ConnectToConfigDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to open configdb: %w", err)
	}
	defer configPool.Close()

	configStore := configdb.NewStore(configPool)
	profileProvider := storageprofile.NewStorageProfileProvider(configStore)

	// Calculate time range
	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(hours) * time.Hour)
	startDateint := timeDateint(startTime)
	endDateint := timeDateint(endTime)
	startTimeMs := startTime.UnixMilli()
	endTimeMs := endTime.UnixMilli()

	cloudManagers, err := cloudstorage.NewCloudManagers(ctx)
	if err != nil {
		return fmt.Errorf("failed to create cloud managers: %w", err)
	}

	switch signal {
	case "metrics":
		return downloadMetricSegments(ctx, lrStore, cloudManagers, profileProvider, orgID, orgStr,
			startDateint, endDateint, startTimeMs, endTimeMs, endpoint, frequencyMs)
	case "logs":
		return downloadLogSegments(ctx, lrStore, cloudManagers, profileProvider, orgID, orgStr,
			startDateint, endDateint, startTimeMs, endTimeMs, endpoint)
	case "traces":
		return downloadTraceSegments(ctx, lrStore, cloudManagers, profileProvider, orgID, orgStr,
			startDateint, endDateint, startTimeMs, endTimeMs, endpoint)
	default:
		return fmt.Errorf("unknown signal type: %s", signal)
	}
}

func downloadMetricSegments(ctx context.Context, store *lrdb.Store, cloudManagers cloudstorage.ClientProvider,
	profileProvider storageprofile.StorageProfileProvider, orgID uuid.UUID, orgStr string,
	startDateint, endDateint int32, startTimeMs, endTimeMs int64, endpoint string, frequencyMs int32) error {

	segments, err := store.GetMetricSegmentsForDownload(ctx, lrdb.GetMetricSegmentsForDownloadParams{
		OrganizationID: orgID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTime:      startTimeMs,
		EndTime:        endTimeMs,
		FrequencyMs:    frequencyMs,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch metric segments: %w", err)
	}

	if len(segments) == 0 {
		if frequencyMs > 0 {
			fmt.Printf("No segments found for the specified time range with frequency %dms\n", frequencyMs)
		} else {
			fmt.Println("No segments found for the specified time range")
		}
		return nil
	}

	dir := createOutputDir(orgStr, segments[0].Dateint)
	fmt.Printf("Downloading %d metric segments to %s\n", len(segments), dir)

	if err := writeSegments(segments, filepath.Join(dir, "metric_seg.json")); err != nil {
		return fmt.Errorf("failed to write segment metadata: %w", err)
	}

	downloads := make([]downloadInfo, 0, len(segments))
	for _, seg := range segments {
		startTs, _ := getTsRangeBounds(seg.TsRange)
		downloads = append(downloads, downloadInfo{
			OrganizationID: seg.OrganizationID,
			Dateint:        seg.Dateint,
			SegmentID:      seg.SegmentID,
			InstanceNum:    seg.InstanceNum,
			StartTs:        startTs,
		})
	}

	if err := downloadParquetFiles(ctx, cloudManagers, profileProvider, downloads, dir, "metrics", orgID, endpoint); err != nil {
		return fmt.Errorf("failed to download files: %w", err)
	}

	fmt.Printf("Successfully downloaded %d metric segments\n", len(segments))
	return nil
}

func downloadLogSegments(ctx context.Context, store *lrdb.Store, cloudManagers cloudstorage.ClientProvider,
	profileProvider storageprofile.StorageProfileProvider, orgID uuid.UUID, orgStr string,
	startDateint, endDateint int32, startTimeMs, endTimeMs int64, endpoint string) error {

	segments, err := store.GetLogSegmentsForDownload(ctx, lrdb.GetLogSegmentsForDownloadParams{
		OrganizationID: orgID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTime:      startTimeMs,
		EndTime:        endTimeMs,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch log segments: %w", err)
	}

	if len(segments) == 0 {
		fmt.Println("No segments found for the specified time range")
		return nil
	}

	dir := createOutputDir(orgStr, segments[0].Dateint)
	fmt.Printf("Downloading %d log segments to %s\n", len(segments), dir)

	if err := writeSegments(segments, filepath.Join(dir, "log_seg.json")); err != nil {
		return fmt.Errorf("failed to write segment metadata: %w", err)
	}

	downloads := make([]downloadInfo, 0, len(segments))
	for _, seg := range segments {
		startTs, _ := getTsRangeBounds(seg.TsRange)
		downloads = append(downloads, downloadInfo{
			OrganizationID: seg.OrganizationID,
			Dateint:        seg.Dateint,
			SegmentID:      seg.SegmentID,
			InstanceNum:    seg.InstanceNum,
			StartTs:        startTs,
		})
	}

	if err := downloadParquetFiles(ctx, cloudManagers, profileProvider, downloads, dir, "logs", orgID, endpoint); err != nil {
		return fmt.Errorf("failed to download files: %w", err)
	}

	fmt.Printf("Successfully downloaded %d log segments\n", len(segments))
	return nil
}

func downloadTraceSegments(ctx context.Context, store *lrdb.Store, cloudManagers cloudstorage.ClientProvider,
	profileProvider storageprofile.StorageProfileProvider, orgID uuid.UUID, orgStr string,
	startDateint, endDateint int32, startTimeMs, endTimeMs int64, endpoint string) error {

	segments, err := store.GetTraceSegmentsForDownload(ctx, lrdb.GetTraceSegmentsForDownloadParams{
		OrganizationID: orgID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTime:      startTimeMs,
		EndTime:        endTimeMs,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch trace segments: %w", err)
	}

	if len(segments) == 0 {
		fmt.Println("No segments found for the specified time range")
		return nil
	}

	dir := createOutputDir(orgStr, segments[0].Dateint)
	fmt.Printf("Downloading %d trace segments to %s\n", len(segments), dir)

	if err := writeSegments(segments, filepath.Join(dir, "trace_seg.json")); err != nil {
		return fmt.Errorf("failed to write segment metadata: %w", err)
	}

	downloads := make([]downloadInfo, 0, len(segments))
	for _, seg := range segments {
		startTs, _ := getTsRangeBounds(seg.TsRange)
		downloads = append(downloads, downloadInfo{
			OrganizationID: seg.OrganizationID,
			Dateint:        seg.Dateint,
			SegmentID:      seg.SegmentID,
			InstanceNum:    seg.InstanceNum,
			StartTs:        startTs,
		})
	}

	if err := downloadParquetFiles(ctx, cloudManagers, profileProvider, downloads, dir, "traces", orgID, endpoint); err != nil {
		return fmt.Errorf("failed to download files: %w", err)
	}

	fmt.Printf("Successfully downloaded %d trace segments\n", len(segments))
	return nil
}

type downloadInfo struct {
	OrganizationID uuid.UUID
	Dateint        int32
	SegmentID      int64
	InstanceNum    int16
	StartTs        int64
}

// Generic download function for Parquet files
func downloadParquetFiles(ctx context.Context, cloudManagers cloudstorage.ClientProvider, profileProvider storageprofile.StorageProfileProvider,
	downloads []downloadInfo, dir, signal string, orgID uuid.UUID, endpoint string) error {

	for _, dl := range downloads {
		profile, err := profileProvider.GetStorageProfileForOrganizationAndInstance(ctx, orgID, dl.InstanceNum)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for org %s instance %d: %w", orgID, dl.InstanceNum, err)
		}

		if endpoint != "" {
			profile.Endpoint = endpoint
		}

		storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
		if err != nil {
			return fmt.Errorf("failed to create storage client for org %s instance %d: %w", orgID, dl.InstanceNum, err)
		}

		hour := helpers.HourFromMillis(dl.StartTs)
		s3Key := helpers.MakeDBObjectID(
			dl.OrganizationID,
			profile.CollectorName,
			dl.Dateint,
			hour,
			dl.SegmentID,
			signal,
		)

		localPath := filepath.Join(dir, fmt.Sprintf("tbl_%d.parquet", dl.SegmentID))

		fmt.Printf("Downloading %s from %s -> %s\n", s3Key, profile.Bucket, localPath)

		tmpFile, _, notFound, err := storageClient.DownloadObject(ctx, dir, profile.Bucket, s3Key)
		if err != nil {
			if notFound {
				return fmt.Errorf("object %s not found in bucket %s", s3Key, profile.Bucket)
			}
			return fmt.Errorf("failed to download %s from bucket %s: %w", s3Key, profile.Bucket, err)
		}

		if err := os.Rename(tmpFile, localPath); err != nil {
			if err := copyFile(tmpFile, localPath); err != nil {
				_ = os.Remove(tmpFile)
				return fmt.Errorf("failed to move file to %s: %w", localPath, err)
			}
			_ = os.Remove(tmpFile)
		}
	}

	return nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = sourceFile.Close() }()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = destFile.Close() }()

	_, err = destFile.ReadFrom(sourceFile)
	return err
}

// Generic write function for segments
func writeSegments[T lrdb.TraceSeg | lrdb.LogSeg | lrdb.MetricSeg](segments []T, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	encoder := json.NewEncoder(file)
	for _, seg := range segments {
		if err := encoder.Encode(seg); err != nil {
			return err
		}
	}
	return nil
}

// Helper functions
func timeDateint(t time.Time) int32 {
	return int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
}

func createOutputDir(orgID string, dateint int32) string {
	baseName := fmt.Sprintf("%s_%d", orgID, dateint)
	dir := baseName

	for i := 1; ; i++ {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			break
		}
		dir = fmt.Sprintf("%s_%d", baseName, i)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(fmt.Errorf("failed to create directory: %w", err))
	}

	return dir
}

func getTsRangeBounds(tsRange pgtype.Range[pgtype.Int8]) (int64, int64) {
	if !tsRange.Valid {
		return 0, 0
	}

	var start, end int64
	if tsRange.Lower.Valid {
		start = tsRange.Lower.Int64
	}
	if tsRange.Upper.Valid {
		end = tsRange.Upper.Int64 - 1 // Upper bound is exclusive in postgres ranges
	}
	return start, end
}
