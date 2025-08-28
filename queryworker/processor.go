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

package queryworker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/promql"
)

func (s *Service) processSegments(ctx context.Context, request promql.PushDownRequest, resultsCh chan<- promql.SketchInput) error {
	for _, segment := range request.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := s.processSegment(ctx, request, segment, resultsCh); err != nil {
			slog.Error("Failed to process segment",
				"segmentID", segment.SegmentID,
				"error", err)
			continue // Continue with other segments
		}
	}
	return nil
}

func (s *Service) processSegment(ctx context.Context, request promql.PushDownRequest, segment promql.SegmentInfo, resultsCh chan<- promql.SketchInput) error {
	// Construct S3 key from segment info
	s3Key := s.buildS3Key(segment)

	slog.Debug("Processing segment",
		"segmentID", segment.SegmentID,
		"s3Key", s3Key,
		"startTs", segment.StartTs,
		"endTs", segment.EndTs)

	// Parse organization ID from segment
	organizationID, err := uuid.Parse(segment.CustomerID)
	if err != nil {
		return fmt.Errorf("failed to parse organization ID from segment: %w", err)
	}

	// Get the Parquet file (from cache or S3)
	localPath, err := s.cache.GetFile(ctx, organizationID, s3Key)
	if err != nil {
		return fmt.Errorf("failed to get parquet file: %w", err)
	}

	// Cleanup temp file if not cached
	if !s.cache.enableCache {
		defer func() {
			if err := s.cache.cleanupTempFile(localPath); err != nil {
				slog.Warn("Failed to cleanup temp file", "error", err)
			}
		}()
	}

	// Query the Parquet file
	if err := s.queryParquetFile(ctx, localPath, request, segment, resultsCh); err != nil {
		return fmt.Errorf("failed to query parquet file: %w", err)
	}

	return nil
}

func (s *Service) buildS3Key(segment promql.SegmentInfo) string {
	// Build S3 key based on segment metadata
	// Format: {dataset}/{customerID}/{dateInt}/{hour}/{segmentID}.parquet
	return fmt.Sprintf("%s/%s/%d/%s/%s.parquet",
		segment.Dataset,
		segment.CustomerID,
		segment.DateInt,
		segment.Hour,
		segment.SegmentID)
}

func (s *Service) queryParquetFile(ctx context.Context, filePath string, request promql.PushDownRequest, segment promql.SegmentInfo, resultsCh chan<- promql.SketchInput) error {
	// TODO: Implement actual Parquet querying logic
	// This would typically involve:
	// 1. Opening the Parquet file
	// 2. Applying filters based on request.StartTs, request.EndTs
	// 3. Evaluating the BaseExpr against the data
	// 4. Streaming results to resultsCh

	slog.Info("Querying parquet file",
		"filePath", filePath,
		"segmentID", segment.SegmentID,
		"exprID", request.BaseExpr.ID)

	// Placeholder implementation - generate mock data
	// In real implementation, this would read and process the Parquet file
	for i := range 10 {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Mock result
		result := promql.SketchInput{
			ExprID:         request.BaseExpr.ID,
			OrganizationID: segment.CustomerID,
			Timestamp:      request.StartTs + int64(i*1000), // 1 second intervals
			Frequency:      segment.Frequency / 1000,        // Convert ms to seconds
			SketchTags:     promql.SketchTags{},             // Empty for now
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultsCh <- result:
		}
	}

	slog.Debug("Completed querying parquet file", "segmentID", segment.SegmentID)
	return nil
}

// Add cleanup method to cache
func (c *ParquetCache) cleanupTempFile(path string) error {
	if filepath.Dir(path) == c.cacheDir {
		// Don't delete cached files
		return nil
	}
	return os.Remove(path)
}
