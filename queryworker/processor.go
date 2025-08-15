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
	"strings"

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
				slog.Warn("Failed to cleanup temp file", "path", localPath, "error", err)
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
	// Generate SQL from the BaseExpr
	sql := request.BaseExpr.ToWorkerSQL(request.Step)
	if sql == "" {
		return fmt.Errorf("no SQL generated for expression")
	}

	// Replace template placeholders with actual values
	sql = strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", request.StartTs))
	sql = strings.ReplaceAll(sql, "{end}", fmt.Sprintf("%d", request.EndTs))
	sql = strings.ReplaceAll(sql, "{table}", fmt.Sprintf("read_parquet('%s')", filePath))

	slog.Info("Executing SQL on parquet file",
		"filePath", filePath,
		"segmentID", segment.SegmentID,
		"exprID", request.BaseExpr.ID,
		"sql", sql)

	// Execute the SQL query
	rows, err := s.ddb.Query(ctx, sql)
	if err != nil {
		slog.Error("failed to query parquet file", "segmentID", segment.SegmentID, "err", err.Error())
		return fmt.Errorf("failed to query parquet file: %w", err)
	}
	defer rows.Close()

	// Get column information
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Process query results
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			slog.Error("failed to scan row", "err", err)
			continue
		}

		var ts int64
		agg := map[string]float64{}
		tags := map[string]any{}

		// Parse columns similar to how evaluator.go does it
		for i, col := range cols {
			switch col {
			case "bucket_ts":
				switch v := vals[i].(type) {
				case int64:
					ts = v
				case int32:
					ts = int64(v)
				case int:
					ts = int64(v)
				default:
					slog.Error("unexpected type for bucket_ts", "value", vals[i])
					continue
				}
			case promql.SUM, promql.COUNT, promql.MIN, promql.MAX:
				if vals[i] == nil {
					continue
				}
				switch v := vals[i].(type) {
				case float64:
					agg[col] = v
				case float32:
					agg[col] = float64(v)
				case int64:
					agg[col] = float64(v)
				case int32:
					agg[col] = float64(v)
				case int:
					agg[col] = float64(v)
				default:
					slog.Warn("unexpected numeric type in agg", "col", col, "value", vals[i])
				}
			default:
				if vals[i] != nil {
					tags[col] = vals[i]
				}
			}
		}

		// Create and send result
		result := promql.SketchInput{
			ExprID:         request.BaseExpr.ID,
			OrganizationID: segment.CustomerID,
			Timestamp:      ts,
			Frequency:      int64(request.Step.Seconds()),
			SketchTags: promql.SketchTags{
				Tags:       tags,
				SketchType: promql.SketchMAP,
				Agg:        agg,
			},
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultsCh <- result:
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
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
