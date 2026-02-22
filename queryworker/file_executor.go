// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/cardinalhq/lakerunner/queryapi"
)

// FileResult holds metadata about a Parquet artifact written by EvaluatePushDownToFile.
type FileResult struct {
	RowCount int64
	MinTs    int64
	MaxTs    int64
}

// EvaluatePushDownToFile downloads segments and writes query results to a Parquet file
// using DuckDB COPY TO.
func EvaluatePushDownToFile(
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	userSQL string,
	outputPath string,
	tsColumn string,
) (*FileResult, error) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
	ctx, evalSpan := tracer.Start(ctx, "query.worker.evaluate_pushdown_to_file")
	defer evalSpan.End()

	evalSpan.SetAttributes(
		attribute.String("dataset", w.dataset),
		attribute.Int("segment_count", len(request.Segments)),
		attribute.String("output_path", outputPath),
	)

	if len(request.Segments) == 0 {
		evalSpan.SetStatus(codes.Error, "no segment paths")
		return nil, errors.New("no segment paths")
	}
	if !strings.Contains(userSQL, "{table}") {
		evalSpan.SetStatus(codes.Error, "invalid SQL template")
		return nil, errors.New(`userSQL must contain "{table}" placeholder`)
	}

	// Collect all local paths after downloading all segments.
	var allLocalPaths []string

	segmentsByOrg := make(map[uuid.UUID]map[int16][]queryapi.SegmentInfo)
	for _, seg := range request.Segments {
		if _, ok := segmentsByOrg[seg.OrganizationID]; !ok {
			segmentsByOrg[seg.OrganizationID] = make(map[int16][]queryapi.SegmentInfo)
		}
		segmentsByOrg[seg.OrganizationID][seg.InstanceNum] =
			append(segmentsByOrg[seg.OrganizationID][seg.InstanceNum], seg)
	}

	for orgId, instances := range segmentsByOrg {
		for instanceNum, segments := range instances {
			profile, err := w.getProfile(ctx, orgId, instanceNum)
			if err != nil {
				evalSpan.RecordError(err)
				evalSpan.SetStatus(codes.Error, "failed to get profile")
				return nil, err
			}

			var objectIDs []string
			for _, seg := range segments {
				objectID := fmt.Sprintf("db/%s/%s/%d/%s/%s/tbl_%d.parquet",
					orgId.String(),
					profile.CollectorName,
					seg.DateInt,
					w.dataset,
					seg.Hour,
					seg.SegmentID)
				objectIDs = append(objectIDs, objectID)
			}

			if len(objectIDs) > 0 {
				if err := w.downloadForQuery(ctx, profile, objectIDs); err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to download from S3")
					return nil, fmt.Errorf("download from S3: %w", err)
				}
			}

			for _, objID := range objectIDs {
				localPath, exists, err := w.parquetCache.GetOrPrepare(profile.Region, profile.Bucket, objID)
				if err != nil {
					return nil, fmt.Errorf("get local path for %s: %w", objID, err)
				}
				if exists {
					allLocalPaths = append(allLocalPaths, localPath)
				}
			}
		}
	}

	if len(allLocalPaths) == 0 {
		evalSpan.SetAttributes(attribute.Int("rows", 0))
		return &FileResult{RowCount: 0}, nil
	}

	// Build read_parquet source from all local paths.
	quoted := make([]string, len(allLocalPaths))
	for i := range allLocalPaths {
		quoted[i] = "'" + escapeSQL(allLocalPaths[i]) + "'"
	}
	array := "[" + strings.Join(quoted, ", ") + "]"
	src := fmt.Sprintf(`read_parquet(%s, union_by_name=true)`, array)
	selectSQL := strings.Replace(userSQL, "{table}", src, 1)

	// COPY TO Parquet.
	copySQL := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`, selectSQL, escapeSQL(outputPath))

	conn, release, err := w.pool.GetConnection(ctx)
	if err != nil {
		evalSpan.RecordError(err)
		evalSpan.SetStatus(codes.Error, "connection acquire failed")
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	slog.Info("Executing COPY TO", slog.Int("numFiles", len(allLocalPaths)), slog.String("output", outputPath))
	start := time.Now()
	_, err = conn.ExecContext(ctx, copySQL)
	if err != nil {
		// Retry without fingerprint normalization if applicable.
		if isMissingFingerprintError(err) {
			slog.Warn("Retrying COPY TO without fingerprint normalization")
			modCopy := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET)`,
				removeFingerprintNormalization(selectSQL), escapeSQL(outputPath))
			_, err = conn.ExecContext(ctx, modCopy)
		}
		if err != nil {
			evalSpan.RecordError(err)
			evalSpan.SetStatus(codes.Error, "COPY TO failed")
			return nil, fmt.Errorf("COPY TO: %w", err)
		}
	}
	slog.Info("COPY TO completed", slog.Duration("duration", time.Since(start)))

	// Extract metadata from the output file.
	result, err := extractFileMetadata(ctx, conn, outputPath, tsColumn)
	if err != nil {
		evalSpan.RecordError(err)
		evalSpan.SetStatus(codes.Error, "metadata extraction failed")
		return nil, fmt.Errorf("extract metadata: %w", err)
	}

	evalSpan.SetAttributes(
		attribute.Int64("row_count", result.RowCount),
		attribute.Int64("min_ts", result.MinTs),
		attribute.Int64("max_ts", result.MaxTs),
	)

	return result, nil
}

// extractFileMetadata queries a Parquet file for row count and timestamp range.
func extractFileMetadata(ctx context.Context, conn *sql.Conn, filePath, tsColumn string) (*FileResult, error) {
	var metaSQL string
	if tsColumn != "" {
		metaSQL = fmt.Sprintf(
			`SELECT count(*), coalesce(min("%s"), 0), coalesce(max("%s"), 0) FROM read_parquet('%s')`,
			tsColumn, tsColumn, escapeSQL(filePath))
	} else {
		metaSQL = fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQL(filePath))
	}

	result := &FileResult{}
	if tsColumn != "" {
		err := conn.QueryRowContext(ctx, metaSQL).Scan(&result.RowCount, &result.MinTs, &result.MaxTs)
		if err != nil {
			return nil, fmt.Errorf("metadata query: %w", err)
		}
	} else {
		err := conn.QueryRowContext(ctx, metaSQL).Scan(&result.RowCount)
		if err != nil {
			return nil, fmt.Errorf("metadata query: %w", err)
		}
	}
	return result, nil
}
