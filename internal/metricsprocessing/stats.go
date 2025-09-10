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
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
)

// fileMetadata contains extracted metadata from a parquet file result
type fileMetadata struct {
	Fingerprints []int64
	StartTs      int64 // Inclusive start timestamp
	EndTs        int64 // Exclusive end timestamp (LastTS + 1)
	Dateint      int32 // YYYYMMDD
	Hour         int16 // Hour of day (0-23)
}

// extractFileMetadata extracts and validates metadata from a parquet file result
func extractFileMetadata(ctx context.Context, file parquetwriter.Result) (*fileMetadata, error) {
	ll := logctx.FromContext(ctx)

	stats, ok := file.Metadata.(factories.MetricsFileStats)
	if !ok {
		ll.Error("Failed to extract MetricsFileStats from result metadata",
			slog.String("metadataType", fmt.Sprintf("%T", file.Metadata)))
		return nil, fmt.Errorf("missing or invalid MetricsFileStats in result metadata")
	}

	// Validate timestamp range
	if stats.FirstTS == 0 || stats.LastTS == 0 || stats.FirstTS > stats.LastTS {
		ll.Error("Invalid timestamp range in metrics file stats",
			slog.Int64("startTs", stats.FirstTS),
			slog.Int64("lastTs", stats.LastTS),
			slog.Int64("recordCount", file.RecordCount))
		return nil, fmt.Errorf("invalid timestamp range: startTs=%d, lastTs=%d", stats.FirstTS, stats.LastTS)
	}

	// Extract dateint and hour from actual file timestamp data
	t := time.Unix(stats.FirstTS/1000, 0).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	hour := int16(t.Hour())

	return &fileMetadata{
		Fingerprints: stats.Fingerprints,
		StartTs:      stats.FirstTS,
		EndTs:        stats.LastTS + 1, // Database expects end-exclusive range [start, end)
		Dateint:      dateint,
		Hour:         hour,
	}, nil
}
