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

package factories

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

const (
	// AggFrequency is the time bucket frequency in milliseconds (10 seconds = 10000ms)
	AggFrequency int64 = 10000
)

var (
	// AggFilesWrittenCounter counts successfully written agg_ files
	AggFilesWrittenCounter otelmetric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/parquetwriter/factories")

	var err error
	AggFilesWrittenCounter, err = meter.Int64Counter(
		"lakerunner.logs.agg_files_written",
		otelmetric.WithDescription("Number of aggregation parquet files written"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create agg_files_written counter: %w", err))
	}
}

// LogAggRow represents a single row in the aggregation parquet file.
type LogAggRow struct {
	TimestampBucket int64  `parquet:"timestamp_bucket"`
	LogLevel        string `parquet:"log_level"`
	StreamId        string `parquet:"stream_id"`
	Frequency       int64  `parquet:"frequency"`
	Count           int64  `parquet:"count"`
}

// WriteAggParquet writes the aggregation data to a parquet file.
// The file is sorted by timestamp_bucket.
// Returns the file size in bytes.
func WriteAggParquet(ctx context.Context, filename string, counts map[LogAggKey]int64) (int64, error) {
	if len(counts) == 0 {
		return 0, nil
	}

	// Convert map to sorted slice of rows
	rows := make([]LogAggRow, 0, len(counts))
	for key, count := range counts {
		rows = append(rows, LogAggRow{
			TimestampBucket: key.TimestampBucket,
			LogLevel:        key.LogLevel,
			StreamId:        key.StreamId,
			Frequency:       AggFrequency,
			Count:           count,
		})
	}

	// Sort by timestamp_bucket for efficient querying
	slices.SortFunc(rows, func(a, b LogAggRow) int {
		if a.TimestampBucket < b.TimestampBucket {
			return -1
		}
		if a.TimestampBucket > b.TimestampBucket {
			return 1
		}
		return 0
	})

	// Create output file
	f, err := os.Create(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to create agg parquet file: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Create parquet writer
	writer := parquet.NewGenericWriter[LogAggRow](f,
		parquet.Compression(&parquet.Zstd),
	)

	// Write all rows
	if _, err := writer.Write(rows); err != nil {
		return 0, fmt.Errorf("failed to write agg rows: %w", err)
	}

	// Close writer to flush data
	if err := writer.Close(); err != nil {
		return 0, fmt.Errorf("failed to close agg parquet writer: %w", err)
	}

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat agg parquet file: %w", err)
	}

	return stat.Size(), nil
}

// GetAggFields returns the list of fields used in the aggregation.
// These are stored in the agg_fields column of the log_seg table.
func GetAggFields(streamField string) []string {
	if streamField == "" {
		// Use default - resource_customer_domain has higher priority
		return []string{"log_level", "resource_customer_domain"}
	}
	return []string{"log_level", streamField}
}

// RecordAggFileWritten records a successful agg file write metric.
func RecordAggFileWritten(ctx context.Context, orgID uuid.UUID, instanceNum int16) {
	AggFilesWrittenCounter.Add(ctx, 1,
		otelmetric.WithAttributes(
			attribute.String("organization_id", orgID.String()),
			attribute.Int("instance_num", int(instanceNum)),
		),
	)
}
