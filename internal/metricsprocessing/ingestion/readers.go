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

package ingestion

import (
	"compress/gzip"
	"fmt"
	"os"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// IsSupportedMetricsFile checks if the file is a supported metrics file type
func IsSupportedMetricsFile(objectID string) bool {
	// Only support .binpb and .binpb.gz files from otel-raw/ path structure
	if !strings.HasPrefix(objectID, "otel-raw/") {
		return false
	}

	return strings.HasSuffix(objectID, ".binpb") || strings.HasSuffix(objectID, ".binpb.gz")
}

// createMetricOrderSelector creates a selector function for metrics that orders by [metric_name, tid, timestamp]
func createMetricOrderSelector() filereader.SelectFunc {
	return func(rows []filereader.Row) int {
		if len(rows) == 0 {
			return 0
		}

		bestIdx := 0
		bestRow := rows[0]
		if bestRow == nil {
			return bestIdx
		}

		// Extract comparison values from the best row
		bestName, _ := bestRow[wkk.RowKeyCName].(string)
		bestTid, _ := bestRow[wkk.RowKeyCTID].(int64)
		bestTs, _ := bestRow[wkk.RowKeyCTimestamp].(int64)

		for i := 1; i < len(rows); i++ {
			if rows[i] == nil {
				continue
			}

			// Extract comparison values from current row
			name, _ := rows[i][wkk.RowKeyCName].(string)
			tid, _ := rows[i][wkk.RowKeyCTID].(int64)
			ts, _ := rows[i][wkk.RowKeyCTimestamp].(int64)

			// Compare by [name, tid, timestamp] in ascending order
			if name < bestName ||
				(name == bestName && tid < bestTid) ||
				(name == bestName && tid == bestTid && ts < bestTs) {
				bestIdx = i
				bestName = name
				bestTid = tid
				bestTs = ts
			}
		}

		return bestIdx
	}
}

// CreateMetricProtoReader creates a protocol buffer reader for metrics files
func CreateMetricProtoReader(filename string) (filereader.Reader, error) {
	// Only support .binpb and .binpb.gz files
	switch {
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createMetricProtoBinaryGzReader(filename)
	case strings.HasSuffix(filename, ".binpb"):
		return createMetricProtoBinaryReader(filename)
	default:
		return nil, fmt.Errorf("unsupported metrics file type: %s (only .binpb and .binpb.gz are supported)", filename)
	}
}

// createMetricProtoBinaryReader creates a metrics proto reader for a protobuf file
func createMetricProtoBinaryReader(filename string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	reader, err := filereader.NewIngestProtoMetricsReader(file, 1000)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}

// createMetricProtoBinaryGzReader creates a metrics proto reader for a gzipped protobuf file
func createMetricProtoBinaryGzReader(filename string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := filereader.NewIngestProtoMetricsReader(gzipReader, 1000)
	if err != nil {
		gzipReader.Close()
		file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}
