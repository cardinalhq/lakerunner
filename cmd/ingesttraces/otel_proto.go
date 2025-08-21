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

// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the GNU Affero General Public License, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR ANY PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package ingesttraces

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	oteltranslate "github.com/cardinalhq/oteltools/pkg/translate"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/fileconv/proto"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/buffet"
)

// NumTracePartitions is the number of partitions/slots for trace processing.
// Can be configured via LAKERUNNER_TRACE_PARTITIONS environment variable, defaults to 16.
// Compaction compacts all files in a slot - so increase this to increase parallelism.
// However, more slots means more individual files, so for smaller customers it's better to keep it low.
var NumTracePartitions = func() int {
	if partitionsStr := os.Getenv("LAKERUNNER_TRACE_PARTITIONS"); partitionsStr != "" {
		if partitions, err := strconv.Atoi(partitionsStr); err == nil && partitions > 0 {
			return partitions
		}
		// Log warning if invalid value, fall back to default
		slog.Warn("Invalid LAKERUNNER_TRACE_PARTITIONS value, using default",
			"value", partitionsStr, "default", 16)
	}
	return 16
}()

// determineSlot determines which partition slot a trace should go to.
// This ensures that the same trace ID always goes to the same slot for consistency.
func determineSlot(traceID string, dateint int32, orgID string) int {
	// Create a unique key combining trace ID, dateint, and orgID
	key := fmt.Sprintf("%s_%d_%s", traceID, dateint, orgID)

	// Hash the key to get a deterministic slot assignment
	h := sha256.Sum256([]byte(key))

	// Use the first 2 bytes of the hash to get a 16-bit number, then modulo by partition count
	return int(binary.BigEndian.Uint16(h[:])) % NumTracePartitions
}

// ConvertProtoFile converts a protobuf file to the standardized format
func ConvertProtoFile(tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64, dateint int32, orgID string) ([]TraceFileResult, error) {
	// Create a mapper for protobuf files
	mapper := translate.NewMapper()

	// Create a traces protobuf reader using the mapper
	r, err := proto.NewTracesProtoReader(tmpfilename, mapper, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create traces proto reader: %w", err)
	}
	defer r.Close()

	// First pass: read all rows to build complete schema
	allRows := make([]map[string]any, 0)
	nmb := buffet.NewNodeMapBuilder()

	// Add base items to schema builder
	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   ingestlogs.GetFileType(objectID),
	}

	// Add base items to schema
	for k, v := range baseitems {
		if err := nmb.Add(map[string]any{k: v}); err != nil {
			return nil, fmt.Errorf("failed to add base item to schema: %w", err)
		}
	}

	// Read all rows and build complete schema
	for {
		row, done, err := r.GetRow()
		if err != nil {
			return nil, fmt.Errorf("failed to get row from traces reader: %w", err)
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

	// Build complete schema from all rows
	completeSchema := nmb.Build()

	// Create NumTracePartitions BuffetWriters, one per slot, all using the complete schema
	writers := make(map[int]*buffet.Writer, NumTracePartitions)

	// Initialize writers for each slot
	for slot := 0; slot < NumTracePartitions; slot++ {
		// Create a unique directory for each slot
		slotDir := filepath.Join(tmpdir, fmt.Sprintf("slot_%d", slot))
		if err := os.MkdirAll(slotDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create slot directory %d: %w", slot, err)
		}

		// Create writer for this slot using the complete schema
		writer, err := buffet.NewWriter("fileconv", slotDir, completeSchema, rpfEstimate)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer for slot %d: %w", slot, err)
		}

		writers[slot] = writer
	}

	// Track timestamp ranges for each slot (similar to how logs track hourly boundaries)
	slotTimestampRanges := make(map[int]*SlotTimestampRange)

	// Second pass: route each row to the appropriate slot based on trace ID
	for _, row := range allRows {
		// Extract trace ID from the row - use the correct field name
		traceID, ok := row[oteltranslate.CardinalFieldSpanTraceID].(string)
		if !ok {
			// If no trace ID, use a default slot (0)
			traceID = "unknown"
		}

		// Extract timestamp from the row
		timestamp, ok := row[oteltranslate.CardinalFieldTimestamp].(int64)
		if !ok {
			// If no timestamp, skip this row or use a default
			continue
		}

		// Determine which slot this trace should go to
		slot := determineSlot(traceID, dateint, orgID)

		// Track timestamp range for this slot
		if slotRange, exists := slotTimestampRanges[slot]; exists {
			if timestamp < slotRange.MinTimestamp {
				slotRange.MinTimestamp = timestamp
			}
			if timestamp > slotRange.MaxTimestamp {
				slotRange.MaxTimestamp = timestamp
			}
		} else {
			slotTimestampRanges[slot] = &SlotTimestampRange{
				MinTimestamp: timestamp,
				MaxTimestamp: timestamp,
			}
		}

		// Get the writer for this slot
		writer, exists := writers[slot]
		if !exists {
			return nil, fmt.Errorf("writer for slot %d not found", slot)
		}

		// Write the row to the appropriate slot
		if err := writer.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write row to slot %d: %w", slot, err)
		}
	}

	// Close all writers and collect results
	var allResults []TraceFileResult
	for slot, writer := range writers {
		result, err := writer.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close writer for slot %d: %w", slot, err)
		}

		// Add all files from this slot to results
		for _, res := range result {
			// Include timestamp range information in the result
			timestampRange := slotTimestampRanges[slot]
			allResults = append(allResults, TraceFileResult{
				FileName:     res.FileName,
				RecordCount:  res.RecordCount,
				FileSize:     res.FileSize,
				SlotID:       slot,
				MinTimestamp: timestampRange.MinTimestamp,
				MaxTimestamp: timestampRange.MaxTimestamp,
			})
		}
	}

	if len(allResults) == 0 {
		return nil, fmt.Errorf("no records written to any slot")
	}

	return allResults, nil
}

// SlotTimestampRange tracks the min and max timestamps for a slot
type SlotTimestampRange struct {
	MinTimestamp int64
	MaxTimestamp int64
}

// TraceFileResult contains information about a converted trace file
type TraceFileResult struct {
	FileName     string
	RecordCount  int64
	FileSize     int64
	SlotID       int
	MinTimestamp int64
	MaxTimestamp int64
}
