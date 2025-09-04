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
	"fmt"
	"os"
	"path/filepath"

	oteltranslate "github.com/cardinalhq/oteltools/pkg/translate"
	mapset "github.com/deckarep/golang-set/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/fileconv/proto"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// ConvertProtoFile converts a protobuf file to the standardized format
func ConvertProtoFile(tmpfilename, tmpdir, bucket, objectID string, rpfEstimate int64, dateint int32, orgID string) ([]TraceFileResult, error) {
	// Create a mapper for protobuf files
	mapper := translate.NewMapper()

	data, err := os.ReadFile(tmpfilename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", tmpfilename, err)
	}

	unmarshaler := &ptrace.ProtoUnmarshaler{}
	traces, err := unmarshaler.UnmarshalTraces(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf traces: %w", err)
	}

	// Pass 1: traverse reader to build complete schema
	r, err := proto.NewTracesProtoReaderFromTraces(&traces, mapper, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create traces proto reader: %w", err)
	}

	nmb := buffet.NewNodeMapBuilder()

	// Add base items to schema builder
	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   helpers.GetFileType(objectID),
	}

	// Add base items to schema
	for k, v := range baseitems {
		if err := nmb.Add(map[string]any{k: v}); err != nil {
			return nil, fmt.Errorf("failed to add base item to schema: %w", err)
		}
	}

	// Read all rows once to build complete schema
	for {
		row, done, err := r.GetRow()
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("failed to get row from traces reader: %w", err)
		}
		if done {
			break
		}

		for k, v := range baseitems {
			row[k] = v
		}

		if err := nmb.Add(row); err != nil {
			r.Close()
			return nil, fmt.Errorf("failed to add row to schema: %w", err)
		}
	}

	r.Close()

	// Build complete schema
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

	// Second pass: stream rows directly into slot writers
	r2, err := proto.NewTracesProtoReaderFromTraces(&traces, mapper, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create traces proto reader: %w", err)
	}
	defer r2.Close()

	fingerprints := mapset.NewSet[int64]()

	for {
		row, done, err := r2.GetRow()
		if err != nil {
			return nil, fmt.Errorf("failed to get row from traces reader: %w", err)
		}
		if done {
			break
		}

		for k, v := range baseitems {
			row[k] = v
		}

		traceID, ok := row[oteltranslate.CardinalFieldSpanTraceID].(string)
		if !ok {
			traceID = "unknown"
		}

		timestamp, ok := row[oteltranslate.CardinalFieldTimestamp].(int64)
		if !ok {
			continue
		}

		// Collect fingerprints for this row
		rowTagValues := make(map[string]mapset.Set[string])
		for tagName, tagValue := range row {
			var tagValueStr string
			switch v := tagValue.(type) {
			case string:
				tagValueStr = v
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				tagValueStr = fmt.Sprintf("%d", v)
			case float32, float64:
				tagValueStr = fmt.Sprintf("%f", v)
			default:
				tagValueStr = fmt.Sprintf("%v", v)
			}

			if _, exists := rowTagValues[tagName]; !exists {
				rowTagValues[tagName] = mapset.NewSet[string]()
			}
			rowTagValues[tagName].Add(tagValueStr)
		}

		rowFingerprints := buffet.ToFingerprints(rowTagValues)
		rowFingerprints.Each(func(fp int64) bool {
			fingerprints.Add(fp)
			return false
		})

		slot := DetermineTraceSlot(traceID, dateint, orgID)

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

		writer, exists := writers[slot]
		if !exists {
			return nil, fmt.Errorf("writer for slot %d not found", slot)
		}

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
				Fingerprints: res.Fingerprints,
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
	Fingerprints mapset.Set[int64]
}
