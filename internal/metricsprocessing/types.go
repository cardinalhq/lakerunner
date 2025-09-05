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
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

// ProcessingInput contains all inputs needed for the generic processing pipeline
type ProcessingInput struct {
	ReaderStack       *ReaderStackResult
	TargetFrequencyMs int32 // Same as source for compact, next level for rollup
	TmpDir            string
	RecordsLimit      int64  // Maximum records per file
	EstimatedRecords  int64  // Estimated target records for optimization
	Action            string // "compact" or "rollup" for metrics
	InputRecords      int64  // Total input records for metrics tracking
	InputBytes        int64  // Total input bytes for metrics tracking
}

// ProcessingResult contains the output of the processing pipeline
type ProcessingResult struct {
	RawResults []parquetwriter.Result // Raw parquet results before conversion
	Segments   ProcessedSegments      // Processed segments (may be empty until processed)
	Stats      ProcessingStats        // Statistics about the processing
}

// ProcessingStats contains statistics about the processing
type ProcessingStats struct {
	InputSegments    int
	OutputSegments   int
	InputRecords     int64
	OutputRecords    int64
	InputBytes       int64
	OutputBytes      int64
	CompressionRatio float64
	TotalRows        int64
	BatchCount       int
}

// ProcessingContext contains metadata about the work being processed
type ProcessingContext struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32 // Source frequency
	InstanceNum    int16
	SlotID         int32
	SlotCount      int32
	BatchID        string
	WorkItemIDs    []int64
}
