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
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionWorkQueuer defines the interface for queuing metric compaction work
type CompactionWorkQueuer interface {
	PutMetricCompactionWork(ctx context.Context, arg lrdb.PutMetricCompactionWorkParams) error
}

// QueueMetricCompaction queues compaction work for a specific segment
func QueueMetricCompaction(ctx context.Context, mdb CompactionWorkQueuer, organizationID uuid.UUID, dateint int32, frequencyMs int32, instanceNum int16, segmentID int64, recordCount int64, startTs int64, endTs int64) error {
	priority := GetCompactionPriority(frequencyMs)

	startTime := time.UnixMilli(startTs).UTC()
	endTime := time.UnixMilli(endTs).UTC()
	tsRange := helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange()

	err := mdb.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: organizationID,
		Dateint:        dateint,
		FrequencyMs:    int64(frequencyMs),
		SegmentID:      segmentID,
		InstanceNum:    instanceNum,
		TsRange:        tsRange,
		RecordCount:    recordCount,
		Priority:       priority,
	})

	if err != nil {
		return fmt.Errorf("failed to queue metric compaction work: %w", err)
	}

	return nil
}
