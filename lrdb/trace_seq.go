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

package lrdb

import (
	"context"

	"github.com/google/uuid"
)

// CompactTraceSegsParams defines the parameters for trace segment compaction with Kafka offsets
type CompactTraceSegsParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	IngestDateint  int32
	InstanceNum    int16
	NewRecords     []CompactTraceSegsNew
	OldRecords     []CompactTraceSegsOld
	CreatedBy      CreatedBy
}

// CompactTraceSegsOld represents an old trace segment to be marked as compacted
type CompactTraceSegsOld struct {
	SegmentID int64
}

// CompactTraceSegsNew represents a new compacted trace segment to be inserted
type CompactTraceSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
	LabelNameMap []byte
}

func (q *Store) InsertTraceSegment(ctx context.Context, params InsertTraceSegmentParams) error {
	if err := q.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.insertTraceSegmentDirect(ctx, params)
}
