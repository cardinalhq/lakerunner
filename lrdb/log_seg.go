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

package lrdb

import (
	"github.com/google/uuid"
)

// CompactLogSegsParams defines the parameters for log segment compaction with Kafka offsets
type CompactLogSegsParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	IngestDateint  int32
	InstanceNum    int16
	NewRecords     []CompactLogSegsNew
	OldRecords     []CompactLogSegsOld
	CreatedBy      CreatedBy
}

// CompactLogSegsOld represents an old log segment to be marked as compacted
type CompactLogSegsOld struct {
	SegmentID int64
}

// CompactLogSegsNew represents a new compacted log segment to be inserted
type CompactLogSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
	LabelNameMap []byte
}
