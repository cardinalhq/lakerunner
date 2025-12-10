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

// Sort version constants for log segments
const (
	// LogSortVersionUnknown indicates the file's sort order is unknown or unsorted (legacy files)
	LogSortVersionUnknown = 0
	// LogSortVersionTimestamp indicates the file is sorted by [timestamp] only (legacy)
	LogSortVersionTimestamp = 1
	// LogSortVersionServiceFingerprintTimestamp indicates the file is sorted by
	// [resource_customer_domain OR resource_service_name, fingerprint, timestamp]
	LogSortVersionServiceFingerprintTimestamp = 2
)

// Current log sort configuration - single source of truth for all log sorting
const (
	// CurrentLogSortVersion is the sort version used for all newly created log segments
	CurrentLogSortVersion = LogSortVersionServiceFingerprintTimestamp
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
	StreamIds    []string
	SortVersion  int16
}
