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

package cmd

import (
	"time"

	"github.com/google/uuid"
)

// IngestItem represents a work item for ingestion processing.
// This replaces lrdb.Inqueue and contains only the essential fields needed for processing.
type IngestItem struct {
	OrganizationID uuid.UUID `json:"organization_id"`
	InstanceNum    int16     `json:"instance_num"`
	Bucket         string    `json:"bucket"`
	ObjectID       string    `json:"object_id"`
	Signal         string    `json:"signal"`
	FileSize       int64     `json:"file_size"`
	QueuedAt       time.Time `json:"queued_at"`
}
