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

package workqueue

import "github.com/google/uuid"

// WorkItem represents a work queue item independent of database schema
type WorkItem struct {
	ID       int64
	WorkerID int64
}

// InqueueItem represents an inqueue work item independent of database schema
type InqueueItem struct {
	ID             uuid.UUID
	OrganizationID uuid.UUID
	Bucket         string
	ObjectID       string
	ClaimedBy      int64
	Tries          int32
	InstanceNum    int16
}
