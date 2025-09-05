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

package cloudstorage

import (
	"context"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// Object cleanup utilities

// ObjectCleanupStore provides the minimal interface needed for scheduling S3 object deletions
type ObjectCleanupStore interface {
	ObjectCleanupAdd(ctx context.Context, arg lrdb.ObjectCleanupAddParams) error
}

// ScheduleS3Delete schedules an S3 object for deletion by adding it to the cleanup queue
func ScheduleS3Delete(ctx context.Context, mdb ObjectCleanupStore, orgID uuid.UUID, instanceNum int16, bucketID, objectID string) error {
	return mdb.ObjectCleanupAdd(ctx, lrdb.ObjectCleanupAddParams{
		OrganizationID: orgID,
		BucketID:       bucketID,
		ObjectID:       objectID,
		InstanceNum:    instanceNum,
	})
}
