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

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
)

// ConvertIngestItemsToInqueue converts IngestItems to lrdb.Inqueue for compatibility
// with existing processing code. This is a temporary adapter until all code is updated.
func ConvertIngestItemsToInqueue(items []IngestItem) []lrdb.Inqueue {
	result := make([]lrdb.Inqueue, len(items))
	for i, item := range items {
		result[i] = lrdb.Inqueue{
			ID:             uuid.New(), // Generate a temporary ID for processing
			QueueTs:        item.QueuedAt,
			OrganizationID: item.OrganizationID,
			InstanceNum:    item.InstanceNum,
			Bucket:         item.Bucket,
			ObjectID:       item.ObjectID,
			Signal:         item.Signal,
			FileSize:       item.FileSize,
			// These fields are not used in processing but need to be set
			Priority:      0,
			CollectorName: "",
			Tries:         0,
			ClaimedBy:     0,
			ClaimedAt:     nil,
			HeartbeatedAt: nil,
			EligibleAt:    time.Now(),
		}
	}
	return result
}
