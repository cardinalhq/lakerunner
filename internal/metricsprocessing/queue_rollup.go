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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// RollupWorkQueuer defines the interface for queuing metric rollup work
type RollupWorkQueuer interface {
	PutMetricRollupWork(ctx context.Context, arg lrdb.PutMetricRollupWorkParams) error
}

// QueueMetricRollup queues rollup work for the next frequency level
func QueueMetricRollup(ctx context.Context, mdb RollupWorkQueuer, organizationID uuid.UUID, dateint int32, frequencyMs int32, instanceNum int16, slotID int32, slotCount int32, startTs int64, endTs int64) error {
	nextFrequency, exists := RollupTo[frequencyMs]
	if !exists {
		return nil
	}

	priority := GetRollupPriority(nextFrequency)

	err := mdb.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: organizationID,
		Dateint:        dateint,
		FrequencyMs:    int64(frequencyMs),
		InstanceNum:    instanceNum,
		SlotID:         slotID,
		SlotCount:      slotCount,
		Priority:       priority,
	})

	if err != nil {
		return fmt.Errorf("failed to queue metric rollup work: %w", err)
	}

	return nil
}
