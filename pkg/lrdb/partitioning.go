// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lrdb

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/cardinalhq/nestbox/dbase"
)

func (q *Queries) ensureLogFPPartition(ctx context.Context, parent string, org_id uuid.UUID, dateint int32) error {
	partitionTableName := parent + "_" + dbase.UUIDToBase36(org_id)
	key := fmt.Sprintf("%s_%d", partitionTableName, dateint)
	if IsPartitionTableRemembered(key) {
		return nil
	}
	sql := fmt.Sprintf(`SELECT create_logfpseg_partition('%s', '%s', '%s', %d, %d)`,
		parent, partitionTableName, org_id.String(), dateint, dateint+1)
	_, err := q.db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create partition %s for table %s: %w", partitionTableName, parent, err)
	}
	RememberPartitionTable(key)
	return nil
}

func (q *Queries) ensureMetricSegmentPartition(ctx context.Context, org_id uuid.UUID, dateint int32) error {
	partitionTableName := "mseg_" + dbase.UUIDToBase36(org_id)
	key := fmt.Sprintf("%s_%d", partitionTableName, dateint)
	if IsPartitionTableRemembered(key) {
		return nil
	}
	sql := fmt.Sprintf(`SELECT create_metricseg_partition('metric_seg', '%s', '%s', %d, %d)`,
		partitionTableName, org_id.String(), dateint, dateint+1)
	_, err := q.db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create partition %s for table metric_seg: %w", partitionTableName, err)
	}
	RememberPartitionTable(key)
	return nil
}
