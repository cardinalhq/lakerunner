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
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/idgen"
)

func (q *Queries) ensureLogFPPartition(ctx context.Context, parent string, org_id uuid.UUID, dateint int32) error {
	partitionTableName := parent + "_" + idgen.UUIDToBase36(org_id)
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

func (q *Queries) ensureTraceFPPartition(ctx context.Context, parent string, org_id uuid.UUID, dateint int32) error {
	partitionTableName := parent + "_" + idgen.UUIDToBase36(org_id)
	key := fmt.Sprintf("%s_%d", partitionTableName, dateint)
	if IsPartitionTableRemembered(key) {
		return nil
	}
	sql := fmt.Sprintf(`SELECT create_tracefpseg_partition('%s', '%s', '%s', %d, %d)`,
		parent, partitionTableName, org_id.String(), dateint, dateint+1)
	_, err := q.db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create partition %s for table %s: %w", partitionTableName, parent, err)
	}
	RememberPartitionTable(key)
	return nil
}

func (q *Queries) ensureMetricSegmentPartition(ctx context.Context, org_id uuid.UUID, dateint int32) error {
	partitionTableName := "mseg_" + idgen.UUIDToBase36(org_id)
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
