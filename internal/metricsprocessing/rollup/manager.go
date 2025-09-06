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

package rollup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type rollupStore interface {
	cloudstorage.ObjectCleanupStore
	MrqClaimBatch(ctx context.Context, arg lrdb.MrqClaimBatchParams) ([]lrdb.MrqClaimBatchRow, error)
	CompleteRollup(ctx context.Context, workerID int64, ids []int64) error
	MrqHeartbeat(ctx context.Context, arg lrdb.MrqHeartbeatParams) (int64, error)
	MrqRelease(ctx context.Context, arg lrdb.MrqReleaseParams) error
	GetMetricSegsByIds(ctx context.Context, params lrdb.GetMetricSegsByIdsParams) ([]lrdb.MetricSeg, error)
	RollupMetricSegs(ctx context.Context, sourceParams lrdb.RollupSourceParams, targetParams lrdb.RollupTargetParams, sourceSegmentIDs []int64, newRecords []lrdb.RollupNewRecord) error
	McqQueueWork(ctx context.Context, arg lrdb.McqQueueWorkParams) error
	MrqQueueWork(ctx context.Context, arg lrdb.MrqQueueWorkParams) error
	MrqClaimSingleRow(ctx context.Context, arg lrdb.MrqClaimSingleRowParams) (lrdb.MrqClaimSingleRowRow, error)
}

type config struct {
	BatchLimit int32
}

func GetConfigFromEnv() config {
	batchLimit := int32(10)
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_BATCH_LIMIT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			batchLimit = int32(val)
		}
	}

	return config{
		BatchLimit: batchLimit,
	}
}

type Manager struct {
	db       rollupStore
	workerID int64
	config   config
	sp       storageprofile.StorageProfileProvider
	cmgr     *cloudstorage.CloudManagers
}

func NewManager(db rollupStore, workerID int64, config config, sp storageprofile.StorageProfileProvider, cmgr *cloudstorage.CloudManagers) *Manager {
	return &Manager{
		db:       db,
		workerID: workerID,
		config:   config,
		sp:       sp,
		cmgr:     cmgr,
	}
}

func (m *Manager) ClaimWork(ctx context.Context) ([]lrdb.MrqClaimBatchRow, error) {
	ll := logctx.FromContext(ctx)

	// Special case: when batch limit is 1, use the single row claim for compatibility
	if m.config.BatchLimit == 1 {
		row, err := m.db.MrqClaimSingleRow(ctx, lrdb.MrqClaimSingleRowParams{
			WorkerID: m.workerID,
			Now:      time.Now(),
		})
		if err != nil {
			// sql.ErrNoRows means no work available, which is not an error
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to claim single rollup row: %w", err)
		}

		// Convert the single row to a batch row
		item := lrdb.MrqClaimBatchRow(row)

		ll.Info("Claimed single rollup row",
			slog.Int64("id", row.ID),
			slog.Int64("recordCount", row.RecordCount))

		return []lrdb.MrqClaimBatchRow{item}, nil
	}

	// Claim a batch of work items
	rows, err := m.db.MrqClaimBatch(ctx, lrdb.MrqClaimBatchParams{
		WorkerID:   m.workerID,
		Now:        time.Now(),
		BatchLimit: m.config.BatchLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to claim metric rollup batch: %w", err)
	}

	if len(rows) > 0 {
		ll.Info("Claimed metric rollup batch",
			slog.Int("workItems", len(rows)))
	}

	return rows, nil
}

func (m *Manager) CompleteWork(ctx context.Context, ids []int64) error {
	ll := logctx.FromContext(ctx)

	if len(ids) == 0 {
		return nil
	}
	if err := m.db.CompleteRollup(ctx, m.workerID, ids); err != nil {
		ll.Error("Failed to complete work items",
			slog.Any("ids", ids),
			slog.Any("error", err))
		return fmt.Errorf("failed to complete work items: %w", err)
	}
	return nil
}

func (m *Manager) ReleaseWork(ctx context.Context, ids []int64) error {
	ll := logctx.FromContext(ctx)

	if len(ids) == 0 {
		return nil
	}

	if err := m.db.MrqRelease(ctx, lrdb.MrqReleaseParams{
		WorkerID: m.workerID,
		Ids:      ids,
	}); err != nil {
		ll.Error("Failed to release work items",
			slog.Int("count", len(ids)),
			slog.Any("error", err))
		return fmt.Errorf("failed to release work items: %w", err)
	}

	ll.Info("Released work items back to queue for retry",
		slog.Int("count", len(ids)))

	return nil
}

func (m *Manager) FailWork(ctx context.Context, ids []int64) error {
	ll := logctx.FromContext(ctx)

	if len(ids) == 0 {
		return nil
	}

	// Delete the failed work items from the queue
	// They will be re-queued naturally when new work needs to be done
	err := m.db.CompleteRollup(ctx, m.workerID, ids)

	if err != nil {
		ll.Error("Failed to delete failed work items",
			slog.Int("count", len(ids)),
			slog.Any("error", err))
		return fmt.Errorf("failed to delete failed work items: %w", err)
	}

	ll.Warn("Deleted failed work items from queue",
		slog.Int("count", len(ids)))

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	return runLoop(ctx, m, m.db, m.sp, m.cmgr)
}
