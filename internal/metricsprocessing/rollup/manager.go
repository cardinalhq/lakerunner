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

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type rollupStore interface {
	s3helper.ObjectCleanupStore
	ClaimRollupBundle(ctx context.Context, params lrdb.BundleParams) (*lrdb.RollupBundleResult, error)
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
	TargetRecords int64
	OverFactor    float64
	BatchLimit    int32
	Grace         time.Duration
	DeferBase     time.Duration
	MaxAttempts   int
}

func GetConfigFromEnv() config {
	targetRecords := int64(40000) // Default max records for rollup batches
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_TARGET_RECORDS"); env != "" {
		if val, err := strconv.ParseInt(env, 10, 64); err == nil && val > 0 {
			targetRecords = val
		}
	}

	overFactor := 1.2
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_OVER_FACTOR"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil && val > 1.0 {
			overFactor = val
		}
	}

	batchLimit := int32(100)
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_BATCH_LIMIT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			batchLimit = int32(val)
		}
	}

	grace := time.Hour
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_GRACE"); env != "" {
		if val, err := time.ParseDuration(env); err == nil && val > 0 {
			grace = val
		}
	}

	deferBase := 5 * time.Minute
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_DEFER_BASE"); env != "" {
		if val, err := time.ParseDuration(env); err == nil && val > 0 {
			deferBase = val
		}
	}

	maxAttempts := 5
	if env := os.Getenv("LAKERUNNER_METRIC_ROLLUP_MAX_ATTEMPTS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			maxAttempts = val
		}
	}

	return config{
		TargetRecords: targetRecords,
		OverFactor:    overFactor,
		BatchLimit:    batchLimit,
		Grace:         grace,
		DeferBase:     deferBase,
		MaxAttempts:   maxAttempts,
	}
}

type Manager struct {
	db         rollupStore
	workerID   int64
	config     config
	ll         *slog.Logger
	sp         storageprofile.StorageProfileProvider
	awsmanager *awsclient.Manager
}

func NewManager(
	db rollupStore,
	workerID int64,
	config config,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
) *Manager {
	return &Manager{
		db:         db,
		workerID:   workerID,
		config:     config,
		ll:         slog.Default().With(slog.String("component", "metric-rollup-manager")),
		sp:         sp,
		awsmanager: awsmanager,
	}
}

func (m *Manager) ClaimWork(ctx context.Context) (*lrdb.RollupBundleResult, error) {
	// Special case: when batch limit is 1, just grab the first available work item
	if m.config.BatchLimit == 1 {
		row, err := m.db.MrqClaimSingleRow(ctx, lrdb.MrqClaimSingleRowParams{
			WorkerID: m.workerID,
			Now:      time.Now(),
		})
		if err != nil {
			// sql.ErrNoRows means no work available, which is not an error
			if errors.Is(err, sql.ErrNoRows) {
				return &lrdb.RollupBundleResult{Items: nil}, nil
			}
			return nil, fmt.Errorf("failed to claim single rollup row: %w", err)
		}

		// Convert the single row to a RollupBundleResult
		item := lrdb.MrqFetchCandidatesRow(row)

		result := &lrdb.RollupBundleResult{
			Items:           []lrdb.MrqFetchCandidatesRow{item},
			EstimatedTarget: row.RecordCount, // Use the actual record count as the estimate
		}

		m.ll.Info("Claimed single rollup row",
			slog.Int64("id", row.ID),
			slog.Int64("recordCount", row.RecordCount))

		return result, nil
	}

	// Normal bundling logic for batch limit > 1
	result, err := m.db.ClaimRollupBundle(ctx, lrdb.BundleParams{
		WorkerID:      m.workerID,
		TargetRecords: m.config.TargetRecords,
		OverFactor:    m.config.OverFactor,
		BatchLimit:    m.config.BatchLimit,
		Grace:         m.config.Grace,
		DeferBase:     m.config.DeferBase,
		MaxAttempts:   m.config.MaxAttempts,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to claim metric rollup bundle: %w", err)
	}

	if result != nil && len(result.Items) > 0 {
		m.ll.Info("Claimed metric rollup bundle",
			slog.Int("workItems", len(result.Items)),
			slog.Int64("estimatedTarget", result.EstimatedTarget))
	}

	return result, nil
}

func (m *Manager) CompleteWork(ctx context.Context, rows []lrdb.MrqFetchCandidatesRow) error {
	if len(rows) == 0 {
		return nil
	}
	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	if err := m.db.CompleteRollup(ctx, m.workerID, ids); err != nil {
		m.ll.Error("Failed to complete work items",
			slog.Any("ids", ids),
			slog.Any("error", err))
		return fmt.Errorf("failed to complete work items: %w", err)
	}
	return nil
}

func (m *Manager) ReleaseWork(ctx context.Context, rows []lrdb.MrqFetchCandidatesRow) error {
	if len(rows) == 0 {
		return nil
	}

	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}

	if err := m.db.MrqRelease(ctx, lrdb.MrqReleaseParams{
		WorkerID: m.workerID,
		Ids:      ids,
	}); err != nil {
		m.ll.Error("Failed to release work items",
			slog.Int("count", len(rows)),
			slog.Any("error", err))
		return fmt.Errorf("failed to release work items: %w", err)
	}

	m.ll.Info("Released work items back to queue for retry",
		slog.Int("count", len(rows)))

	return nil
}

func (m *Manager) FailWork(ctx context.Context, rows []lrdb.MrqFetchCandidatesRow) error {
	if len(rows) == 0 {
		return nil
	}

	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}

	// Delete the failed work items from the queue
	// They will be re-queued naturally when new work needs to be done
	err := m.db.CompleteRollup(ctx, m.workerID, ids)

	if err != nil {
		m.ll.Error("Failed to delete failed work items",
			slog.Int("count", len(rows)),
			slog.Any("error", err))
		return fmt.Errorf("failed to delete failed work items: %w", err)
	}

	m.ll.Warn("Deleted failed work items from queue",
		slog.Int("count", len(rows)))

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	return runLoop(ctx, m, m.db, m.sp, m.awsmanager)
}
