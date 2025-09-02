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

package compaction

import (
	"context"
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

type compactionStore interface {
	s3helper.ObjectCleanupStore
	ClaimCompactionBundle(ctx context.Context, p lrdb.BundleParams) (lrdb.CompactionBundleResult, error)
	McqCompleteDelete(ctx context.Context, arg lrdb.McqCompleteDeleteParams) error
	McqDeferKey(ctx context.Context, arg lrdb.McqDeferKeyParams) error
	McqHeartbeat(ctx context.Context, arg lrdb.McqHeartbeatParams) error
	McqGetSegmentsByIds(ctx context.Context, segmentIds []int64) ([]lrdb.MetricSeg, error)
	CompactMetricSegs(ctx context.Context, args lrdb.CompactMetricSegsParams) error
	GetMetricSegsForCompactionWork(ctx context.Context, params lrdb.GetMetricSegsForCompactionWorkParams) ([]lrdb.MetricSeg, error)
	MarkMetricSegsCompactedByKeys(ctx context.Context, arg lrdb.MarkMetricSegsCompactedByKeysParams) error
	SetMetricSegCompacted(ctx context.Context, arg lrdb.SetMetricSegCompactedParams) error
}

type config struct {
	OverFactor  float64
	BatchLimit  int32
	Grace       time.Duration
	DeferBase   time.Duration
	Jitter      time.Duration
	MaxAttempts int
}

func GetConfigFromEnv() config {
	overFactor := 1.20
	if env := os.Getenv("LAKERUNNER_METRIC_COMPACTION_OVER_FACTOR"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil && val > 1.0 {
			overFactor = val
		}
	}

	batchLimit := int32(100)
	if env := os.Getenv("LAKERUNNER_METRIC_COMPACTION_BATCH_LIMIT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			batchLimit = int32(val)
		}
	}

	grace := 5 * time.Minute
	if env := os.Getenv("LAKERUNNER_METRIC_COMPACTION_GRACE_MINUTES"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			grace = time.Duration(val) * time.Minute
		}
	}

	deferBase := 30 * time.Second
	if env := os.Getenv("LAKERUNNER_METRIC_COMPACTION_DEFER_SECONDS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			deferBase = time.Duration(val) * time.Second
		}
	}

	maxAttempts := 5
	if env := os.Getenv("LAKERUNNER_METRIC_COMPACTION_MAX_ATTEMPTS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			maxAttempts = val
		}
	}

	return config{
		OverFactor:  overFactor,
		BatchLimit:  batchLimit,
		Grace:       grace,
		DeferBase:   deferBase,
		Jitter:      10 * time.Second, // Fixed jitter
		MaxAttempts: maxAttempts,
	}
}

type Manager struct {
	db         compactionStore
	workerID   int64
	config     config
	ll         *slog.Logger
	sp         storageprofile.StorageProfileProvider
	awsmanager *awsclient.Manager
}

func NewManager(
	db compactionStore,
	workerID int64,
	config config,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
) *Manager {
	return &Manager{
		db:         db,
		workerID:   workerID,
		config:     config,
		ll:         slog.Default().With(slog.String("component", "metric-compaction-manager")),
		sp:         sp,
		awsmanager: awsmanager,
	}
}

// ClaimWork returns the bundle and estimated target records for writers
func (m *Manager) ClaimWork(ctx context.Context) (*lrdb.CompactionBundleResult, error) {
	bundleParams := lrdb.BundleParams{
		WorkerID:    m.workerID,
		OverFactor:  m.config.OverFactor,
		BatchLimit:  m.config.BatchLimit,
		Grace:       m.config.Grace,
		DeferBase:   m.config.DeferBase,
		Jitter:      m.config.Jitter,
		MaxAttempts: m.config.MaxAttempts,
	}

	bundle, err := m.db.ClaimCompactionBundle(ctx, bundleParams)
	if err != nil {
		return nil, fmt.Errorf("failed to claim compaction bundle: %w", err)
	}

	if len(bundle.Items) > 0 {
		m.ll.Info("Claimed compaction work bundle",
			slog.Int("workItems", len(bundle.Items)),
			slog.Int64("estimatedTarget", bundle.EstimatedTarget))
	}

	return &bundle, nil
}

func (m *Manager) CompleteWork(ctx context.Context, items []lrdb.McqFetchCandidatesRow) error {
	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}

	if err := m.db.McqCompleteDelete(ctx, lrdb.McqCompleteDeleteParams{
		WorkerID: m.workerID,
		Ids:      ids,
	}); err != nil {
		m.ll.Error("Failed to complete work items",
			slog.Int("count", len(items)),
			slog.Any("error", err))
		return fmt.Errorf("failed to complete work items: %w", err)
	}
	return nil
}

func (m *Manager) FailWork(ctx context.Context, items []lrdb.McqFetchCandidatesRow) error {
	if len(items) == 0 {
		return nil
	}

	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}

	// Delete the failed work items from the queue
	// They will be re-queued naturally when new segments arrive that need compaction
	err := m.db.McqCompleteDelete(ctx, lrdb.McqCompleteDeleteParams{
		WorkerID: m.workerID,
		Ids:      ids,
	})

	if err != nil {
		m.ll.Error("Failed to delete failed work items",
			slog.Int("count", len(items)),
			slog.Any("error", err))
		return fmt.Errorf("failed to delete failed work items: %w", err)
	}

	m.ll.Warn("Deleted failed work items from queue",
		slog.Int("count", len(items)))

	return nil
}

// Run starts the compaction loop using the manager's dependencies
func (m *Manager) Run(ctx context.Context) error {
	return runLoop(ctx, m, m.db, m.sp, m.awsmanager)
}
