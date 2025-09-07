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
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionStore is the interface for database operations needed by compaction
type CompactionStore interface {
	cloudstorage.ObjectCleanupStore
	ClaimCompactionBundle(ctx context.Context, p lrdb.BundleParams) (lrdb.CompactionBundleResult, error)
	McqCompleteDelete(ctx context.Context, arg lrdb.McqCompleteDeleteParams) error
	McqDeferItems(ctx context.Context, arg lrdb.McqDeferItemsParams) error
	McqHeartbeat(ctx context.Context, arg lrdb.McqHeartbeatParams) (int64, error)
	McqRelease(ctx context.Context, arg lrdb.McqReleaseParams) error
	GetMetricSegByPrimaryKey(ctx context.Context, arg lrdb.GetMetricSegByPrimaryKeyParams) (lrdb.MetricSeg, error)
	GetMetricSegsByIds(ctx context.Context, arg lrdb.GetMetricSegsByIdsParams) ([]lrdb.MetricSeg, error)
	CompactMetricSegs(ctx context.Context, args lrdb.CompactMetricSegsParams) error
	MarkMetricSegsCompactedByKeys(ctx context.Context, arg lrdb.MarkMetricSegsCompactedByKeysParams) error
	SetMetricSegCompacted(ctx context.Context, arg lrdb.SetMetricSegCompactedParams) error
	MrqQueueWork(ctx context.Context, arg lrdb.MrqQueueWorkParams) error // For queueing rollup work
	// Kafka offset tracking
	KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error)
	KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error
}

type Config struct {
	OverFactor  float64
	BatchLimit  int32
	Grace       time.Duration
	DeferBase   time.Duration
	Jitter      time.Duration
	MaxAttempts int
}

func ConfigFromViper(cfg *config.CompactionConfig) Config {
	overFactor := cfg.OverFactor
	if overFactor <= 1.0 {
		overFactor = 1.20
	}

	batchLimit := int32(cfg.BatchLimit)
	if batchLimit <= 0 {
		batchLimit = 100
	}

	grace := time.Duration(cfg.GraceMinutes) * time.Minute
	if grace <= 0 {
		grace = 5 * time.Minute
	}

	deferBase := time.Duration(cfg.DeferSeconds) * time.Second
	if deferBase <= 0 {
		deferBase = 30 * time.Second
	}

	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	return Config{
		OverFactor:  overFactor,
		BatchLimit:  batchLimit,
		Grace:       grace,
		DeferBase:   deferBase,
		Jitter:      10 * time.Second, // Fixed jitter
		MaxAttempts: maxAttempts,
	}
}

// GetConfigFromEnv is deprecated - use ConfigFromViper with config.Load() instead
// This function is kept for backward compatibility
func GetConfigFromEnv() Config {
	cfg, err := config.Load()
	if err != nil {
		// Fall back to defaults if config loading fails
		return Config{
			OverFactor:  1.20,
			BatchLimit:  100,
			Grace:       5 * time.Minute,
			DeferBase:   30 * time.Second,
			Jitter:      10 * time.Second,
			MaxAttempts: 5,
		}
	}
	return ConfigFromViper(&cfg.Metrics.Compaction)
}

type Manager struct {
	db       CompactionStore
	workerID int64
	config   Config
	sp       storageprofile.StorageProfileProvider
	cmgr     cloudstorage.ClientProvider
}

// GetDB returns the database store
func (m *Manager) GetDB() CompactionStore {
	return m.db
}

// GetStorageProfileProvider returns the storage profile provider
func (m *Manager) GetStorageProfileProvider() storageprofile.StorageProfileProvider {
	return m.sp
}

// GetCloudManager returns the cloud storage client provider
func (m *Manager) GetCloudManager() cloudstorage.ClientProvider {
	return m.cmgr
}

func NewManager(db CompactionStore, workerID int64, cfg *config.CompactionConfig, sp storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider) *Manager {
	return &Manager{
		db:       db,
		workerID: workerID,
		config:   ConfigFromViper(cfg),
		sp:       sp,
		cmgr:     cmgr,
	}
}

// ClaimWork returns the bundle and estimated target records for writers
func (m *Manager) ClaimWork(ctx context.Context) (*lrdb.CompactionBundleResult, error) {
	ll := logctx.FromContext(ctx)

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
		ll.Info("Claimed compaction work bundle",
			slog.Int("workItems", len(bundle.Items)),
			slog.Int64("estimatedTarget", bundle.EstimatedTarget))
	}

	return &bundle, nil
}

func (m *Manager) CompleteWork(ctx context.Context, items []lrdb.McqFetchCandidatesRow) error {
	ll := logctx.FromContext(ctx)

	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}

	if err := m.db.McqCompleteDelete(ctx, lrdb.McqCompleteDeleteParams{
		WorkerID: m.workerID,
		Ids:      ids,
	}); err != nil {
		ll.Error("Failed to complete work items",
			slog.Int("count", len(items)),
			slog.Any("error", err))
		return fmt.Errorf("failed to complete work items: %w", err)
	}
	return nil
}

func (m *Manager) ReleaseWork(ctx context.Context, items []lrdb.McqFetchCandidatesRow) error {
	ll := logctx.FromContext(ctx)

	if len(items) == 0 {
		return nil
	}

	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.ID
	}

	if err := m.db.McqRelease(ctx, lrdb.McqReleaseParams{
		WorkerID: m.workerID,
		Ids:      ids,
	}); err != nil {
		ll.Error("Failed to release work items",
			slog.Int("count", len(items)),
			slog.Any("error", err))
		return fmt.Errorf("failed to release work items: %w", err)
	}

	ll.Info("Released work items back to queue for retry",
		slog.Int("count", len(items)))

	return nil
}

func (m *Manager) FailWork(ctx context.Context, items []lrdb.McqFetchCandidatesRow) error {
	ll := logctx.FromContext(ctx)

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
		ll.Error("Failed to delete failed work items",
			slog.Int("count", len(items)),
			slog.Any("error", err))
		return fmt.Errorf("failed to delete failed work items: %w", err)
	}

	ll.Warn("Deleted failed work items from queue",
		slog.Int("count", len(items)))

	return nil
}

// Run starts the compaction loop using the manager's dependencies
func (m *Manager) Run(ctx context.Context) error {
	return runLoop(ctx, m, m.db, m.sp, m.cmgr)
}
