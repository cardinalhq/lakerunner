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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionStore is the interface for database operations needed by compaction
type CompactionStore interface {
	cloudstorage.ObjectCleanupStore
	GetMetricSegByPrimaryKey(ctx context.Context, arg lrdb.GetMetricSegByPrimaryKeyParams) (lrdb.MetricSeg, error)
	CompactMetricSegs(ctx context.Context, args lrdb.CompactMetricSegsParams) error
	CompactMetricSegsWithKafkaOffsets(ctx context.Context, params lrdb.CompactMetricSegsParams, kafkaOffsets []lrdb.KafkaOffsetUpdate) error // New atomic transaction
	SetMetricSegCompacted(ctx context.Context, arg lrdb.SetMetricSegCompactedParams) error
	SetSingleMetricSegCompacted(ctx context.Context, arg lrdb.SetSingleMetricSegCompactedParams) error     // For single segment with full PK
	MarkMetricSegsCompactedByKeys(ctx context.Context, arg lrdb.MarkMetricSegsCompactedByKeysParams) error // For batch marking
	MrqQueueWork(ctx context.Context, arg lrdb.MrqQueueWorkParams) error                                   // For queueing rollup work
	// Kafka offset tracking
	KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error)
	KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error
	// Metric estimates
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// Config holds configuration for compaction
type Config struct {
	TargetFileSizeBytes int64 // Target file size in bytes for compaction
}

func ConfigFromViper(cfg *config.CompactionConfig) Config {
	targetSize := cfg.TargetFileSizeBytes
	if targetSize == 0 {
		targetSize = 1024 * 1024 // Default to 1MB if not configured
	}
	return Config{
		TargetFileSizeBytes: targetSize,
	}
}

type Manager struct {
	db     CompactionStore
	config Config
	sp     storageprofile.StorageProfileProvider
	cmgr   cloudstorage.ClientProvider
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

// GetConfig returns the compaction configuration
func (m *Manager) GetConfig() Config {
	return m.config
}

func NewManager(db CompactionStore, cfg *config.CompactionConfig, sp storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider) *Manager {
	return &Manager{
		db:     db,
		config: ConfigFromViper(cfg),
		sp:     sp,
		cmgr:   cmgr,
	}
}
