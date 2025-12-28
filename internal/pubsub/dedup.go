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

package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// DedupInserter defines the interface needed for deduplication
type DedupInserter interface {
	PubSubMessageHistoryInsert(ctx context.Context, params lrdb.PubSubMessageHistoryInsertParams) (pgconn.CommandTag, error)
	PubSubMessageHistoryInsertBatch(ctx context.Context, params lrdb.PubSubMessageHistoryInsertBatchParams) ([]lrdb.PubSubMessageHistoryInsertBatchRow, error)
}

// DedupItem represents an item to be deduplicated
type DedupItem struct {
	Bucket   string
	ObjectID string
}

// Deduplicator defines the interface for message deduplication
type Deduplicator interface {
	CheckAndRecord(ctx context.Context, bucket, objectID, source string) (bool, error)
	CheckAndRecordBatch(ctx context.Context, items []DedupItem, source string) ([]DedupItem, error)
}

// DatabaseDeduplicator handles message deduplication using database
type DatabaseDeduplicator struct {
	store DedupInserter
}

// NewDatabaseDeduplicator creates a new database deduplicator instance
func NewDatabaseDeduplicator(store DedupInserter) *DatabaseDeduplicator {
	return &DatabaseDeduplicator{
		store: store,
	}
}

// NewDeduplicator creates a new deduplicator instance (returns interface)
func NewDeduplicator(store DedupInserter) Deduplicator {
	return NewDatabaseDeduplicator(store)
}

// CheckAndRecord checks if a message is a duplicate and records it if not
// Returns true if the message should be processed (not a duplicate)
func (d *DatabaseDeduplicator) CheckAndRecord(ctx context.Context, bucket, objectID, source string) (bool, error) {
	// Try to insert into database - only succeeds if not a duplicate
	result, err := d.store.PubSubMessageHistoryInsert(ctx, lrdb.PubSubMessageHistoryInsertParams{
		Bucket:   bucket,
		ObjectID: objectID,
		Source:   source,
	})

	if err != nil {
		// Database error - fail closed (don't process message)
		slog.Error("Failed to check message deduplication",
			slog.Any("error", err),
			slog.String("bucket", bucket),
			slog.String("object_id", objectID))
		return false, fmt.Errorf("deduplication check failed: %w", err)
	}

	wasInserted := result.RowsAffected() > 0
	if wasInserted {
		slog.Debug("New message recorded in dedup table",
			slog.String("bucket", bucket),
			slog.String("object_id", objectID))
		return true, nil
	}

	slog.Info("Duplicate message detected, skipping",
		slog.String("bucket", bucket),
		slog.String("object_id", objectID),
		slog.String("source", source))
	recordDuplicate(ctx, bucket, source)
	return false, nil
}

// CheckAndRecordBatch checks multiple items for duplicates in a single database call.
// Returns the list of items that should be processed (not duplicates).
func (d *DatabaseDeduplicator) CheckAndRecordBatch(ctx context.Context, items []DedupItem, source string) ([]DedupItem, error) {
	if len(items) == 0 {
		return nil, nil
	}

	// Build arrays for batch insert
	buckets := make([]string, len(items))
	objectIDs := make([]string, len(items))
	sources := make([]string, len(items))
	for i, item := range items {
		buckets[i] = item.Bucket
		objectIDs[i] = item.ObjectID
		sources[i] = source
	}

	// Batch insert - returns only the rows that were actually inserted (non-duplicates)
	inserted, err := d.store.PubSubMessageHistoryInsertBatch(ctx, lrdb.PubSubMessageHistoryInsertBatchParams{
		Buckets:   buckets,
		ObjectIds: objectIDs,
		Sources:   sources,
	})
	if err != nil {
		slog.Error("Failed to batch check message deduplication", slog.Any("error", err))
		return nil, fmt.Errorf("batch deduplication check failed: %w", err)
	}

	// Build set of inserted items for fast lookup
	insertedSet := make(map[string]struct{}, len(inserted))
	for _, row := range inserted {
		key := row.Bucket + "\x00" + row.ObjectID
		insertedSet[key] = struct{}{}
	}

	// Filter to only non-duplicate items
	var result []DedupItem
	for _, item := range items {
		key := item.Bucket + "\x00" + item.ObjectID
		if _, ok := insertedSet[key]; ok {
			result = append(result, item)
		} else {
			slog.Debug("Duplicate message detected in batch",
				slog.String("bucket", item.Bucket),
				slog.String("object_id", item.ObjectID),
				slog.String("source", source))
			recordDuplicate(ctx, item.Bucket, source)
		}
	}

	return result, nil
}

// GetCleanupRetention returns the retention period for cleanup from config
func GetCleanupRetention(cfg *config.Config) time.Duration {
	return cfg.PubSub.Dedup.RetentionDuration
}

// GetCleanupBatchSize returns the batch size for cleanup operations from config
func GetCleanupBatchSize(cfg *config.Config) int {
	return cfg.PubSub.Dedup.CleanupBatchSize
}

// recordDuplicate increments the duplicate counter metric
func recordDuplicate(ctx context.Context, bucket, source string) {
	itemsDuplicated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bucket", bucket),
		attribute.String("source", source),
	))
}
