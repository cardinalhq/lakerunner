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
	"path/filepath"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// KafkaNotificationSender sends object storage notifications to Kafka
type KafkaNotificationSender interface {
	SendBatch(ctx context.Context, signal string, notifications []messages.ObjStoreNotificationMessage) error
}

// ConversionResult holds the result of converting parsed items to Kafka notifications
type ConversionResult struct {
	NotificationsBySignal map[string][]messages.ObjStoreNotificationMessage
	ItemsSkipped          int
	ItemsProcessed        int
	SkipReasons           map[string]int            // reason -> count
	FileTypeCounts        map[string]map[string]int // signal -> extension -> count
}

// getFileExtensionCategory returns a normalized extension category for counting
func getFileExtensionCategory(objectID string) string {
	ext := strings.ToLower(filepath.Ext(objectID))

	// Handle compound extensions first
	if strings.HasSuffix(strings.ToLower(objectID), ".json.gz") {
		return "json.gz"
	}
	if strings.HasSuffix(strings.ToLower(objectID), ".binpb.gz") {
		return "binpb.gz"
	}

	// Handle simple extensions
	switch ext {
	case ".json":
		return "json"
	case ".binpb":
		return "binpb"
	case ".parquet":
		return "parquet"
	default:
		return "other"
	}
}

// convertItemsToKafkaMessages processes parsed S3 items and converts them to Kafka notification messages
// This function is separated from sending to make it testable in isolation
func convertItemsToKafkaMessages(
	ctx context.Context,
	items []IngestItem,
	sp storageprofile.StorageProfileProvider,
) (ConversionResult, error) {
	result := ConversionResult{
		NotificationsBySignal: make(map[string][]messages.ObjStoreNotificationMessage),
		SkipReasons:           make(map[string]int),
		FileTypeCounts:        make(map[string]map[string]int),
	}

	for _, item := range items {
		// Skip database files
		if strings.HasPrefix(item.ObjectID, "db/") {
			slog.Debug("Skipping database file", slog.String("objectID", item.ObjectID))
			result.ItemsSkipped++
			result.SkipReasons["database_file"]++
			itemsSkipped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("reason", "database_file"),
				attribute.String("bucket", item.Bucket),
			))
			continue
		}

		// For otel-raw paths, org and signal were already extracted in parser
		if !strings.HasPrefix(item.ObjectID, "otel-raw/") {
			// Use new organization resolution logic that also returns the signal from prefix mapping
			resolution, err := sp.ResolveOrganization(ctx, item.Bucket, item.ObjectID)
			if err != nil {
				slog.Error("Failed to resolve organization with signal",
					slog.Any("error", err),
					slog.String("bucket", item.Bucket),
					slog.String("object_id", item.ObjectID))
				result.ItemsSkipped++
				result.SkipReasons["resolve_org_failed"]++
				itemsSkipped.Add(ctx, 1, metric.WithAttributes(
					attribute.String("reason", "resolve_org_failed"),
					attribute.String("bucket", item.Bucket),
				))
				continue
			}
			item.OrganizationID = resolution.OrganizationID
			item.Signal = resolution.Signal
		}

		// Get the lowest instance for this organization and bucket
		profile, err := sp.GetLowestInstanceStorageProfile(ctx, item.OrganizationID, item.Bucket)
		if err != nil {
			slog.Error("Failed to get lowest instance storage profile",
				slog.Any("error", err),
				slog.String("organization_id", item.OrganizationID.String()),
				slog.String("bucket", item.Bucket))
			result.ItemsSkipped++
			result.SkipReasons["get_profile_failed"]++
			itemsSkipped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("reason", "get_profile_failed"),
				attribute.String("bucket", item.Bucket),
			))
			continue
		}

		instanceNum := profile.InstanceNum
		collectorName := profile.CollectorName
		item.InstanceNum = instanceNum

		slog.Debug("Processing item for Kafka",
			slog.String("bucket", item.Bucket),
			slog.String("object_id", item.ObjectID),
			slog.String("telemetry_type", item.Signal),
			slog.String("organization_id", item.OrganizationID.String()),
			slog.String("collector_name", collectorName),
			slog.Int("instance_num", int(instanceNum)))

		// Create object storage notification message for Kafka
		notification := messages.ObjStoreNotificationMessage{
			OrganizationID: item.OrganizationID,
			InstanceNum:    item.InstanceNum,
			Bucket:         item.Bucket,
			ObjectID:       item.ObjectID,
			FileSize:       item.FileSize,
			QueuedAt:       time.Now(),
		}

		// Add to the appropriate signal group
		result.NotificationsBySignal[item.Signal] = append(result.NotificationsBySignal[item.Signal], notification)
		result.ItemsProcessed++

		// Count file types by signal
		if result.FileTypeCounts[item.Signal] == nil {
			result.FileTypeCounts[item.Signal] = make(map[string]int)
		}
		extCategory := getFileExtensionCategory(item.ObjectID)
		result.FileTypeCounts[item.Signal][extCategory]++
	}

	return result, nil
}

// handleMessageWithKafka processes object storage notifications and sends them to Kafka
func handleMessageWithKafka(
	ctx context.Context,
	msg []byte,
	source string,
	sp storageprofile.StorageProfileProvider,
	kafkaSender KafkaNotificationSender,
	deduplicator Deduplicator,
	stats *StatsAggregator,
) error {
	if len(msg) == 0 {
		return fmt.Errorf("empty message received")
	}

	items, err := parseS3LikeEvents(msg)
	if err != nil {
		return fmt.Errorf("failed to parse S3-like events: %w", err)
	}

	// Deduplicate items early before processing (batch operation)
	dedupItems := make([]DedupItem, len(items))
	for i, item := range items {
		dedupItems[i] = DedupItem{Bucket: item.Bucket, ObjectID: item.ObjectID}
	}

	nonDuplicates, err := deduplicator.CheckAndRecordBatch(ctx, dedupItems, source)
	if err != nil {
		return fmt.Errorf("batch deduplication check failed: %w", err)
	}

	// Build set of non-duplicate items for filtering
	nonDupSet := make(map[string]struct{}, len(nonDuplicates))
	for _, d := range nonDuplicates {
		nonDupSet[d.Bucket+"\x00"+d.ObjectID] = struct{}{}
	}

	// Filter to only non-duplicate items
	var dedupedItems []IngestItem
	for _, item := range items {
		if _, ok := nonDupSet[item.Bucket+"\x00"+item.ObjectID]; ok {
			dedupedItems = append(dedupedItems, item)
		}
	}

	// Convert items to Kafka messages
	result, err := convertItemsToKafkaMessages(ctx, dedupedItems, sp)
	if err != nil {
		return err
	}

	// Record file type counts in stats aggregator
	if stats != nil {
		for signal, extCounts := range result.FileTypeCounts {
			stats.RecordFileTypes(signal, extCounts)
		}
	}

	// Track skipped items in stats
	for _, count := range result.SkipReasons {
		if stats != nil && count > 0 {
			stats.RecordSkipped("unknown", count)
		}
	}

	// Send notifications grouped by signal to appropriate topics
	for signal, signalNotifications := range result.NotificationsBySignal {
		if err := kafkaSender.SendBatch(ctx, signal, signalNotifications); err != nil {
			// Record failed items in stats
			if stats != nil {
				stats.RecordFailed(signal, len(signalNotifications))
			}
			return fmt.Errorf("failed to send notifications to Kafka for signal %s: %w", signal, err)
		}

		// Record successful items in stats and metrics
		if stats != nil {
			stats.RecordProcessed(signal, len(signalNotifications))
		}

		for _, notification := range signalNotifications {
			itemsProcessed.Add(ctx, 1, metric.WithAttributes(
				attribute.String("bucket", notification.Bucket),
				attribute.String("telemetry_type", signal),
				attribute.String("source", source),
			))
		}
	}

	return nil
}

// KafkaHandler wraps the Kafka notification manager for pubsub services
type KafkaHandler struct {
	manager      *fly.ObjStoreNotificationManager
	source       string
	sp           storageprofile.StorageProfileProvider
	deduplicator Deduplicator
	stats        *StatsAggregator
}

// NewKafkaHandler creates a new Kafka handler for pubsub notifications
func NewKafkaHandler(
	ctx context.Context,
	cfg *config.Config,
	factory *fly.Factory,
	source string,
	sp storageprofile.StorageProfileProvider,
	deduplicator Deduplicator,
) (*KafkaHandler, error) {
	manager := fly.NewObjStoreNotificationManager(ctx, cfg, factory)

	// Create stats aggregator with 20 second reporting interval
	stats := NewStatsAggregator(20 * time.Second)
	stats.Start(ctx)

	return &KafkaHandler{
		manager:      manager,
		source:       source,
		sp:           sp,
		deduplicator: deduplicator,
		stats:        stats,
	}, nil
}

// HandleMessage processes a message and sends it to Kafka
func (h *KafkaHandler) HandleMessage(ctx context.Context, msg []byte) error {
	producer, err := h.manager.GetProducer(ctx, h.source)
	if err != nil {
		return fmt.Errorf("failed to get Kafka producer: %w", err)
	}

	return handleMessageWithKafka(ctx, msg, h.source, h.sp, producer, h.deduplicator, h.stats)
}

// Close closes the Kafka handler
func (h *KafkaHandler) Close() error {
	if h.stats != nil {
		h.stats.Stop()
	}
	return h.manager.Close()
}
