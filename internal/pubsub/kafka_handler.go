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

// handleMessageWithKafka processes object storage notifications and sends them to Kafka
func handleMessageWithKafka(
	ctx context.Context,
	msg []byte,
	source string,
	sp storageprofile.StorageProfileProvider,
	kafkaSender KafkaNotificationSender,
	deduplicator *Deduplicator,
	stats *StatsAggregator,
) error {
	if len(msg) == 0 {
		return fmt.Errorf("empty message received")
	}

	items, err := parseS3LikeEvents(msg)
	if err != nil {
		return fmt.Errorf("failed to parse S3-like events: %w", err)
	}

	// Group notifications by signal
	notificationsBySignal := make(map[string][]messages.ObjStoreNotificationMessage)

	for _, item := range items {
		// Skip database files
		if strings.HasPrefix(item.ObjectID, "db/") {
			slog.Debug("Skipping database file", slog.String("objectID", item.ObjectID))
			itemsSkipped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("reason", "database_file"),
				attribute.String("bucket", item.Bucket),
			))
			if stats != nil {
				stats.RecordSkipped("unknown", 1)
			}
			continue
		}

		// Use new organization resolution logic
		orgID, err := sp.ResolveOrganization(ctx, item.Bucket, item.ObjectID)
		if err != nil {
			slog.Error("Failed to resolve organization",
				slog.Any("error", err),
				slog.String("bucket", item.Bucket),
				slog.String("object_id", item.ObjectID))
			itemsSkipped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("reason", "resolve_org_failed"),
				attribute.String("bucket", item.Bucket),
			))
			continue
		}

		// For otel-raw paths, org and collector were already extracted in parser
		// For other paths, we now have the resolved org
		if !strings.HasPrefix(item.ObjectID, "otel-raw/") {
			item.OrganizationID = orgID
			item.CollectorName = "" // Not used in v2
		}

		// Determine collector name and instance number
		collectorName := item.CollectorName
		instanceNum := int16(1) // Default instance number

		// Lookup instance number from storage profile
		if collectorName != "" {
			profile, err := sp.GetStorageProfileForOrganizationAndCollector(ctx, item.OrganizationID, collectorName)
			if err != nil {
				slog.Warn("Failed to lookup storage profile for collector, using default instance",
					slog.Any("error", err),
					slog.String("organization_id", item.OrganizationID.String()),
					slog.String("collector_name", collectorName))
			} else {
				instanceNum = profile.InstanceNum
			}
		}

		// Update instance number in item for deduplication
		item.InstanceNum = instanceNum

		// Check for duplicate message
		shouldProcess, err := deduplicator.CheckAndRecord(ctx, &item, source)
		if err != nil {
			// Deduplication failure - return error to trigger message retry
			return fmt.Errorf("deduplication check failed for bucket %s object %s: %w",
				item.Bucket, item.ObjectID, err)
		}
		if !shouldProcess {
			// Message is a duplicate, skip it
			continue
		}

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
			InstanceNum:    instanceNum,
			Bucket:         item.Bucket,
			ObjectID:       item.ObjectID,
			FileSize:       item.FileSize,
			QueuedAt:       time.Now(),
		}

		// Add to the appropriate signal group
		notificationsBySignal[item.Signal] = append(notificationsBySignal[item.Signal], notification)

		itemsProcessed.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bucket", item.Bucket),
			attribute.String("telemetry_type", item.Signal),
			attribute.String("source", source),
		))
	}

	// Send notifications grouped by signal to appropriate topics
	for signal, signalNotifications := range notificationsBySignal {
		if err := kafkaSender.SendBatch(ctx, signal, signalNotifications); err != nil {
			// Record failed items in stats
			if stats != nil {
				stats.RecordFailed(signal, len(signalNotifications))
			}
			return fmt.Errorf("failed to send notifications to Kafka for signal %s: %w", signal, err)
		}
		// Record successful items in stats
		if stats != nil {
			stats.RecordProcessed(signal, len(signalNotifications))
		}
	}

	return nil
}

// KafkaHandler wraps the Kafka notification manager for pubsub services
type KafkaHandler struct {
	manager      *fly.ObjStoreNotificationManager
	source       string
	sp           storageprofile.StorageProfileProvider
	deduplicator *Deduplicator
	stats        *StatsAggregator
}

// NewKafkaHandler creates a new Kafka handler for pubsub notifications
func NewKafkaHandler(
	ctx context.Context,
	cfg *config.Config,
	factory *fly.Factory,
	source string,
	sp storageprofile.StorageProfileProvider,
	deduplicator *Deduplicator,
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
