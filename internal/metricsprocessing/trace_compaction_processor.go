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

package metricsprocessing

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// TraceGroupValidationError represents an error when messages in a group have inconsistent fields
type TraceGroupValidationError struct {
	Field    string
	Expected any
	Got      any
	Message  string
}

func (e *TraceGroupValidationError) Error() string {
	return fmt.Sprintf("trace group validation failed - %s: expected %v, got %v (%s)", e.Field, e.Expected, e.Got, e.Message)
}

// TraceCompactionStore defines database operations needed for trace compaction
type TraceCompactionStore interface {
	GetTraceSeg(ctx context.Context, params lrdb.GetTraceSegParams) (lrdb.TraceSeg, error)
	CompactTraceSegsWithKafkaOffsets(ctx context.Context, params lrdb.CompactTraceSegsParams, kafkaOffsets []lrdb.KafkaOffsetUpdate) error
	MarkTraceSegsCompactedByKeys(ctx context.Context, params lrdb.MarkTraceSegsCompactedByKeysParams) error
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
	InsertSegmentJournal(ctx context.Context, params lrdb.InsertSegmentJournalParams) error
}

// TraceCompactionProcessor implements the Processor interface for trace segment compaction
type TraceCompactionProcessor struct {
	store           TraceCompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
}

// newTraceCompactor creates a new trace compactor instance
func newTraceCompactor(store TraceCompactionStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, cfg *config.Config) *TraceCompactionProcessor {
	return &TraceCompactionProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
	}
}

// validateTraceGroupConsistency ensures all messages in a group have consistent org, instance, dateint, and slot
func validateTraceGroupConsistency(group *accumulationGroup[messages.TraceCompactionKey]) error {
	if len(group.Messages) == 0 {
		return &TraceGroupValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	expectedOrg := group.Key.OrganizationID
	expectedInstance := group.Key.InstanceNum
	expectedDateInt := group.Key.DateInt
	expectedSlotID := group.Key.SlotID

	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.TraceCompactionMessage)
		if !ok {
			return &TraceGroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not a TraceCompactionMessage", i),
			}
		}

		if msg.OrganizationID != expectedOrg {
			return &TraceGroupValidationError{
				Field:    "organization_id",
				Expected: expectedOrg,
				Got:      msg.OrganizationID,
				Message:  fmt.Sprintf("message %d has inconsistent organization ID", i),
			}
		}

		if msg.InstanceNum != expectedInstance {
			return &TraceGroupValidationError{
				Field:    "instance_num",
				Expected: expectedInstance,
				Got:      msg.InstanceNum,
				Message:  fmt.Sprintf("message %d has inconsistent instance number", i),
			}
		}

		if msg.DateInt != expectedDateInt {
			return &TraceGroupValidationError{
				Field:    "dateint",
				Expected: expectedDateInt,
				Got:      msg.DateInt,
				Message:  fmt.Sprintf("message %d has inconsistent dateint", i),
			}
		}

		if msg.SlotID != expectedSlotID {
			return &TraceGroupValidationError{
				Field:    "slot_id",
				Expected: expectedSlotID,
				Got:      msg.SlotID,
				Message:  fmt.Sprintf("message %d has inconsistent slot ID", i),
			}
		}
	}

	return nil
}

// Process implements the Processor interface and performs trace segment compaction
func (p *TraceCompactionProcessor) Process(ctx context.Context, group *accumulationGroup[messages.TraceCompactionKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	groupAge := time.Since(group.CreatedAt)

	ll.Info("Processing trace compaction notification group",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("slotID", int(group.Key.SlotID)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	if err := validateTraceGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Extract all segment IDs from the compaction messages
	var segmentIDs []int64
	segmentRecords := int64(0)
	segmentSize := int64(0)

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.TraceCompactionMessage)
		if !ok {
			continue
		}
		segmentIDs = append(segmentIDs, msg.SegmentID)
		segmentRecords += msg.Records
		segmentSize += msg.FileSize
	}

	ll.Info("Received trace compaction notifications",
		slog.Int("segmentCount", len(segmentIDs)),
		slog.Int64("totalRecords", segmentRecords),
		slog.Int64("totalSize", segmentSize),
		slog.Any("segmentIDs", segmentIDs))

	// For now, we just log that we received the compaction notifications
	// The actual compaction work will be done by the existing work queue system
	// This consumer serves as a way to monitor and react to trace segment creation

	// If we have Kafka offsets to commit, handle them
	if kafkaCommitData != nil && len(kafkaCommitData.Offsets) > 0 {
		// Convert KafkaCommitData to KafkaOffsetUpdate slice for database storage
		var kafkaOffsets []lrdb.KafkaOffsetUpdate
		for partition, offset := range kafkaCommitData.Offsets {
			kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetUpdate{
				ConsumerGroup:  kafkaCommitData.ConsumerGroup,
				Topic:          kafkaCommitData.Topic,
				Partition:      partition,
				Offset:         offset,
				OrganizationID: group.Key.OrganizationID,
				InstanceNum:    group.Key.InstanceNum,
			})
		}

		// For now, we would handle Kafka offset updates here
		// This would typically be done as part of a transaction with the actual compaction
		ll.Debug("Kafka offsets to commit",
			slog.Int("offsetCount", len(kafkaOffsets)),
			slog.String("consumerGroup", kafkaCommitData.ConsumerGroup),
			slog.String("topic", kafkaCommitData.Topic))
	}

	// Note: Segment journal logging would require more setup of signal/action constants
	// and proper parameter mapping, so omitting for this initial implementation

	ll.Info("Trace compaction notification processing completed",
		slog.Int("segmentCount", len(segmentIDs)))

	return nil
}

// GetTargetRecordCount returns the target record count for accumulation (similar to logs)
func (p *TraceCompactionProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.TraceCompactionKey) int64 {
	return p.store.GetTraceEstimate(ctx, groupingKey.OrganizationID)
}
