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
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// TraceCompactionProcessor implements compaction processing for traces using the base framework
type TraceCompactionProcessor struct {
	store           TraceCompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
}

// NewTraceCompactionProcessor creates a new trace compaction processor
func NewTraceCompactionProcessor(
	store TraceCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg *config.Config,
) *TraceCompactionProcessor {
	return &TraceCompactionProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
	}
}

// Process implements the processor interface for trace compaction
func (p *TraceCompactionProcessor) Process(ctx context.Context, group *accumulationGroup[messages.TraceCompactionKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	// Calculate group age from Hunter timestamp
	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting trace compaction",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	// TODO: Implement trace compaction logic
	// For now, trace compaction is not implemented in the V2 framework
	ll.Warn("Trace compaction not yet implemented in V2 framework - skipping")

	return nil
}

// GetTargetRecordCount returns the target record count for a grouping key
func (p *TraceCompactionProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.TraceCompactionKey) int64 {
	return p.store.GetTraceEstimate(ctx, groupingKey.OrganizationID)
}
