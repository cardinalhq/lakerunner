// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package traces

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	_ string // apiKey placeholder for future use
)

func GetCompactCmd() *cobra.Command {
	var (
		orgIDStr   string
		instance   int16
		timeStr    string
		endTimeStr string
		dryRun     bool
	)

	compactCmd := &cobra.Command{
		Use:   "compact",
		Short: "Add trace compaction work queue items",
		Long: `Add trace compaction work queue items for specified organization, instance, slot, and time.
		
Time must be specified in RFC3339 format (e.g., 2023-01-01T12:00:00Z).
If --end-time is specified, multiple work items will be queued to cover the entire time range.
Times will be aligned to hour boundaries as traces are processed hourly.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			// Parse and validate parameters
			orgID, err := uuid.Parse(orgIDStr)
			if err != nil {
				return fmt.Errorf("invalid org-id: %w", err)
			}

			startTime, err := time.Parse(time.RFC3339, timeStr)
			if err != nil {
				return fmt.Errorf("invalid time format: %w", err)
			}

			var endTime *time.Time
			if endTimeStr != "" {
				t, err := time.Parse(time.RFC3339, endTimeStr)
				if err != nil {
					return fmt.Errorf("invalid end-time format: %w", err)
				}
				endTime = &t
			}

			return runTraceCompact(orgID, instance, startTime, endTime, dryRun)
		},
	}

	compactCmd.Flags().StringVar(&orgIDStr, "org-id", "", "Organization ID (required)")
	compactCmd.Flags().Int16Var(&instance, "instance", 0, "Instance number (required)")
	compactCmd.Flags().StringVar(&timeStr, "time", "", "Start time in RFC3339 format (required)")
	compactCmd.Flags().StringVar(&endTimeStr, "end-time", "", "End time in RFC3339 format (optional, for time ranges)")
	compactCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be queued without actually adding work items")

	_ = compactCmd.MarkFlagRequired("org-id")
	_ = compactCmd.MarkFlagRequired("instance")
	_ = compactCmd.MarkFlagRequired("slot-id")
	_ = compactCmd.MarkFlagRequired("time")

	return compactCmd
}

func SetAPIKey(key string) {
	_ = key // apiKey for future use
}

func runTraceCompact(orgID uuid.UUID, instance int16, startTime time.Time, endTime *time.Time, dryRun bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var store lrdb.StoreFull
	if !dryRun {
		var err error
		store, err = lrdb.LRDBStoreForAdmin(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to lrdb: %w", err)
		}
	}

	// Align start time to hour boundary
	alignedStart := helpers.TruncateToHour(startTime)

	// If no end time, queue single item
	if endTime == nil {
		return queueSingleTraceCompact(ctx, store, orgID, instance, alignedStart, dryRun)
	}

	// Queue items for time range
	alignedEnd := helpers.TruncateToHour(*endTime)
	current := alignedStart

	var queuedCount int
	for current.Before(alignedEnd) || current.Equal(alignedEnd) {
		if err := queueSingleTraceCompact(ctx, store, orgID, instance, current, dryRun); err != nil {
			return fmt.Errorf("failed to queue trace compaction for time %v: %w", current, err)
		}
		queuedCount++
		current = current.Add(time.Hour)
	}

	if dryRun {
		fmt.Printf("[DRY-RUN] Would queue %d trace compaction work items from %v to %v\n",
			queuedCount, alignedStart, alignedEnd)
	} else {
		fmt.Printf("Successfully queued %d trace compaction work items from %v to %v\n",
			queuedCount, alignedStart, alignedEnd)
	}
	return nil
}

func queueSingleTraceCompact(ctx context.Context, store lrdb.StoreFull, orgID uuid.UUID, instance int16, hourTime time.Time, dryRun bool) error {
	return fmt.Errorf("trace compaction queueing is no longer supported - work queue system has been removed")
}
