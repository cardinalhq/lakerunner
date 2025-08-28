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

package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func GetRollupCmd() *cobra.Command {
	var (
		orgIDStr     string
		instance     int16
		slotID       int32
		frequencyStr string
		timeStr      string
		endTimeStr   string
		dryRun       bool
	)

	rollupCmd := &cobra.Command{
		Use:   "rollup",
		Short: "Add metric rollup work queue items",
		Long: `Add metric rollup work queue items for specified organization, instance, frequency, and time.
		
Time must be specified in RFC3339 format (e.g., 2023-01-01T12:00:00Z).
Frequency must be one of: 10s, 60s, 300s, 1200s (3600s not allowed for rollups as it has no upstream).
If --end-time is specified, multiple work items will be queued to cover the entire time range.
Times will be aligned to the upstream frequency boundaries.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			// Parse and validate parameters
			orgID, err := uuid.Parse(orgIDStr)
			if err != nil {
				return fmt.Errorf("invalid org-id: %w", err)
			}

			frequency, err := parseRollupFrequency(frequencyStr)
			if err != nil {
				return fmt.Errorf("invalid frequency: %w", err)
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

			return runMetricRollup(orgID, instance, slotID, frequency, startTime, endTime, dryRun)
		},
	}

	rollupCmd.Flags().StringVar(&orgIDStr, "org-id", "", "Organization ID (required)")
	rollupCmd.Flags().Int16Var(&instance, "instance", 0, "Instance number (required)")
	rollupCmd.Flags().Int32Var(&slotID, "slot-id", 0, "Slot ID (defaults to 0)")
	rollupCmd.Flags().StringVar(&frequencyStr, "frequency", "", "Frequency (10s, 60s, 300s, 1200s) (required)")
	rollupCmd.Flags().StringVar(&timeStr, "time", "", "Start time in RFC3339 format (required)")
	rollupCmd.Flags().StringVar(&endTimeStr, "end-time", "", "End time in RFC3339 format (optional, for time ranges)")
	rollupCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be queued without actually adding work items")

	_ = rollupCmd.MarkFlagRequired("org-id")
	_ = rollupCmd.MarkFlagRequired("instance")
	_ = rollupCmd.MarkFlagRequired("frequency")
	_ = rollupCmd.MarkFlagRequired("time")

	return rollupCmd
}

func parseRollupFrequency(frequencyStr string) (int32, error) {
	switch frequencyStr {
	case "10s":
		return 10_000, nil
	case "60s":
		return 60_000, nil
	case "300s":
		return 300_000, nil
	case "1200s":
		return 1_200_000, nil
	case "3600s":
		return 0, fmt.Errorf("3600s rollups not allowed - no upstream frequency exists")
	default:
		return 0, fmt.Errorf("frequency must be one of: 10s, 60s, 300s, 1200s")
	}
}

func priorityForFrequencyForRollup(f int32) int32 {
	switch f {
	case 10_000:
		return 1000
	case 60_000:
		return 800
	case 300_000:
		return 600
	case 1_200_000:
		return 400
	case 3_600_000:
		return 200
	default:
		return 0
	}
}

func runMetricRollup(orgID uuid.UUID, instance int16, slotID int32, frequency int32, startTime time.Time, endTime *time.Time, dryRun bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var store lrdb.StoreFull
	if !dryRun {
		var err error
		store, err = dbopen.LRDBStoreForAdmin(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to lrdb: %w", err)
		}
	}

	// Get upstream frequency for alignment
	upstreamFrequency, ok := helpers.RollupNotifications[frequency]
	if !ok {
		return fmt.Errorf("unknown frequency %d for rollup", frequency)
	}

	upstreamDur := time.Duration(upstreamFrequency) * time.Millisecond
	alignedStart := startTime.Truncate(upstreamDur)

	// If no end time, queue single item
	if endTime == nil {
		return queueSingleMetricRollup(ctx, store, orgID, instance, slotID, frequency, alignedStart, dryRun)
	}

	// Queue items for time range
	alignedEnd := endTime.Truncate(upstreamDur)
	current := alignedStart

	var queuedCount int
	for current.Before(alignedEnd) || current.Equal(alignedEnd) {
		if err := queueSingleMetricRollup(ctx, store, orgID, instance, slotID, frequency, current, dryRun); err != nil {
			return fmt.Errorf("failed to queue metric rollup for time %v: %w", current, err)
		}
		queuedCount++
		current = current.Add(upstreamDur)
	}

	if dryRun {
		fmt.Printf("[DRY-RUN] Would queue %d metric rollup work items from %v to %v\n",
			queuedCount, alignedStart, alignedEnd)
	} else {
		fmt.Printf("Successfully queued %d metric rollup work items from %v to %v\n",
			queuedCount, alignedStart, alignedEnd)
	}
	return nil
}

func queueSingleMetricRollup(ctx context.Context, store lrdb.StoreFull, orgID uuid.UUID, instance int16, slotID int32, frequency int32, alignedTime time.Time, dryRun bool) error {
	// Get upstream frequency for range calculation
	upstreamFrequency, ok := helpers.RollupNotifications[frequency]
	if !ok {
		return fmt.Errorf("unknown frequency %d for rollup", frequency)
	}

	upstreamDur := time.Duration(upstreamFrequency) * time.Millisecond
	parentStart := alignedTime
	parentEnd := parentStart.Add(upstreamDur)
	dateint, _ := helpers.MSToDateintHour(parentStart.UTC().UnixMilli())
	runableAt := parentStart.Add(upstreamDur * 2)

	params := lrdb.WorkQueueAddParams{
		OrgID:     orgID,
		Instance:  instance,
		Signal:    lrdb.SignalEnumMetrics,
		Action:    lrdb.ActionEnumRollup,
		Dateint:   dateint,
		Frequency: upstreamFrequency, // Rollup uses upstream frequency, not input frequency
		SlotID:    slotID,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: parentStart.UTC(), Valid: true},
			Upper:     pgtype.Timestamptz{Time: parentEnd.UTC(), Valid: true},
			Valid:     true,
		},
		RunnableAt: runableAt,
		Priority:   priorityForFrequencyForRollup(upstreamFrequency),
	}

	if dryRun {
		fmt.Printf("[DRY-RUN] Would queue metric rollup: org=%s, instance=%d, slot=%d, input_frequency=%d, rollup_frequency=%d, dateint=%d, time_range=%v to %v, priority=%d\n",
			orgID, instance, slotID, frequency, upstreamFrequency, dateint, parentStart.Format(time.RFC3339), parentEnd.Format(time.RFC3339), params.Priority)
	} else {
		if err := store.WorkQueueAdd(ctx, params); err != nil {
			return fmt.Errorf("failed to add work queue item: %w", err)
		}

		fmt.Printf("Queued metric rollup: org=%s, instance=%d, slot=%d, input_frequency=%d, rollup_frequency=%d, dateint=%d, time_range=%v to %v\n",
			orgID, instance, slotID, frequency, upstreamFrequency, dateint, parentStart.Format(time.RFC3339), parentEnd.Format(time.RFC3339))
	}

	return nil
}
