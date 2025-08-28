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

var (
	_ string // apiKey placeholder for future use
)

func GetCompactCmd() *cobra.Command {
	var (
		orgIDStr     string
		instance     int16
		slotID       int32
		frequencyStr string
		timeStr      string
		endTimeStr   string
		dryRun       bool
	)

	compactCmd := &cobra.Command{
		Use:   "compact",
		Short: "Add metric compaction work queue items",
		Long: `Add metric compaction work queue items for specified organization, instance, frequency, and time.
		
Time must be specified in RFC3339 format (e.g., 2023-01-01T12:00:00Z).
Frequency must be one of: 10s, 60s, 300s, 1200s, 3600s.
If --end-time is specified, multiple work items will be queued to cover the entire time range.
Times will be aligned to frequency boundaries.`,
		RunE: func(_ *cobra.Command, _ []string) error {
			// Parse and validate parameters
			orgID, err := uuid.Parse(orgIDStr)
			if err != nil {
				return fmt.Errorf("invalid org-id: %w", err)
			}

			frequency, err := parseFrequency(frequencyStr)
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

			return runMetricCompact(orgID, instance, slotID, frequency, startTime, endTime, dryRun)
		},
	}

	compactCmd.Flags().StringVar(&orgIDStr, "org-id", "", "Organization ID (required)")
	compactCmd.Flags().Int16Var(&instance, "instance", 0, "Instance number (required)")
	compactCmd.Flags().Int32Var(&slotID, "slot-id", 0, "Slot ID (defaults to 0)")
	compactCmd.Flags().StringVar(&frequencyStr, "frequency", "", "Frequency (10s, 60s, 300s, 1200s, 3600s) (required)")
	compactCmd.Flags().StringVar(&timeStr, "time", "", "Start time in RFC3339 format (required)")
	compactCmd.Flags().StringVar(&endTimeStr, "end-time", "", "End time in RFC3339 format (optional, for time ranges)")
	compactCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be queued without actually adding work items")

	_ = compactCmd.MarkFlagRequired("org-id")
	_ = compactCmd.MarkFlagRequired("instance")
	_ = compactCmd.MarkFlagRequired("frequency")
	_ = compactCmd.MarkFlagRequired("time")

	return compactCmd
}

func SetAPIKey(key string) {
	_ = key // apiKey for future use
}

func parseFrequency(frequencyStr string) (int32, error) {
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
		return 3_600_000, nil
	default:
		return 0, fmt.Errorf("frequency must be one of: 10s, 60s, 300s, 1200s, 3600s")
	}
}

func priorityForFrequencyForCompaction(f int32) int32 {
	switch f {
	case 10_000:
		return 1001
	case 60_000:
		return 801
	case 300_000:
		return 601
	case 1_200_000:
		return 401
	case 3_600_000:
		return 201
	default:
		return 0
	}
}

func runMetricCompact(orgID uuid.UUID, instance int16, slotID int32, frequency int32, startTime time.Time, endTime *time.Time, dryRun bool) error {
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
		return fmt.Errorf("unknown frequency %d for compaction", frequency)
	}

	upstreamDur := time.Duration(upstreamFrequency) * time.Millisecond
	alignedStart := startTime.Truncate(upstreamDur)

	// If no end time, queue single item
	if endTime == nil {
		return queueSingleMetricCompact(ctx, store, orgID, instance, slotID, frequency, alignedStart, dryRun)
	}

	// Queue items for time range
	alignedEnd := endTime.Truncate(upstreamDur)
	current := alignedStart

	var queuedCount int
	for current.Before(alignedEnd) || current.Equal(alignedEnd) {
		if err := queueSingleMetricCompact(ctx, store, orgID, instance, slotID, frequency, current, dryRun); err != nil {
			return fmt.Errorf("failed to queue metric compaction for time %v: %w", current, err)
		}
		queuedCount++
		current = current.Add(upstreamDur)
	}

	if dryRun {
		fmt.Printf("[DRY-RUN] Would queue %d metric compaction work items from %v to %v\n",
			queuedCount, alignedStart, alignedEnd)
	} else {
		fmt.Printf("Successfully queued %d metric compaction work items from %v to %v\n",
			queuedCount, alignedStart, alignedEnd)
	}
	return nil
}

func queueSingleMetricCompact(ctx context.Context, store lrdb.StoreFull, orgID uuid.UUID, instance int16, slotID int32, frequency int32, alignedTime time.Time, dryRun bool) error {
	// Get upstream frequency for range calculation
	upstreamFrequency, ok := helpers.RollupNotifications[frequency]
	if !ok {
		return fmt.Errorf("unknown frequency %d for compaction", frequency)
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
		Action:    lrdb.ActionEnumCompact,
		Dateint:   dateint,
		Frequency: frequency,
		SlotID:    slotID,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: parentStart.UTC(), Valid: true},
			Upper:     pgtype.Timestamptz{Time: parentEnd.UTC(), Valid: true},
			Valid:     true,
		},
		RunnableAt: runableAt,
		Priority:   priorityForFrequencyForCompaction(frequency),
	}

	if dryRun {
		fmt.Printf("[DRY-RUN] Would queue metric compaction: org=%s, instance=%d, slot=%d, frequency=%d, dateint=%d, time_range=%v to %v, priority=%d\n",
			orgID, instance, slotID, frequency, dateint, parentStart.Format(time.RFC3339), parentEnd.Format(time.RFC3339), params.Priority)
	} else {
		if err := store.WorkQueueAdd(ctx, params); err != nil {
			return fmt.Errorf("failed to add work queue item: %w", err)
		}

		fmt.Printf("Queued metric compaction: org=%s, instance=%d, slot=%d, frequency=%d, dateint=%d, time_range=%v to %v\n",
			orgID, instance, slotID, frequency, dateint, parentStart.Format(time.RFC3339), parentEnd.Format(time.RFC3339))
	}

	return nil
}
