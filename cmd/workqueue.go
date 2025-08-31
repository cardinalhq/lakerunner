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

package cmd

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type qmc struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
	FrequencyMs    int32
	TsRange        pgtype.Range[pgtype.Timestamptz]
}

func qmcFromWorkable(inf lockmgr.Workable) qmc {
	return qmc{
		OrganizationID: inf.OrganizationID(),
		InstanceNum:    inf.InstanceNum(),
		FrequencyMs:    inf.FrequencyMs(),
		TsRange:        inf.TsRange(),
	}
}

func qmcFromInqueue(inf lrdb.Inqueue, frequency int32, startTS int64) qmc {
	startTime := time.UnixMilli(startTS).UTC()
	endTime := startTime.Add(time.Duration(frequency) * time.Millisecond)

	return qmc{
		OrganizationID: inf.OrganizationID,
		InstanceNum:    inf.InstanceNum,
		FrequencyMs:    frequency,
		TsRange:        helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange(),
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

func queueMetricCompaction(ctx context.Context, mdb lrdb.StoreFull, inf qmc) error {
	// compaction will lock the upstream's frequency's worth of data, so find the upstream frequency.
	upstreamFrequency, ok := helpers.RollupNotifications[inf.FrequencyMs]
	if !ok {
		return nil
		// slog.Warn("unknown frequency for compaction", "frequency_ms", inf.FrequencyMs)
		// upstreamFrequency = inf.FrequencyMs
	}

	startTS, _, ok := helpers.RangeBounds(inf.TsRange)
	if !ok {
		slog.Error("invalid time range for metric compaction", "ts_range", inf.TsRange)
		return nil // invalid range, nothing to do
	}

	upstreamDur := time.Duration(upstreamFrequency) * time.Millisecond
	parentStart := startTS.Time.UTC().Truncate(upstreamDur)
	parentEnd := parentStart.Add(upstreamDur)
	dateint, _ := helpers.MSToDateintHour(parentStart.UTC().UnixMilli())
	runableAt := parentStart.Add(upstreamDur * 2)

	rp := lrdb.WorkQueueAddParams{
		OrgID:     inf.OrganizationID,
		Instance:  inf.InstanceNum,
		Signal:    lrdb.SignalEnumMetrics,
		Action:    lrdb.ActionEnumCompact,
		Dateint:   dateint,
		Frequency: inf.FrequencyMs,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: parentStart, Valid: true},
			Upper:     pgtype.Timestamptz{Time: parentEnd, Valid: true},
			Valid:     true,
		},
		RunnableAt: runableAt,
		Priority:   priorityForFrequencyForCompaction(inf.FrequencyMs),
	}
	//slog.Info("queueing metric compaction", "params", rp)
	return mdb.WorkQueueAdd(ctx, rp)
}

// queueMetricRollup queues a metric rollup job for the given frequency and time range.
func queueMetricRollup(ctx context.Context, mdb lrdb.StoreFull, inf qmc) error {
	// rollups end when the last frequency is processed.
	upstreamFrequency, ok := helpers.RollupNotifications[inf.FrequencyMs]
	if !ok {
		slog.Warn("unknown frequency for rollup", "frequency_ms", inf.FrequencyMs)
		return nil
	}

	startTS, _, ok := helpers.RangeBounds(inf.TsRange)
	if !ok {
		slog.Error("invalid time range for metric rollup", "ts_range", inf.TsRange)
		return nil // invalid range, nothing to do
	}

	upstreamDur := time.Duration(upstreamFrequency) * time.Millisecond
	parentStart := startTS.Time.UTC().Truncate(upstreamDur)
	parentEnd := parentStart.Add(upstreamDur)
	dateint, _ := helpers.MSToDateintHour(parentStart.UTC().UnixMilli())
	runableAt := parentStart.Add(upstreamDur * 2)

	rp := lrdb.WorkQueueAddParams{
		OrgID:     inf.OrganizationID,
		Instance:  inf.InstanceNum,
		Signal:    lrdb.SignalEnumMetrics,
		Action:    lrdb.ActionEnumRollup,
		Dateint:   dateint,
		Frequency: upstreamFrequency,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: parentStart, Valid: true},
			Upper:     pgtype.Timestamptz{Time: parentEnd, Valid: true},
			Valid:     true,
		},
		RunnableAt: runableAt,
		Priority:   priorityForFrequencyForRollup(upstreamFrequency),
	}
	//slog.Info("queueing metric rollup", "params", rp)
	return mdb.WorkQueueAdd(ctx, rp)
}
