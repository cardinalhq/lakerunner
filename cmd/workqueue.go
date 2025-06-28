// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/pkg/lockmgr"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
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
	return qmc{
		OrganizationID: inf.OrganizationID,
		InstanceNum:    inf.InstanceNum,
		FrequencyMs:    frequency,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: time.UnixMilli(startTS).UTC(), Valid: true},
			Upper:     pgtype.Timestamptz{Time: time.UnixMilli(startTS).UTC().Add(time.Duration(frequency) * time.Millisecond), Valid: true},
		},
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
	upstreamFrequency, ok := rollupNotifications[inf.FrequencyMs]
	if !ok {
		return nil
		// slog.Warn("unknown frequency for compaction", "frequency_ms", inf.FrequencyMs)
		// upstreamFrequency = inf.FrequencyMs
	}

	startTS, _, ok := RangeBounds(inf.TsRange)
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
	upstreamFrequency, ok := rollupNotifications[inf.FrequencyMs]
	if !ok {
		slog.Warn("unknown frequency for rollup", "frequency_ms", inf.FrequencyMs)
		return nil
	}

	startTS, _, ok := RangeBounds(inf.TsRange)
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

// queueLogCompaction queues a log compaction job for the entire dateint
func queueLogCompaction(ctx context.Context, mdb lrdb.StoreFull, inf qmc) error {
	upstreamDur := 24 * time.Hour
	startTS, _, ok := RangeBounds(inf.TsRange)
	if !ok {
		slog.Error("invalid time range for log compaction notification", "ts_range", inf.TsRange)
		return nil // invalid range, nothing to do
	}

	parentStart := startTS.Time.UTC().Truncate(upstreamDur)
	parentEnd := parentStart.Add(upstreamDur)
	dateint, _ := helpers.MSToDateintHour(parentStart.UTC().UnixMilli())

	return mdb.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		OrgID:     inf.OrganizationID,
		Instance:  inf.InstanceNum,
		Signal:    lrdb.SignalEnumLogs,
		Action:    lrdb.ActionEnumCompact,
		Dateint:   dateint,
		Frequency: -1,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Timestamptz{Time: parentStart, Valid: true},
			Upper:     pgtype.Timestamptz{Time: parentEnd, Valid: true},
			Valid:     true,
		},
		RunnableAt: time.Now().UTC().Add(5 * time.Minute), // give it a little time to settle
	})
}
