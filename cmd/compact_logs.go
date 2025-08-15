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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-logs",
		Short: "Compact logs into optimally sized files",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

			servicename := "lakerunner-compact-logs"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "logs"),
				attribute.String("action", "compact"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			compactLogsDoneCtx = doneCtx

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewRunqueueLoopContext(doneCtx, "logs", "compact", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, compactLogsFor)
		},
	}
	rootCmd.AddCommand(cmd)
}

var compactLogsDoneCtx context.Context

func compactLogsFor(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
) (WorkResult, error) {
	profile, err := sp.Get(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}
	if profile.Role == "" {
		if !profile.Hosted {
			ll.Error("No role on non-hosted profile")
			return WorkResultTryAgainLater, err
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	ll.Info("Processing log compression item", slog.Any("workItem", inf.AsMap()))
	return logCompactItemDo(ctx, ll, mdb, tmpdir, inf, profile, s3client, rpfEstimate)
}

func logCompactItemDo(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	sp storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rpfEstimate int64,
) (WorkResult, error) {
	st, et, ok := RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, errors.New("error getting range bounds")
	}
	stdi := timeToDateint(st.Time)
	etdi := timeToDateint(et.Time.Add(-time.Millisecond)) // end dateint is inclusive, so subtract 1ms
	if stdi != etdi {
		ll.Error("Range bounds are not the same dateint",
			slog.Int("startDateint", int(stdi)),
			slog.Time("st", st.Time),
			slog.Int("endDateint", int(etdi)),
			slog.Time("et", et.Time),
		)
		return WorkResultTryAgainLater, errors.New("range bounds are not the same dateint")
	}

	segments, err := mdb.GetLogSegmentsForCompaction(ctx, lrdb.GetLogSegmentsForCompactionParams{
		OrganizationID: sp.OrganizationID,
		Dateint:        stdi,
		InstanceNum:    sp.InstanceNum,
	})
	if err != nil {
		ll.Error("Error getting log segments for compaction", slog.String("error", err.Error()))
		return WorkResultTryAgainLater, err
	}
	ll.Info("Got log segments for compaction",
		slog.Int("segmentCount", len(segments)))

	if len(segments) == 0 {
		ll.Info("No segments to compact")
		return WorkResultSuccess, nil
	}

	packed, err := logcrunch.PackSegments(segments, rpfEstimate)
	if err != nil {
		ll.Error("Error packing segments", slog.String("error", err.Error()))
		return WorkResultTryAgainLater, err
	}

	lastGroupSmall := false
	if len(packed) > 0 {
		// if the last packed segment is smaller than half our target size, drop it.
		bytecount := int64(0)
		lastGroup := packed[len(packed)-1]
		for _, segment := range lastGroup {
			bytecount += segment.FileSize
		}
		if bytecount < targetFileSize/2 {
			packed = packed[:len(packed)-1]
			lastGroupSmall = true
		}
	}

	if len(packed) == 0 {
		ll.Info("No segments to compact")
		return WorkResultSuccess, nil
	}

	ll.Info("counts", slog.Int("currentSegments", len(segments)), slog.Int("packGroups", len(packed)), slog.Bool("lastGroupSmall", lastGroupSmall))

	for i, group := range packed {
		ll := ll.With(slog.Int("groupIndex", i))
		err = packSegment(ctx, ll, tmpdir, s3client, mdb, group, sp, stdi)
		if err != nil {
			break
		}
		select {
		case <-compactLogsDoneCtx.Done():
			return WorkResultTryAgainLater, errors.New("Asked to shut down, will retry work")
		default:
		}
	}

	if err != nil {
		return WorkResultTryAgainLater, err
	}
	ll.Info("Successfully packed segments", slog.Int("groupCount", len(packed)))
	return WorkResultSuccess, nil
}

// timeToDateint computes the dateint for the current time.  This is YYYYMMDD as an int32.
func timeToDateint(t time.Time) int32 {
	return int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
}

var dropFieldNames = []string{
	"minute",
	"hour",
	"day",
	"month",
	"year",
}

func firstFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int64 {
	if len(group) == 0 {
		return 0
	}
	first := group[0].StartTs
	for _, segment := range group {
		first = min(first, segment.StartTs)
	}
	return first
}

func lastFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int64 {
	last := int64(0)
	for _, segment := range group {
		last = max(last, segment.EndTs)
	}
	return last
}

func segmentIDsFrom(segments []lrdb.GetLogSegmentsForCompactionRow) []int64 {
	ids := make([]int64, len(segments))
	for i, segment := range segments {
		ids[i] = segment.SegmentID
	}
	return ids
}

func ingestDateintFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int32 {
	if len(group) == 0 {
		return 0
	}
	first := int32(0)
	for _, segment := range group {
		first = max(first, segment.IngestDateint)
	}
	return first
}
