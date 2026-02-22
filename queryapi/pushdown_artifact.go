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

package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi/workmanager"
)

// workDispatcher abstracts the dispatch-and-wait operation for testing.
type workDispatcher interface {
	DispatchAndWait(ctx context.Context, queryID, leafID, affinityKey string, spec []byte) (*workmanager.WorkResult, error)
}

// dispatchPerSegment fans out one work item per segment using segment ID as
// the rendezvous affinity key. Each returned channel streams that segment's
// sorted results independently so that MergeSorted can merge them correctly.
// The parse function converts a WorkResult into a typed slice.
//
// The returned error channel receives one error per failed segment and is
// closed when all segment goroutines have finished. Callers must drain it
// after consuming the data channels to detect partial failures.
func dispatchPerSegment[T any](
	ctx context.Context,
	mgr workDispatcher,
	queryID, leafID string,
	req PushDownRequest,
	parse func(*workmanager.WorkResult) ([]T, error),
) ([]<-chan T, <-chan error, error) {
	if len(req.Segments) == 0 {
		return nil, nil, nil
	}

	chans := make([]<-chan T, len(req.Segments))
	errc := make(chan error, len(req.Segments))
	var wg sync.WaitGroup
	wg.Add(len(req.Segments))

	for i, seg := range req.Segments {
		segReq := req
		segReq.Segments = []SegmentInfo{seg}
		spec, err := json.Marshal(segReq)
		if err != nil {
			return nil, nil, fmt.Errorf("marshal spec for segment %d: %w", seg.SegmentID, err)
		}
		affinityKey := fmt.Sprintf("%d", seg.SegmentID)
		segLeafID := fmt.Sprintf("%s-s%d", leafID, i)

		ch := make(chan T, 256)
		chans[i] = ch

		go func() {
			defer wg.Done()
			defer close(ch)
			result, err := mgr.DispatchAndWait(ctx, queryID, segLeafID, affinityKey, spec)
			if err != nil {
				if ctx.Err() == nil {
					errc <- fmt.Errorf("segment %d dispatch: %w", seg.SegmentID, err)
				}
				return
			}
			if result == nil || result.RowCount == 0 {
				return
			}
			items, err := parse(result)
			if err != nil {
				errc <- fmt.Errorf("segment %d parse: %w", seg.SegmentID, err)
				return
			}
			for _, item := range items {
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errc)
	}()

	return chans, errc, nil
}

// drainErrors collects all errors from an error channel and returns them
// joined. Returns nil when there are no errors or errc is nil.
func drainErrors(errc <-chan error) error {
	if errc == nil {
		return nil
	}
	var errs []error
	for err := range errc {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// mergeErrChans fans in multiple error channels into a single channel.
func mergeErrChans(errcs []<-chan error) <-chan error {
	out := make(chan error, len(errcs))
	var wg sync.WaitGroup
	for _, ec := range errcs {
		if ec == nil {
			continue
		}
		wg.Add(1)
		go func(c <-chan error) {
			defer wg.Done()
			for err := range c {
				out <- err
			}
		}(ec)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// metricsPushDownArtifact dispatches a metrics query per segment and returns
// one sorted channel per segment for MergeSorted consumption.
func (q *QuerierService) metricsPushDownArtifact(
	ctx context.Context,
	queryID, leafID string,
	request PushDownRequest,
) ([]<-chan promql.SketchInput, <-chan error, error) {
	return dispatchPerSegment(ctx, q.WorkMgr, queryID, leafID, request,
		func(r *workmanager.WorkResult) ([]promql.SketchInput, error) {
			return parseMetricsArtifact(r.ArtifactData, request)
		})
}

// summaryPushDownArtifact dispatches a summary query per segment and returns
// one channel per segment.
func (q *QuerierService) summaryPushDownArtifact(
	ctx context.Context,
	queryID, leafID string,
	request PushDownRequest,
) ([]<-chan promql.SketchInput, <-chan error, error) {
	return dispatchPerSegment(ctx, q.WorkMgr, queryID, leafID, request,
		func(r *workmanager.WorkResult) ([]promql.SketchInput, error) {
			return parseSummaryArtifact(r.ArtifactData, request)
		})
}

// logsPushDownArtifact dispatches a logs query per segment and returns
// one sorted channel per segment for MergeSorted consumption.
func (q *QuerierService) logsPushDownArtifact(
	ctx context.Context,
	queryID, leafID string,
	request PushDownRequest,
) ([]<-chan promql.Timestamped, <-chan error, error) {
	return dispatchPerSegment(ctx, q.WorkMgr, queryID, leafID, request,
		func(r *workmanager.WorkResult) ([]promql.Timestamped, error) {
			items, err := parseLogsArtifact(r.ArtifactData)
			if err != nil {
				return nil, err
			}
			ts := make([]promql.Timestamped, len(items))
			for i := range items {
				ts[i] = items[i]
			}
			return ts, nil
		})
}

// spansPushDownArtifact dispatches a spans query per segment and returns
// one sorted channel per segment for MergeSorted consumption.
func (q *QuerierService) spansPushDownArtifact(
	ctx context.Context,
	queryID, leafID string,
	request PushDownRequest,
) ([]<-chan promql.Timestamped, <-chan error, error) {
	return dispatchPerSegment(ctx, q.WorkMgr, queryID, leafID, request,
		func(r *workmanager.WorkResult) ([]promql.Timestamped, error) {
			items, err := parseLogsArtifact(r.ArtifactData)
			if err != nil {
				return nil, err
			}
			ts := make([]promql.Timestamped, len(items))
			for i := range items {
				ts[i] = items[i]
			}
			return ts, nil
		})
}

// tagValuesPushDownArtifact dispatches a tag values query per segment and
// returns one channel per segment.
func (q *QuerierService) tagValuesPushDownArtifact(
	ctx context.Context,
	queryID, leafID string,
	request PushDownRequest,
) ([]<-chan promql.TagValue, <-chan error, error) {
	return dispatchPerSegment(ctx, q.WorkMgr, queryID, leafID, request,
		func(r *workmanager.WorkResult) ([]promql.TagValue, error) {
			return parseTagValuesArtifact(r.ArtifactData)
		})
}
