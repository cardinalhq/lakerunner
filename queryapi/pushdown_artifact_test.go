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
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/queryapi/workmanager"
)

// mockDispatcher implements workDispatcher for testing.
type mockDispatcher struct {
	fn func(ctx context.Context, queryID, leafID, affinityKey string, spec []byte) (*workmanager.WorkResult, error)
}

func (m *mockDispatcher) DispatchAndWait(ctx context.Context, queryID, leafID, affinityKey string, spec []byte) (*workmanager.WorkResult, error) {
	return m.fn(ctx, queryID, leafID, affinityKey, spec)
}

func TestDispatchPerSegment_EmptySegments(t *testing.T) {
	req := PushDownRequest{
		OrganizationID: uuid.New(),
	}
	results, errc, err := dispatchPerSegment(t.Context(), nil, "q1", "l1", req,
		func(_ *workmanager.WorkResult) ([]int, error) { return nil, nil })
	assert.NoError(t, err)
	assert.Nil(t, results)
	assert.Nil(t, errc)
}

func TestDispatchPerSegment_DispatchError(t *testing.T) {
	disp := &mockDispatcher{
		fn: func(_ context.Context, _, _, _ string, _ []byte) (*workmanager.WorkResult, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	req := PushDownRequest{
		OrganizationID: uuid.New(),
		Segments:       []SegmentInfo{{SegmentID: 42}, {SegmentID: 43}},
	}

	chs, errc, err := dispatchPerSegment(t.Context(), disp, "q1", "l1", req,
		func(r *workmanager.WorkResult) ([]int, error) {
			return []int{1, 2}, nil
		})
	require.NoError(t, err)
	assert.Len(t, chs, 2)

	// Drain data channels — they should close with no items.
	for _, ch := range chs {
		var items []int
		for v := range ch {
			items = append(items, v)
		}
		assert.Empty(t, items)
	}

	// errc should have errors from both segments.
	segErr := drainErrors(errc)
	require.Error(t, segErr)
	assert.Contains(t, segErr.Error(), "segment 42 dispatch")
	assert.Contains(t, segErr.Error(), "segment 43 dispatch")
}

func TestDispatchPerSegment_ParseError(t *testing.T) {
	disp := &mockDispatcher{
		fn: func(_ context.Context, _, _, _ string, _ []byte) (*workmanager.WorkResult, error) {
			return &workmanager.WorkResult{RowCount: 10, ArtifactData: []byte("bad")}, nil
		},
	}

	req := PushDownRequest{
		OrganizationID: uuid.New(),
		Segments:       []SegmentInfo{{SegmentID: 7}},
	}

	chs, errc, err := dispatchPerSegment(t.Context(), disp, "q1", "l1", req,
		func(_ *workmanager.WorkResult) ([]int, error) {
			return nil, fmt.Errorf("invalid parquet")
		})
	require.NoError(t, err)

	// Drain data channel.
	for range chs[0] {
	}

	segErr := drainErrors(errc)
	require.Error(t, segErr)
	assert.Contains(t, segErr.Error(), "segment 7 parse")
	assert.Contains(t, segErr.Error(), "invalid parquet")
}

func TestDispatchPerSegment_Success(t *testing.T) {
	disp := &mockDispatcher{
		fn: func(_ context.Context, _, _, _ string, _ []byte) (*workmanager.WorkResult, error) {
			return &workmanager.WorkResult{RowCount: 2, ArtifactData: []byte("ok")}, nil
		},
	}

	req := PushDownRequest{
		OrganizationID: uuid.New(),
		Segments:       []SegmentInfo{{SegmentID: 1}},
	}

	chs, errc, err := dispatchPerSegment(t.Context(), disp, "q1", "l1", req,
		func(_ *workmanager.WorkResult) ([]int, error) {
			return []int{10, 20}, nil
		})
	require.NoError(t, err)

	var items []int
	for v := range chs[0] {
		items = append(items, v)
	}
	assert.Equal(t, []int{10, 20}, items)
	assert.NoError(t, drainErrors(errc))
}

func TestDispatchPerSegment_MixedSuccessAndFailure(t *testing.T) {
	disp := &mockDispatcher{
		fn: func(_ context.Context, _, _, affinityKey string, _ []byte) (*workmanager.WorkResult, error) {
			if affinityKey == "100" {
				return nil, fmt.Errorf("worker down")
			}
			return &workmanager.WorkResult{RowCount: 1, ArtifactData: []byte("ok")}, nil
		},
	}

	req := PushDownRequest{
		OrganizationID: uuid.New(),
		Segments:       []SegmentInfo{{SegmentID: 100}, {SegmentID: 200}},
	}

	chs, errc, err := dispatchPerSegment(t.Context(), disp, "q1", "l1", req,
		func(_ *workmanager.WorkResult) ([]int, error) {
			return []int{99}, nil
		})
	require.NoError(t, err)
	assert.Len(t, chs, 2)

	// Collect all items from both channels.
	var allItems []int
	for _, ch := range chs {
		for v := range ch {
			allItems = append(allItems, v)
		}
	}
	// Only segment 200 succeeded.
	assert.Equal(t, []int{99}, allItems)

	// errc should report the segment 100 failure.
	segErr := drainErrors(errc)
	require.Error(t, segErr)
	assert.Contains(t, segErr.Error(), "segment 100 dispatch")
	assert.NotContains(t, segErr.Error(), "segment 200")
}

func TestDispatchPerSegment_CancelledContext_NoErrorOnErrc(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	disp := &mockDispatcher{
		fn: func(ctx context.Context, _, _, _ string, _ []byte) (*workmanager.WorkResult, error) {
			return nil, ctx.Err()
		},
	}

	req := PushDownRequest{
		OrganizationID: uuid.New(),
		Segments:       []SegmentInfo{{SegmentID: 1}},
	}

	chs, errc, err := dispatchPerSegment(ctx, disp, "q1", "l1", req,
		func(_ *workmanager.WorkResult) ([]int, error) { return nil, nil })
	require.NoError(t, err)

	for range chs[0] {
	}
	assert.NoError(t, drainErrors(errc), "cancelled context should not produce segment errors")
}

func TestDrainErrors_Nil(t *testing.T) {
	assert.NoError(t, drainErrors(nil))
}

func TestDrainErrors_NoErrors(t *testing.T) {
	ch := make(chan error)
	close(ch)
	assert.NoError(t, drainErrors(ch))
}

func TestDrainErrors_CollectsErrors(t *testing.T) {
	ch := make(chan error, 3)
	ch <- fmt.Errorf("err1")
	ch <- fmt.Errorf("err2")
	close(ch)

	err := drainErrors(ch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "err1")
	assert.Contains(t, err.Error(), "err2")
}

func TestMergeErrChans_FansIn(t *testing.T) {
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	ch1 <- fmt.Errorf("from-ch1")
	close(ch1)
	ch2 <- fmt.Errorf("from-ch2")
	close(ch2)

	merged := mergeErrChans([]<-chan error{ch1, ch2})
	err := drainErrors(merged)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "from-ch1")
	assert.Contains(t, err.Error(), "from-ch2")
}

func TestMergeErrChans_NilChannels(t *testing.T) {
	merged := mergeErrChans([]<-chan error{nil, nil})
	err := drainErrors(merged)
	assert.NoError(t, err)
}
