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

package heartbeat

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeartbeater_BasicOperation(t *testing.T) {
	var callCount int64

	heartbeatFunc := func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	heartbeater := New(heartbeatFunc, 50*time.Millisecond, nil)
	cancel := heartbeater.Start(context.Background())

	// Let it run for a bit
	time.Sleep(125 * time.Millisecond)

	cancel()

	// Should have called at least 2 times (initial + at least one interval)
	calls := atomic.LoadInt64(&callCount)
	assert.GreaterOrEqual(t, calls, int64(2), "Should have called heartbeat function multiple times")
}

func TestHeartbeater_InitialHeartbeat(t *testing.T) {
	var callCount int64

	heartbeatFunc := func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	heartbeater := New(heartbeatFunc, 1*time.Hour, nil) // Long interval
	cancel := heartbeater.Start(context.Background())

	// Give it just enough time for the initial call
	time.Sleep(10 * time.Millisecond)

	cancel()

	// Should have called exactly once (initial heartbeat)
	calls := atomic.LoadInt64(&callCount)
	assert.Equal(t, int64(1), calls, "Should call heartbeat function immediately on start")
}

func TestHeartbeater_ContextCancellation(t *testing.T) {
	var callCount int64

	heartbeatFunc := func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	ctx, parentCancel := context.WithCancel(context.Background())
	heartbeater := New(heartbeatFunc, 50*time.Millisecond, nil)
	cancel := heartbeater.Start(ctx)
	defer cancel()

	// Let it run briefly
	time.Sleep(25 * time.Millisecond)

	// Cancel parent context
	parentCancel()

	// Give it time to stop
	time.Sleep(25 * time.Millisecond)

	callsBeforeStop := atomic.LoadInt64(&callCount)

	// Wait a bit more - should not get more calls
	time.Sleep(100 * time.Millisecond)

	callsAfterStop := atomic.LoadInt64(&callCount)

	assert.Equal(t, callsBeforeStop, callsAfterStop, "Should stop calling after context cancellation")
}

func TestHeartbeater_HeartbeatError(t *testing.T) {
	var callCount int64
	expectedErr := errors.New("heartbeat failed")

	heartbeatFunc := func(ctx context.Context) error {
		count := atomic.AddInt64(&callCount, 1)
		if count == 2 {
			return expectedErr // Fail on second call
		}
		return nil
	}

	heartbeater := New(heartbeatFunc, 50*time.Millisecond, nil)
	cancel := heartbeater.Start(context.Background())

	// Let it run through multiple calls including the error
	time.Sleep(125 * time.Millisecond)

	cancel()

	// Should have called multiple times despite the error
	calls := atomic.LoadInt64(&callCount)
	assert.GreaterOrEqual(t, calls, int64(2), "Should continue calling even after error")
}

func TestHeartbeater_CancelFunction(t *testing.T) {
	var callCount int64

	heartbeatFunc := func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	heartbeater := New(heartbeatFunc, 50*time.Millisecond, nil)
	cancel := heartbeater.Start(context.Background())

	// Let it run briefly
	time.Sleep(75 * time.Millisecond)

	callsBeforeCancel := atomic.LoadInt64(&callCount)

	// Cancel via returned function
	cancel()

	// Wait and check that it stopped
	time.Sleep(100 * time.Millisecond)

	callsAfterCancel := atomic.LoadInt64(&callCount)

	assert.Greater(t, callsBeforeCancel, int64(0), "Should have made calls before cancel")
	assert.Equal(t, callsBeforeCancel, callsAfterCancel, "Should stop calling after cancel")
}

func TestHeartbeater_WithCustomLogger(t *testing.T) {
	var callCount int64

	heartbeatFunc := func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	logger := slog.New(slog.NewTextHandler(nil, nil))
	heartbeater := New(heartbeatFunc, 50*time.Millisecond, logger)
	cancel := heartbeater.Start(context.Background())

	time.Sleep(75 * time.Millisecond)
	cancel()

	// Should still work with custom logger
	calls := atomic.LoadInt64(&callCount)
	assert.GreaterOrEqual(t, calls, int64(1), "Should work with custom logger")
}

func TestHeartbeater_NilLogger(t *testing.T) {
	var callCount int64

	heartbeatFunc := func(ctx context.Context) error {
		atomic.AddInt64(&callCount, 1)
		return nil
	}

	// Should not panic with nil logger
	require.NotPanics(t, func() {
		heartbeater := New(heartbeatFunc, 50*time.Millisecond, nil)
		cancel := heartbeater.Start(context.Background())
		time.Sleep(75 * time.Millisecond)
		cancel()
	})

	calls := atomic.LoadInt64(&callCount)
	assert.GreaterOrEqual(t, calls, int64(1), "Should work with nil logger")
}
