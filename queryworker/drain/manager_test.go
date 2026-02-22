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

package drain

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockControlStream struct {
	draining atomic.Bool
}

func (m *mockControlStream) SetDraining(d bool) {
	m.draining.Store(d)
}

type mockWorkDrainer struct {
	draining  atomic.Bool
	blockCh   chan struct{}
	failDrain bool
}

func (m *mockWorkDrainer) SetDraining(d bool) {
	m.draining.Store(d)
}

func (m *mockWorkDrainer) WaitForDrain(ctx context.Context) error {
	if m.blockCh != nil {
		select {
		case <-m.blockCh:
		case <-ctx.Done():
			return fmt.Errorf("drain timed out")
		}
	}
	if m.failDrain {
		return fmt.Errorf("drain failed")
	}
	return nil
}

func TestDrainManager_Drain(t *testing.T) {
	cs := &mockControlStream{}
	wm := &mockWorkDrainer{}
	mgr := NewManager(cs, wm)

	err := mgr.Drain(t.Context())
	require.NoError(t, err)
	assert.True(t, cs.draining.Load())
	assert.True(t, wm.draining.Load())
}

func TestDrainManager_DrainTimeout(t *testing.T) {
	cs := &mockControlStream{}
	wm := &mockWorkDrainer{blockCh: make(chan struct{})}
	mgr := NewManager(cs, wm)
	mgr.SetDrainTimeout(200 * time.Millisecond)

	err := mgr.Drain(t.Context())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestDrainManager_DrainWaitsForCompletion(t *testing.T) {
	cs := &mockControlStream{}
	blockCh := make(chan struct{})
	wm := &mockWorkDrainer{blockCh: blockCh}
	mgr := NewManager(cs, wm)

	done := make(chan error, 1)
	go func() {
		done <- mgr.Drain(t.Context())
	}()

	// Should not finish yet.
	select {
	case <-done:
		t.Fatal("drain returned too early")
	case <-time.After(100 * time.Millisecond):
	}

	close(blockCh)

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("drain timed out")
	}
}

func TestDrainManager_NilComponents(t *testing.T) {
	mgr := NewManager(nil, nil)
	err := mgr.Drain(t.Context())
	assert.NoError(t, err)
}
