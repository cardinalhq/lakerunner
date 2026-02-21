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

package workcoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerInfo_IsAvailable(t *testing.T) {
	tests := []struct {
		name      string
		info      WorkerInfo
		available bool
	}{
		{"alive+accepting", WorkerInfo{Alive: true, AcceptingWork: true, Draining: false}, true},
		{"not alive", WorkerInfo{Alive: false, AcceptingWork: true, Draining: false}, false},
		{"not accepting", WorkerInfo{Alive: true, AcceptingWork: false, Draining: false}, false},
		{"draining", WorkerInfo{Alive: true, AcceptingWork: false, Draining: true}, false},
		{"all false", WorkerInfo{Alive: false, AcceptingWork: false, Draining: false}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.available, tt.info.IsAvailable())
		})
	}
}

func TestWorkerRegistry_Register(t *testing.T) {
	r := NewWorkerRegistry()

	require.NoError(t, r.Register("w1"))
	assert.Equal(t, 1, r.Count())

	w, err := r.Get("w1")
	require.NoError(t, err)
	assert.True(t, w.Alive)
	assert.True(t, w.AcceptingWork)
	assert.False(t, w.Draining)
}

func TestWorkerRegistry_RegisterDuplicate(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))

	err := r.Register("w1")
	require.Error(t, err)
	var dupErr *ErrWorkerAlreadyRegistered
	require.ErrorAs(t, err, &dupErr)
	assert.Equal(t, "w1", dupErr.WorkerID)
}

func TestWorkerRegistry_Remove(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.Remove("w1"))
	assert.Equal(t, 0, r.Count())

	_, err := r.Get("w1")
	var notFound *ErrWorkerNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestWorkerRegistry_RemoveNotFound(t *testing.T) {
	r := NewWorkerRegistry()
	err := r.Remove("nonexistent")
	var notFound *ErrWorkerNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestWorkerRegistry_Disconnect(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.Disconnect("w1"))

	w, err := r.Get("w1")
	require.NoError(t, err)
	assert.False(t, w.Alive)
	assert.False(t, w.AcceptingWork)
	assert.False(t, w.IsAvailable())
}

func TestWorkerRegistry_DisconnectNotFound(t *testing.T) {
	r := NewWorkerRegistry()
	err := r.Disconnect("nonexistent")
	var notFound *ErrWorkerNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestWorkerRegistry_Reconnect(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.Disconnect("w1"))
	require.NoError(t, r.Reconnect("w1"))

	w, err := r.Get("w1")
	require.NoError(t, err)
	assert.True(t, w.Alive)
	assert.True(t, w.AcceptingWork)
	assert.False(t, w.Draining)
	assert.True(t, w.IsAvailable())
}

func TestWorkerRegistry_ReconnectClearsDrain(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.BeginDrain("w1"))
	require.NoError(t, r.Disconnect("w1"))
	require.NoError(t, r.Reconnect("w1"))

	w, err := r.Get("w1")
	require.NoError(t, err)
	assert.True(t, w.Alive)
	assert.True(t, w.AcceptingWork)
	assert.False(t, w.Draining)
}

func TestWorkerRegistry_BeginDrain(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.BeginDrain("w1"))

	w, err := r.Get("w1")
	require.NoError(t, err)
	assert.True(t, w.Alive)
	assert.False(t, w.AcceptingWork)
	assert.True(t, w.Draining)
	assert.False(t, w.IsAvailable())
}

func TestWorkerRegistry_BeginDrainNotFound(t *testing.T) {
	r := NewWorkerRegistry()
	err := r.BeginDrain("nonexistent")
	var notFound *ErrWorkerNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestWorkerRegistry_SetAcceptingWork(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.SetAcceptingWork("w1", false))

	w, err := r.Get("w1")
	require.NoError(t, err)
	assert.False(t, w.AcceptingWork)

	require.NoError(t, r.SetAcceptingWork("w1", true))
	w, err = r.Get("w1")
	require.NoError(t, err)
	assert.True(t, w.AcceptingWork)
}

func TestWorkerRegistry_AvailableWorkers(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.Register("w2"))
	require.NoError(t, r.Register("w3"))

	// All available initially.
	avail := r.AvailableWorkers()
	assert.Len(t, avail, 3)

	// Disconnect w1, drain w2.
	require.NoError(t, r.Disconnect("w1"))
	require.NoError(t, r.BeginDrain("w2"))

	avail = r.AvailableWorkers()
	assert.Len(t, avail, 1)
	assert.Equal(t, "w3", avail[0].WorkerID)
}

func TestWorkerRegistry_AllWorkers(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))
	require.NoError(t, r.Register("w2"))
	require.NoError(t, r.Disconnect("w1"))

	all := r.AllWorkers()
	assert.Len(t, all, 2)
}

func TestWorkerRegistry_GetReturnsSnapshot(t *testing.T) {
	r := NewWorkerRegistry()
	require.NoError(t, r.Register("w1"))

	w, err := r.Get("w1")
	require.NoError(t, err)

	// Modifying the returned copy should not affect the registry.
	w.Alive = false
	assert.False(t, w.Alive) // sanity check mutation happened
	w2, err := r.Get("w1")
	require.NoError(t, err)
	assert.True(t, w2.Alive)
}
