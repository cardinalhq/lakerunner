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

func TestWorkStateString(t *testing.T) {
	tests := []struct {
		state    WorkState
		expected string
	}{
		{WorkStateAssigned, "assigned"},
		{WorkStateAccepted, "accepted"},
		{WorkStateRejected, "rejected"},
		{WorkStateReady, "ready"},
		{WorkStateFailed, "failed"},
		{WorkStateCanceled, "canceled"},
		{WorkState(99), "unknown(99)"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.state.String())
	}
}

func TestWorkStateIsTerminal(t *testing.T) {
	tests := []struct {
		state    WorkState
		terminal bool
	}{
		{WorkStateAssigned, false},
		{WorkStateAccepted, false},
		{WorkStateRejected, true},
		{WorkStateReady, true},
		{WorkStateFailed, true},
		{WorkStateCanceled, true},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.terminal, tt.state.IsTerminal(), "state=%s", tt.state)
	}
}

func TestTransitionWork_ValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from WorkState
		to   WorkState
	}{
		{"assigned->accepted", WorkStateAssigned, WorkStateAccepted},
		{"assigned->rejected", WorkStateAssigned, WorkStateRejected},
		{"assigned->canceled", WorkStateAssigned, WorkStateCanceled},
		{"accepted->ready", WorkStateAccepted, WorkStateReady},
		{"accepted->failed", WorkStateAccepted, WorkStateFailed},
		{"accepted->canceled", WorkStateAccepted, WorkStateCanceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &WorkItem{State: tt.from}
			err := TransitionWork(item, tt.to)
			require.NoError(t, err)
			assert.Equal(t, tt.to, item.State)
		})
	}
}

func TestTransitionWork_InvalidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from WorkState
		to   WorkState
	}{
		// From assigned: cannot go to ready/failed directly.
		{"assigned->ready", WorkStateAssigned, WorkStateReady},
		{"assigned->failed", WorkStateAssigned, WorkStateFailed},
		// From accepted: cannot go back to assigned.
		{"accepted->assigned", WorkStateAccepted, WorkStateAssigned},
		{"accepted->rejected", WorkStateAccepted, WorkStateRejected},
		// From terminal states: no transitions allowed.
		{"rejected->accepted", WorkStateRejected, WorkStateAccepted},
		{"rejected->assigned", WorkStateRejected, WorkStateAssigned},
		{"ready->assigned", WorkStateReady, WorkStateAssigned},
		{"ready->canceled", WorkStateReady, WorkStateCanceled},
		{"failed->assigned", WorkStateFailed, WorkStateAssigned},
		{"failed->accepted", WorkStateFailed, WorkStateAccepted},
		{"canceled->assigned", WorkStateCanceled, WorkStateAssigned},
		{"canceled->accepted", WorkStateCanceled, WorkStateAccepted},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := &WorkItem{State: tt.from}
			err := TransitionWork(item, tt.to)
			require.Error(t, err)
			var invalidErr *ErrInvalidTransition
			require.ErrorAs(t, err, &invalidErr)
			assert.Equal(t, tt.from, invalidErr.From)
			assert.Equal(t, tt.to, invalidErr.To)
			// State should not have changed.
			assert.Equal(t, tt.from, item.State)
		})
	}
}

func TestTransitionWork_DoesNotMutateOnError(t *testing.T) {
	item := &WorkItem{
		QueryID:  "q1",
		LeafID:   "l1",
		WorkID:   "w1",
		WorkerID: "worker-1",
		State:    WorkStateReady,
	}
	err := TransitionWork(item, WorkStateAssigned)
	require.Error(t, err)
	assert.Equal(t, WorkStateReady, item.State)
	assert.Equal(t, "q1", item.QueryID)
}
