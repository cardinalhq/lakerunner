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

import "fmt"

// validWorkTransitions defines the allowed state transitions for work items.
var validWorkTransitions = map[WorkState]map[WorkState]bool{
	WorkStateAssigned: {
		WorkStateAccepted: true,
		WorkStateRejected: true,
		WorkStateCanceled: true,
	},
	WorkStateAccepted: {
		WorkStateReady:    true,
		WorkStateFailed:   true,
		WorkStateCanceled: true,
	},
}

// ErrInvalidTransition is returned when a work state transition is not allowed.
type ErrInvalidTransition struct {
	From WorkState
	To   WorkState
}

func (e *ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid work state transition: %s -> %s", e.From, e.To)
}

// TransitionWork validates and performs a state transition on a work item.
// Returns an error if the transition is not valid.
func TransitionWork(item *WorkItem, to WorkState) error {
	from := item.State
	targets, ok := validWorkTransitions[from]
	if !ok || !targets[to] {
		return &ErrInvalidTransition{From: from, To: to}
	}
	item.State = to
	return nil
}
