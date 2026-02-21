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

// WorkState represents the lifecycle state of a work item.
type WorkState int

const (
	WorkStateAssigned WorkState = iota
	WorkStateAccepted
	WorkStateRejected
	WorkStateReady
	WorkStateFailed
	WorkStateCanceled
)

func (s WorkState) String() string {
	switch s {
	case WorkStateAssigned:
		return "assigned"
	case WorkStateAccepted:
		return "accepted"
	case WorkStateRejected:
		return "rejected"
	case WorkStateReady:
		return "ready"
	case WorkStateFailed:
		return "failed"
	case WorkStateCanceled:
		return "canceled"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// IsTerminal returns true if no further transitions are valid from this state.
func (s WorkState) IsTerminal() bool {
	switch s {
	case WorkStateRejected, WorkStateReady, WorkStateFailed, WorkStateCanceled:
		return true
	default:
		return false
	}
}

// WorkItem represents a unit of work assigned to a worker.
type WorkItem struct {
	QueryID  string
	LeafID   string
	WorkID   string
	WorkerID string
	State    WorkState

	// AffinityKey is used for rendezvous hashing assignment.
	AffinityKey string

	// ArtifactInfo is populated when the work reaches WorkStateReady.
	ArtifactInfo *ArtifactInfo
}

// ArtifactInfo describes a completed work artifact.
type ArtifactInfo struct {
	ArtifactURL          string
	ArtifactSizeBytes    int64
	ArtifactChecksum     string
	RowCount             int64
	MinTs                int64
	MaxTs                int64
	ParquetSchemaVersion int32
}

// CompletionKey uniquely identifies a work completion for idempotency dedup.
// Includes work_id so that distinct work units within the same leaf are
// tracked independently, even if they produce the same checksum.
type CompletionKey struct {
	QueryID          string
	LeafID           string
	WorkID           string
	ArtifactChecksum string
}

// WorkerInfo represents a worker's current membership and status.
type WorkerInfo struct {
	WorkerID      string
	Alive         bool
	AcceptingWork bool
	Draining      bool
}

// IsAvailable returns true if the worker can accept new work.
func (w *WorkerInfo) IsAvailable() bool {
	return w.Alive && w.AcceptingWork && !w.Draining
}
