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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeWorkers(ids ...string) []WorkerInfo {
	workers := make([]WorkerInfo, len(ids))
	for i, id := range ids {
		workers[i] = WorkerInfo{
			WorkerID:      id,
			Alive:         true,
			AcceptingWork: true,
			Draining:      false,
		}
	}
	return workers
}

func TestAssignByRendezvous_NoWorkers(t *testing.T) {
	_, err := AssignByRendezvous("key", nil)
	require.ErrorIs(t, err, ErrNoAvailableWorkers)

	_, err = AssignByRendezvous("key", []WorkerInfo{})
	require.ErrorIs(t, err, ErrNoAvailableWorkers)
}

func TestAssignByRendezvous_AllUnavailable(t *testing.T) {
	workers := []WorkerInfo{
		{WorkerID: "w1", Alive: false, AcceptingWork: true},
		{WorkerID: "w2", Alive: true, AcceptingWork: false},
		{WorkerID: "w3", Alive: true, AcceptingWork: false, Draining: true},
	}
	_, err := AssignByRendezvous("key", workers)
	require.ErrorIs(t, err, ErrNoAvailableWorkers)
}

func TestAssignByRendezvous_SingleWorker(t *testing.T) {
	workers := makeWorkers("w1")
	id, err := AssignByRendezvous("any-key", workers)
	require.NoError(t, err)
	assert.Equal(t, "w1", id)
}

func TestAssignByRendezvous_Deterministic(t *testing.T) {
	workers := makeWorkers("w1", "w2", "w3")
	id1, _ := AssignByRendezvous("key-abc", workers)
	id2, _ := AssignByRendezvous("key-abc", workers)
	assert.Equal(t, id1, id2, "same key+workers must yield same assignment")
}

func TestAssignByRendezvous_DifferentKeysDistribute(t *testing.T) {
	workers := makeWorkers("w1", "w2", "w3", "w4")
	assignments := map[string]int{}

	for i := range 100 {
		key := fmt.Sprintf("key-%d", i)
		id, err := AssignByRendezvous(key, workers)
		require.NoError(t, err)
		assignments[id]++
	}
	// With 100 keys and 4 workers, each should get some assignments.
	assert.Len(t, assignments, 4, "all workers should get at least one assignment")
	for _, count := range assignments {
		assert.Greater(t, count, 0)
	}
}

func TestAssignByRendezvous_SkipsUnavailableWorkers(t *testing.T) {
	workers := []WorkerInfo{
		{WorkerID: "w1", Alive: false, AcceptingWork: true},
		{WorkerID: "w2", Alive: true, AcceptingWork: true},
		{WorkerID: "w3", Alive: true, AcceptingWork: false, Draining: true},
	}
	id, err := AssignByRendezvous("key", workers)
	require.NoError(t, err)
	assert.Equal(t, "w2", id)
}

func TestAssignByRendezvous_ExcludeWorker(t *testing.T) {
	workers := makeWorkers("w1", "w2")

	// Without exclude, one of them wins.
	winner, err := AssignByRendezvous("key", workers)
	require.NoError(t, err)

	// Excluding the winner forces the other.
	other, err := AssignByRendezvous("key", workers, winner)
	require.NoError(t, err)
	assert.NotEqual(t, winner, other)
}

func TestAssignByRendezvous_ExcludeAllReturnsError(t *testing.T) {
	workers := makeWorkers("w1")
	_, err := AssignByRendezvous("key", workers, "w1")
	require.ErrorIs(t, err, ErrNoAvailableWorkers)
}

func TestAssignByRendezvous_MinimalRemappingOnWorkerRemoval(t *testing.T) {
	workers4 := makeWorkers("w1", "w2", "w3", "w4")
	workers3 := makeWorkers("w1", "w2", "w3") // w4 removed

	changed := 0
	total := 200
	for i := range total {
		key := fmt.Sprintf("segment-%d", i)
		id4, _ := AssignByRendezvous(key, workers4)
		id3, _ := AssignByRendezvous(key, workers3)
		if id4 != id3 {
			changed++
		}
	}

	// With rendezvous hashing, removing 1 of 4 workers should remap ~25%
	// of keys. Allow some margin.
	maxExpected := total / 2 // generous upper bound
	assert.Less(t, changed, maxExpected,
		"rendezvous hashing should only remap keys from the removed worker")
	assert.Greater(t, changed, 0, "some keys should remap when a worker is removed")
}

func TestRankedWorkers_OrderIsConsistent(t *testing.T) {
	workers := makeWorkers("w1", "w2", "w3")
	ranked1 := RankedWorkers("key-x", workers)
	ranked2 := RankedWorkers("key-x", workers)
	assert.Equal(t, ranked1, ranked2)
}

func TestRankedWorkers_ContainsOnlyAvailable(t *testing.T) {
	workers := []WorkerInfo{
		{WorkerID: "w1", Alive: true, AcceptingWork: true},
		{WorkerID: "w2", Alive: false, AcceptingWork: true},
		{WorkerID: "w3", Alive: true, AcceptingWork: true},
	}
	ranked := RankedWorkers("key", workers)
	assert.Len(t, ranked, 2)
	for _, id := range ranked {
		assert.NotEqual(t, "w2", id)
	}
}

func TestRankedWorkers_FirstMatchesAssign(t *testing.T) {
	workers := makeWorkers("w1", "w2", "w3")
	ranked := RankedWorkers("key-y", workers)
	assigned, err := AssignByRendezvous("key-y", workers)
	require.NoError(t, err)
	assert.Equal(t, assigned, ranked[0],
		"first ranked worker must match AssignByRendezvous result")
}

func TestRankedWorkers_Empty(t *testing.T) {
	ranked := RankedWorkers("key", nil)
	assert.Empty(t, ranked)
}
