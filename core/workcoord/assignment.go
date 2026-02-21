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
	"errors"

	"github.com/cespare/xxhash/v2"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

// AssignByRendezvous selects the best worker for a given affinity key using
// rendezvous (highest random weight) hashing. Only workers that are available
// (alive, accepting, not draining) are considered. Workers in the exclude set
// are skipped.
func AssignByRendezvous(affinityKey string, workers []WorkerInfo, exclude ...string) (string, error) {
	excludeSet := make(map[string]struct{}, len(exclude))
	for _, id := range exclude {
		excludeSet[id] = struct{}{}
	}

	var bestWorker string
	var bestHash uint64
	found := false

	for _, w := range workers {
		if !w.IsAvailable() {
			continue
		}
		if _, skip := excludeSet[w.WorkerID]; skip {
			continue
		}
		h := xxhash.Sum64String(affinityKey + w.WorkerID)
		if !found || h > bestHash {
			bestWorker = w.WorkerID
			bestHash = h
			found = true
		}
	}
	if !found {
		return "", ErrNoAvailableWorkers
	}
	return bestWorker, nil
}

// RankedWorkers returns workers sorted by rendezvous hash score (highest first)
// for a given affinity key. Only available workers are included.
// This is useful for reassignment: if the top-ranked worker fails, the next
// in the list is the preferred reassignment target.
func RankedWorkers(affinityKey string, workers []WorkerInfo) []string {
	type scored struct {
		workerID string
		hash     uint64
	}

	var candidates []scored
	for _, w := range workers {
		if !w.IsAvailable() {
			continue
		}
		candidates = append(candidates, scored{
			workerID: w.WorkerID,
			hash:     xxhash.Sum64String(affinityKey + w.WorkerID),
		})
	}

	// Sort by hash descending (insertion sort is fine for small worker counts).
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && candidates[j].hash > candidates[j-1].hash; j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}

	result := make([]string, len(candidates))
	for i, c := range candidates {
		result[i] = c.workerID
	}
	return result
}
