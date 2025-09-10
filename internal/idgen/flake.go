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

package idgen

import (
	"encoding/base32"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/sony/sonyflake"
)

var DefaultFlakeGenerator *SonyFlakeGenerator

func init() {
	var err error
	DefaultFlakeGenerator, err = newFlakeGenerator()
	if err != nil {
		panic(err)
	}
}

type SonyFlakeGenerator struct {
	sf *sonyflake.Sonyflake
}

// newFlakeGenerator creates a SonyFlakeGenerator.
func newFlakeGenerator() (*SonyFlakeGenerator, error) {
	settings := sonyflake.Settings{
		StartTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	sf, err := sonyflake.New(settings)
	if err != nil {
		return nil, err
	}
	if sf == nil {
		return nil, errors.New("failed to create Sonyflake instance")
	}
	return &SonyFlakeGenerator{sf: sf}, nil
}

// NextID returns a positive int64 that'll increase roughly in time order.
func (sf *SonyFlakeGenerator) NextID() int64 {
	v, err := sf.sf.NextID()
	if err != nil {
		return rand.Int64()
	}
	return int64(v)
}

// NextBase32ID generates a flake ID and encodes it as base32, removing any padding.
func (sf *SonyFlakeGenerator) NextBase32ID() string {
	id := sf.NextID()

	// Convert int64 to bytes (big endian)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(id))

	// Encode to base32 and remove padding
	encoded := base32.StdEncoding.EncodeToString(buf)
	return strings.TrimRight(encoded, "=")
}

// NextBase32ID is a convenience function that uses the default generator.
func NextBase32ID() string {
	return DefaultFlakeGenerator.NextBase32ID()
}

// GenerateID is a convenience function that uses the default generator to create int64 IDs.
func GenerateID() int64 {
	return DefaultFlakeGenerator.NextID()
}

// GenerateBatchIDs generates a batch of unique int64 IDs with collision detection.
// This ensures uniqueness within the batch, which is critical for database operations
// where multiple segments are created in the same transaction.
func GenerateBatchIDs(count int) []int64 {
	return DefaultFlakeGenerator.NextBatchIDs(count)
}

// NextBatchIDs generates a batch of unique IDs with collision detection
func (sf *SonyFlakeGenerator) NextBatchIDs(count int) []int64 {
	if count <= 0 {
		return nil
	}
	
	ids := make([]int64, count)
	seen := make(map[int64]bool, count)
	
	for i := 0; i < count; i++ {
		var id int64
		attempts := 0
		maxAttempts := 100 // Prevent infinite loops
		
		// Generate IDs until we get a unique one within this batch
		for {
			id = sf.NextID()
			if !seen[id] {
				break
			}
			attempts++
			if attempts >= maxAttempts {
				// Fallback: use base ID + sequential offset if we can't get unique IDs
				// This should be extremely rare with SonyFlake
				baseID := sf.NextID()
				for j := i; j < count; j++ {
					ids[j] = baseID + int64(j-i)
					seen[ids[j]] = true
				}
				return ids
			}
		}
		
		ids[i] = id
		seen[id] = true
	}
	
	return ids
}
