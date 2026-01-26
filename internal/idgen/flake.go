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

package idgen

import (
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/sony/sonyflake"
)

var DefaultFlakeGenerator *sonyFlakeGenerator

func init() {
	var err error
	DefaultFlakeGenerator, err = newFlakeGenerator()
	if err != nil {
		panic(err)
	}
}

type sonyFlakeGenerator struct {
	sf *sonyflake.Sonyflake
}

// machineID returns a deterministic 16-bit machine ID based on POD_NAME and POD_IP.
// If it cannot determine a stable ID (e.g. no env vars or hash -> 0), it logs a warning
// and falls back to generating a random 16-bit ID from crypto/rand.
func machineID() (uint16, error) {
	podName := os.Getenv("POD_NAME")
	podIP := os.Getenv("POD_IP")

	var id uint16
	if podName != "" || podIP != "" {
		combined := podName + "|" + podIP
		h := sha1.Sum([]byte(combined))
		id = binary.BigEndian.Uint16(h[0:2])
	}

	if id == 0 {
		slog.Debug("machineID: POD_NAME/POD_IP not set or hash resulted in 0, falling back to random ID")
		id = uint16(rand.Uint32())
		if id == 0 {
			id = 1
		}
	}

	return id, nil
}

// newFlakeGenerator creates a sonyFlakeGenerator.
func newFlakeGenerator() (*sonyFlakeGenerator, error) {
	settings := sonyflake.Settings{
		StartTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		MachineID: machineID,
	}

	sf, err := sonyflake.New(settings)
	if err != nil {
		return nil, err
	}
	if sf == nil {
		return nil, errors.New("failed to create Sonyflake instance")
	}
	return &sonyFlakeGenerator{sf: sf}, nil
}

// NextID returns a positive int64 that'll increase roughly in time order.
func (sf *sonyFlakeGenerator) NextID() int64 {
	v, err := sf.sf.NextID()
	if err != nil {
		return rand.Int64()
	}
	return int64(v)
}

// NextBase32ID generates a flake ID and encodes it as base32, removing any padding.
func (sf *sonyFlakeGenerator) NextBase32ID() string {
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

// GenerateBatchIDs generates a batch of unique int64 IDs with collision detection.
// This ensures uniqueness within the batch, which is critical for database operations
// where multiple segments are created in the same transaction.
func GenerateBatchIDs(count int) []int64 {
	return DefaultFlakeGenerator.NextBatchIDs(count)
}

// NextBatchIDs generates a batch of unique IDs with collision detection
func (sf *sonyFlakeGenerator) NextBatchIDs(count int) []int64 {
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
