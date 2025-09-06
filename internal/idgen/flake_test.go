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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSonyFlakeGenerator_NextID(t *testing.T) {
	gen, err := newFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	// Check that subsequent IDs are increasing
	id := gen.NextID()
	id2 := gen.NextID()
	assert.Greater(t, id2, id, "NextID() did not return increasing id")
}

func TestSonyFlakeGenerator_NextBase32ID(t *testing.T) {
	gen, err := newFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	// Generate base32 IDs
	id1 := gen.NextBase32ID()
	id2 := gen.NextBase32ID()

	// Should be different
	assert.NotEqual(t, id1, id2, "NextBase32ID() returned duplicate IDs")

	// Should not contain padding
	assert.False(t, strings.Contains(id1, "="), "base32 ID should not contain padding")
	assert.False(t, strings.Contains(id2, "="), "base32 ID should not contain padding")

	// Should be non-empty strings
	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)

	// Test the convenience function
	id3 := NextBase32ID()
	assert.NotEmpty(t, id3)
	assert.False(t, strings.Contains(id3, "="), "base32 ID should not contain padding")
}
