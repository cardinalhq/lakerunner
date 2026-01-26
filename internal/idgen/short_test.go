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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateShortBase32ID(t *testing.T) {
	// Test that it returns a string
	id := GenerateShortBase32ID()
	assert.IsType(t, "", id, "GenerateShortBase32ID should return a string")
	assert.NotEmpty(t, id, "GenerateShortBase32ID should return a non-empty string")

	// Test that multiple calls return different IDs
	id2 := GenerateShortBase32ID()
	assert.NotEqual(t, id, id2, "GenerateShortBase32ID should return different values on subsequent calls")
}
