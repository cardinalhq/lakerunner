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

package lrdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreatedBy_String(t *testing.T) {
	tests := []struct {
		input    CreatedBy
		expected string
	}{
		{CreatedByUnknown, "unknown"},
		{CreatedByIngest, "ingest"},
		{CreatedByCompact, "compact"},
		{CreatedByRollup, "rollup"},
		{CreatedBy(99), "unknown"},
		{CreatedBy(-1), "unknown"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tt.input.String(), "CreatedBy(%d).String()", tt.input)
	}
}
