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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestAnyRollupsNeeded(t *testing.T) {
	tests := []struct {
		name     string
		rows     []lrdb.MetricSeg
		expected bool
	}{
		{
			name: "all rolledup true",
			rows: []lrdb.MetricSeg{
				{Rolledup: true},
				{Rolledup: true},
			},
			expected: true,
		},
		{
			name: "one rolledup false",
			rows: []lrdb.MetricSeg{
				{Rolledup: true},
				{Rolledup: false},
			},
			expected: false,
		},
		{
			name: "all rolledup false",
			rows: []lrdb.MetricSeg{
				{Rolledup: false},
				{Rolledup: false},
			},
			expected: false,
		},
		{
			name: "mixed rolledup values",
			rows: []lrdb.MetricSeg{
				{Rolledup: true},
				{Rolledup: false},
				{Rolledup: true},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := helpers.AllRolledUp(tt.rows)
			assert.Equal(t, tt.expected, result)
		})
	}
}
