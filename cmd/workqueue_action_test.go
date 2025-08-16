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
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func TestFrequenciesToRequest_Logs(t *testing.T) {
	tests := []struct {
		name      string
		signal    string
		action    string
		want      []int32
		wantError bool
	}{
		{
			name:      "logs compact",
			signal:    "logs",
			action:    "compact",
			want:      []int32{-1},
			wantError: false,
		},
		{
			name:      "logs rollup (unknown action)",
			signal:    "logs",
			action:    "rollup",
			wantError: true,
		},
		{
			name:      "metrics compact",
			signal:    "metrics",
			action:    "compact",
			want:      helpers.AcceptedMetricFrequencies,
			wantError: false,
		},
		{
			name:      "metrics rollup",
			signal:    "metrics",
			action:    "rollup",
			want:      []int32{60_000, 300_000, 1_200_000, 3_600_000},
			wantError: false,
		},
		{
			name:      "unknown metrics action",
			signal:    "metrics",
			action:    "unknown",
			wantError: true,
		},
		{
			name:      "unknown signal",
			signal:    "unknown",
			action:    "compact",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := frequenciesToRequest(tt.signal, tt.action)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}
