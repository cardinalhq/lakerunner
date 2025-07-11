// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			want:      acceptedMetricFrequencies,
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
