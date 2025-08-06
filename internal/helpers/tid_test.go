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

package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMatchTags(t *testing.T) {
	tests := []struct {
		name     string
		existing map[string]any
		new      map[string]any
		want     map[string][]any
	}{
		{
			name:     "identical tags",
			existing: map[string]any{"a": "1", "b": "2"},
			new:      map[string]any{"a": "1", "b": "2"},
			want:     map[string][]any{},
		},
		{
			name:     "missing in new",
			existing: map[string]any{"a": "1", "b": "2"},
			new:      map[string]any{"a": "1"},
			want:     map[string][]any{"b": {"2", nil}},
		},
		{
			name:     "missing in existing",
			existing: map[string]any{"a": "1"},
			new:      map[string]any{"a": "1", "b": "2"},
			want:     map[string][]any{"b": {nil, "2"}},
		},
		{
			name:     "different values",
			existing: map[string]any{"a": "1", "b": "2"},
			new:      map[string]any{"a": "1", "b": "3"},
			want:     map[string][]any{"b": {"2", "3"}},
		},
		{
			name:     "multiple mismatches",
			existing: map[string]any{"a": "1", "b": "2"},
			new:      map[string]any{"a": "2", "c": "3"},
			want: map[string][]any{
				"a": {"1", "2"},
				"b": {"2", nil},
				"c": {nil, "3"},
			},
		},
		{
			name:     "empty maps",
			existing: map[string]any{},
			new:      map[string]any{},
			want:     map[string][]any{},
		},
		{
			name:     "existing empty, new has tags",
			existing: map[string]any{},
			new:      map[string]any{"x": "y"},
			want:     map[string][]any{"x": {nil, "y"}},
		},
		{
			name:     "new empty, existing has tags",
			existing: map[string]any{"x": "y"},
			new:      map[string]any{},
			want:     map[string][]any{"x": {"y", nil}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MatchTags(tt.existing, tt.new)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMakeTags(t *testing.T) {
	tests := []struct {
		name string
		rec  map[string]any
		want map[string]any
	}{
		{
			name: "all valid keys and values",
			rec:  map[string]any{"a": "1", "b": 2, "c": true},
			want: map[string]any{"a": "1", "b": 2, "c": true},
		},
		{
			name: "nil value is skipped",
			rec:  map[string]any{"a": nil, "b": 2},
			want: map[string]any{"b": 2},
		},
		{
			name: "empty key is skipped",
			rec:  map[string]any{"": "skip", "b": 2},
			want: map[string]any{"b": 2},
		},
		{
			name: "empty key and nil value both skipped",
			rec:  map[string]any{"": nil, "a": 1},
			want: map[string]any{"a": 1},
		},
		{
			name: "all skipped",
			rec:  map[string]any{"": nil},
			want: map[string]any{},
		},
		{
			name: "empty input",
			rec:  map[string]any{},
			want: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeTags(tt.rec)
			assert.Equal(t, tt.want, got)
		})
	}
}
