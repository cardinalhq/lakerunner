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
