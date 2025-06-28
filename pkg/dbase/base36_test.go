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

package dbase

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestUuidTobase36(t *testing.T) {
	tests := []struct {
		name string
		id   uuid.UUID
		want string
	}{
		{
			name: "Test 1",
			id:   uuid.MustParse("123e4567-e89b-12d3-a456-426614174001"),
			want: "12vqjrnxk8whv3i8qi6qgrlz5",
		},
		{
			name: "Test 2",
			id:   uuid.MustParse("00000000-0000-0000-0000-000000000000"),
			want: "0000000000000000000000000",
		},
		{
			name: "Test 3",
			id:   uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			want: "0000000000000000000000001",
		},
		{
			name: "Test 3",
			id:   uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
			want: "f5lxx1zz5pnorynqglhzmsp33",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UUIDToBase36(tt.id); got != tt.want {
				t.Errorf("uuidTobase36() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBase36ToUUID(t *testing.T) {
	tests := []struct {
		name    string
		base36  string
		want    uuid.UUID
		wantErr bool
	}{
		{
			name:    "Valid base36 string 1",
			base36:  "12vqjrnxk8whv3i8qi6qgrlz5",
			want:    uuid.MustParse("123e4567-e89b-12d3-a456-426614174001"),
			wantErr: false,
		},
		{
			name:    "Valid base36 nil uuid",
			base36:  "0000000000000000000000000",
			want:    uuid.MustParse("00000000-0000-0000-0000-000000000000"),
			wantErr: false,
		},
		{
			name:    "Valid base36 string 2",
			base36:  "0000000000000000000000001",
			want:    uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			wantErr: false,
		},
		{
			name:    "Valid base36 string 3",
			base36:  "f5lxx1zz5pnorynqglhzmsp33",
			want:    uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
			wantErr: false,
		},
		{
			name:    "Invalid base36 string",
			base36:  "invalid base36 string",
			want:    uuid.Nil,
			wantErr: true,
		},
		{
			name:    "Base36 string too large",
			base36:  "zzzzzzzzzzzzzzzzzzzzzzzzz",
			want:    uuid.Nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Base36ToUUID(tt.base36)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
