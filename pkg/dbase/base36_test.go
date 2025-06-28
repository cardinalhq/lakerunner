// This file is part of CardinalHQ, Inc.
//
// CardinalHQ, Inc. proprietary and confidential.
// Unauthorized copying, distribution, or modification of this file,
// via any medium, is strictly prohibited without prior written consent.
//
// Copyright 2024-2025 CardinalHQ, Inc. All rights reserved.

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
