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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name: "Test 4",
			id:   uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
			want: "f5lxx1zz5pnorynqglhzmsp33",
		},
		{
			name: "Random UUID 1",
			id:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
			want: "51a37iakuf5nuuphr0fx89og0",
		},
		{
			name: "Random UUID 2",
			id:   uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
			want: "6dfzh5ik5uxynzmlf5cshamdk",
		},
		{
			name: "Sequential UUID",
			id:   uuid.MustParse("00000000-0000-0000-0000-000000000100"),
			want: "0000000000000000000000074",
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
		{
			name:    "Empty string",
			base36:  "",
			want:    uuid.Nil,
			wantErr: true,
		},
		{
			name:    "Special characters",
			base36:  "12vqjrnxk8whv3i8qi6qgrl!@",
			want:    uuid.Nil,
			wantErr: true,
		},
		{
			name:    "Mixed case (uppercase)",
			base36:  "12VQJRNXK8WHV3I8QI6QGRLZ5",
			want:    uuid.MustParse("123e4567-e89b-12d3-a456-426614174001"),
			wantErr: false,
		},
		{
			name:    "Short base36 string",
			base36:  "1",
			want:    uuid.MustParse("00000000-0000-0000-0000-000000000001"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := base36ToUUID(tt.base36)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUUIDToBase36_FixedLength(t *testing.T) {
	tests := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000000"),
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		uuid.New(),
		uuid.New(),
		uuid.New(),
	}

	for _, id := range tests {
		base36 := UUIDToBase36(id)
		assert.Equal(t, 25, len(base36), "Base36 representation should always be 25 characters long for UUID %s", id)
		assert.True(t, strings.IndexFunc(base36, func(r rune) bool {
			return !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
		}) == -1, "Base36 should only contain lowercase alphanumeric characters")
	}
}

func TestRoundTripConversion(t *testing.T) {
	for range 100 {
		original := uuid.New()

		base36 := UUIDToBase36(original)
		recovered, err := base36ToUUID(base36)

		require.NoError(t, err, "Round trip conversion should not produce an error")
		assert.Equal(t, original, recovered, "Round trip conversion should preserve UUID value")
	}
}

func TestBase36ToUUID_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		base36  string
		wantErr bool
	}{
		{
			name:    "26 character string (too long for valid UUID)",
			base36:  "f5lxx1zz5pnorynqglhzmsp334",
			wantErr: true,
		},
		{
			name:    "24 character string (one short)",
			base36:  "f5lxx1zz5pnorynqglhzmsp3",
			wantErr: false,
		},
		{
			name:    "Exactly at 128-bit boundary",
			base36:  "f5lxx1zz5pnorynqglhzmsp33",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := base36ToUUID(tt.base36)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestUUIDToBase36_Consistency(t *testing.T) {
	id := uuid.MustParse("123e4567-e89b-12d3-a456-426614174001")

	result1 := UUIDToBase36(id)
	result2 := UUIDToBase36(id)

	assert.Equal(t, result1, result2, "Converting the same UUID should always produce the same result")
}

func BenchmarkUUIDToBase36(b *testing.B) {
	id := uuid.MustParse("123e4567-e89b-12d3-a456-426614174001")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = UUIDToBase36(id)
	}
}

func BenchmarkBase36ToUUID(b *testing.B) {
	base36 := "12vqjrnxk8whv3i8qi6qgrlz5"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = base36ToUUID(base36)
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	id := uuid.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base36 := UUIDToBase36(id)
		_, _ = base36ToUUID(base36)
	}
}
