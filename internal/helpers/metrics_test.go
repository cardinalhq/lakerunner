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
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestAllRolledUp(t *testing.T) {
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
			result := AllRolledUp(tt.rows)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRangeBounds_Int(t *testing.T) {
	tests := []struct {
		name      string
		r         pgtype.Range[int]
		wantLower int
		wantUpper int
		wantOK    bool
	}{
		{
			name: "inclusive bounds",
			r: pgtype.Range[int]{
				Lower:     10,
				Upper:     20,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: 10,
			wantUpper: 21, // exclusive, so +1
			wantOK:    true,
		},
		{
			name: "exclusive lower",
			r: pgtype.Range[int]{
				Lower:     10,
				Upper:     20,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: 11,
			wantUpper: 21, // exclusive, so +1
			wantOK:    true,
		},
		{
			name: "exclusive upper",
			r: pgtype.Range[int]{
				Lower:     10,
				Upper:     20,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: 10,
			wantUpper: 20, // already exclusive
			wantOK:    true,
		},
		{
			name: "both exclusive",
			r: pgtype.Range[int]{
				Lower:     10,
				Upper:     20,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: 11,
			wantUpper: 20, // already exclusive
			wantOK:    true,
		},
		{
			name: "lower Unbounded",
			r: pgtype.Range[int]{
				LowerType: pgtype.Unbounded,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantOK: false,
		},
		{
			name: "upper Unbounded",
			r: pgtype.Range[int]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Unbounded,
				Valid:     true,
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper, ok := RangeBounds(tt.r)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantLower, lower)
				assert.Equal(t, tt.wantUpper, upper)
			}
		})
	}
}

func TestRangeBounds_Int64(t *testing.T) {
	tests := []struct {
		name      string
		r         pgtype.Range[int64]
		wantLower int64
		wantUpper int64
		wantOK    bool
	}{
		{
			name: "inclusive bounds",
			r: pgtype.Range[int64]{
				Lower:     100,
				Upper:     200,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: 100,
			wantUpper: 201, // exclusive, so +1
			wantOK:    true,
		},
		{
			name: "exclusive lower",
			r: pgtype.Range[int64]{
				Lower:     100,
				Upper:     200,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: 101,
			wantUpper: 201, // exclusive, so +1
			wantOK:    true,
		},
		{
			name: "exclusive upper",
			r: pgtype.Range[int64]{
				Lower:     100,
				Upper:     200,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: 100,
			wantUpper: 200, // already exclusive
			wantOK:    true,
		},
		{
			name: "both exclusive",
			r: pgtype.Range[int64]{
				Lower:     100,
				Upper:     200,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: 101,
			wantUpper: 200, // already exclusive
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper, ok := RangeBounds(tt.r)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantLower, lower)
				assert.Equal(t, tt.wantUpper, upper)
			}
		})
	}
}

func TestRangeBounds_Time(t *testing.T) {
	baseLower := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	baseUpper := time.Date(2024, 6, 1, 13, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		r         pgtype.Range[time.Time]
		wantLower time.Time
		wantUpper time.Time
		wantOK    bool
	}{
		{
			name: "inclusive bounds",
			r: pgtype.Range[time.Time]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: baseLower,
			wantUpper: baseUpper.Add(time.Nanosecond), // exclusive, so +1ns
			wantOK:    true,
		},
		{
			name: "exclusive lower",
			r: pgtype.Range[time.Time]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: baseLower.Add(time.Nanosecond),
			wantUpper: baseUpper.Add(time.Nanosecond), // exclusive, so +1ns
			wantOK:    true,
		},
		{
			name: "exclusive upper",
			r: pgtype.Range[time.Time]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: baseLower,
			wantUpper: baseUpper, // already exclusive
			wantOK:    true,
		},
		{
			name: "both exclusive",
			r: pgtype.Range[time.Time]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: baseLower.Add(time.Nanosecond),
			wantUpper: baseUpper, // already exclusive
			wantOK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper, ok := RangeBounds(tt.r)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.True(t, lower.Equal(tt.wantLower), "lower: got %v, want %v", lower, tt.wantLower)
				assert.True(t, upper.Equal(tt.wantUpper), "upper: got %v, want %v", upper, tt.wantUpper)
			}
		})
	}
}

func TestRangeBounds_Timestamptz(t *testing.T) {
	baseLower := pgtype.Timestamptz{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	baseUpper := pgtype.Timestamptz{Time: time.Date(2024, 6, 1, 13, 0, 0, 0, time.UTC), Valid: true}

	tests := []struct {
		name      string
		r         pgtype.Range[pgtype.Timestamptz]
		wantLower pgtype.Timestamptz
		wantUpper pgtype.Timestamptz
		wantOK    bool
	}{
		{
			name: "inclusive bounds",
			r: pgtype.Range[pgtype.Timestamptz]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: baseLower,
			wantUpper: pgtype.Timestamptz{Time: baseUpper.Time.Add(time.Nanosecond), Valid: true},
			wantOK:    true,
		},
		{
			name: "exclusive lower",
			r: pgtype.Range[pgtype.Timestamptz]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: pgtype.Timestamptz{Time: baseLower.Time.Add(time.Nanosecond), Valid: true},
			wantUpper: pgtype.Timestamptz{Time: baseUpper.Time.Add(time.Nanosecond), Valid: true},
			wantOK:    true,
		},
		{
			name: "exclusive upper",
			r: pgtype.Range[pgtype.Timestamptz]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: baseLower,
			wantUpper: baseUpper,
			wantOK:    true,
		},
		{
			name: "both exclusive",
			r: pgtype.Range[pgtype.Timestamptz]{
				Lower:     baseLower,
				Upper:     baseUpper,
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: pgtype.Timestamptz{Time: baseLower.Time.Add(time.Nanosecond), Valid: true},
			wantUpper: baseUpper,
			wantOK:    true,
		},
		{
			name: "unbounded lower",
			r: pgtype.Range[pgtype.Timestamptz]{
				LowerType: pgtype.Unbounded,
				Upper:     baseUpper,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantOK: false,
		},
		{
			name: "unbounded upper",
			r: pgtype.Range[pgtype.Timestamptz]{
				Lower:     baseLower,
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Unbounded,
				Valid:     true,
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper, ok := RangeBounds(tt.r)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.True(t, lower.Time.Equal(tt.wantLower.Time), "lower: got %v, want %v", lower.Time, tt.wantLower.Time)
				assert.True(t, upper.Time.Equal(tt.wantUpper.Time), "upper: got %v, want %v", upper.Time, tt.wantUpper.Time)
			}
		})
	}
}

func TestRangeBounds_Int8(t *testing.T) {
	tests := []struct {
		name      string
		r         pgtype.Range[pgtype.Int8]
		wantLower pgtype.Int8
		wantUpper pgtype.Int8
		wantOK    bool
	}{
		{
			name: "inclusive bounds",
			r: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: 1000, Valid: true},
				Upper:     pgtype.Int8{Int64: 2000, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: pgtype.Int8{Int64: 1000, Valid: true},
			wantUpper: pgtype.Int8{Int64: 2001, Valid: true}, // exclusive, so +1
			wantOK:    true,
		},
		{
			name: "exclusive lower",
			r: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: 1000, Valid: true},
				Upper:     pgtype.Int8{Int64: 2000, Valid: true},
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantLower: pgtype.Int8{Int64: 1001, Valid: true},
			wantUpper: pgtype.Int8{Int64: 2001, Valid: true}, // exclusive, so +1
			wantOK:    true,
		},
		{
			name: "exclusive upper",
			r: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: 1000, Valid: true},
				Upper:     pgtype.Int8{Int64: 2000, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: pgtype.Int8{Int64: 1000, Valid: true},
			wantUpper: pgtype.Int8{Int64: 2000, Valid: true}, // already exclusive
			wantOK:    true,
		},
		{
			name: "both exclusive",
			r: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: 1000, Valid: true},
				Upper:     pgtype.Int8{Int64: 2000, Valid: true},
				LowerType: pgtype.Exclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			wantLower: pgtype.Int8{Int64: 1001, Valid: true},
			wantUpper: pgtype.Int8{Int64: 2000, Valid: true}, // already exclusive
			wantOK:    true,
		},
		{
			name: "lower Unbounded",
			r: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Unbounded,
				Upper:     pgtype.Int8{Int64: 2000, Valid: true},
				UpperType: pgtype.Inclusive,
				Valid:     true,
			},
			wantOK: false,
		},
		{
			name: "upper Unbounded",
			r: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: 1000, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Unbounded,
				Valid:     true,
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper, ok := RangeBounds(tt.r)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantLower.Int64, lower.Int64)
				assert.Equal(t, tt.wantUpper.Int64, upper.Int64)
				assert.Equal(t, tt.wantLower.Valid, lower.Valid)
				assert.Equal(t, tt.wantUpper.Valid, upper.Valid)
			}
		})
	}
}