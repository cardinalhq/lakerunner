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
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

var (
	// Map the current frequency into the ones we feed to.
	rollupNotifications = map[int32]int32{
		10_000:    60_000,    // 10 seconds feeds 1m
		60_000:    300_000,   // 1 minute feeds 5m
		300_000:   1_200_000, // 5 minutes feeds 15m
		1_200_000: 3_600_000, // 20 minutes feeds 1h
	}

	// Map our current frequency into the one we feed from.
	rollupSources = map[int32]int32{
		60_000:    10_000,    // 1 minute is from 10 seconds
		300_000:   60_000,    // 5 minutes is from 1 minute
		1_200_000: 300_000,   // 20 minutes is from 5 minutes
		3_600_000: 1_200_000, // 1 hour is from 20 minutes
	}

	acceptedMetricFrequencies = []int32{
		10_000,    // 10 seconds
		60_000,    // 1 minute
		300_000,   // 5 minutes
		1_200_000, // 20 minutes
		3_600_000, // 1 hour
	}
)

func isWantedFrequency(frequency int32) bool {
	return slices.Contains(acceptedMetricFrequencies, frequency)
}

// allRolledUp returns true all rows are rolledup.
// If the slice is empty, the return value is junk.
func allRolledUp(rows []lrdb.MetricSeg) bool {
	for _, row := range rows {
		if !row.Rolledup {
			return false
		}
	}
	return true
}

// Return the bounds of the given range.  The returned start time is inclusive
// and the end time is exclusive.  If the range is unbounded, or cannot be converted
// to concrete bounds, the function returns ok=false and the bounds are invalid.
func RangeBounds[T int | int16 | int32 | int64 | pgtype.Int8 | time.Time | pgtype.Timestamptz](r pgtype.Range[T]) (lower, upper T, ok bool) {
	if r.LowerType != pgtype.Inclusive && r.LowerType != pgtype.Exclusive {
		return lower, upper, false
	}
	if r.UpperType != pgtype.Inclusive && r.UpperType != pgtype.Exclusive {
		return lower, upper, false
	}

	lower = r.Lower
	upper = r.Upper

	// Always return inclusive lower and exclusive upper.
	// Only accept inclusive or exclusive lower, and inclusive or exclusive upper.
	if r.LowerType != pgtype.Inclusive && r.LowerType != pgtype.Exclusive {
		return lower, upper, false
	}
	if r.UpperType != pgtype.Inclusive && r.UpperType != pgtype.Exclusive {
		return lower, upper, false
	}

	// If the input lower is exclusive, increment lower by 1ns or 1 (for ints).
	if r.LowerType == pgtype.Exclusive {
		switch v := any(lower).(type) {
		case time.Time:
			lower = any(v.Add(time.Nanosecond)).(T)
		case pgtype.Timestamptz:
			shifted := pgtype.Timestamptz{
				Time:  v.Time.Add(time.Nanosecond),
				Valid: true,
			}
			lower = any(shifted).(T)
		case pgtype.Int8:
			shifted := pgtype.Int8{
				Int64: v.Int64 + 1,
				Valid: true,
			}
			lower = any(shifted).(T)
		case int:
			lower = any(v + 1).(T)
		case int16:
			lower = any(v + 1).(T)
		case int32:
			lower = any(v + 1).(T)
		case int64:
			lower = any(v + 1).(T)
		default:
			return lower, upper, false
		}
	}

	// If the input upper is inclusive, increment upper by 1ns or 1 (for ints) to make it exclusive.
	if r.UpperType == pgtype.Inclusive {
		switch v := any(upper).(type) {
		case time.Time:
			upper = any(v.Add(time.Nanosecond)).(T)
		case pgtype.Timestamptz:
			shifted := pgtype.Timestamptz{
				Time:  v.Time.Add(time.Nanosecond),
				Valid: true,
			}
			upper = any(shifted).(T)
		case pgtype.Int8:
			shifted := pgtype.Int8{
				Int64: v.Int64 + 1,
				Valid: true,
			}
			upper = any(shifted).(T)
		case int:
			upper = any(v + 1).(T)
		case int16:
			upper = any(v + 1).(T)
		case int32:
			upper = any(v + 1).(T)
		case int64:
			upper = any(v + 1).(T)
		default:
			return lower, upper, false
		}
	}

	return lower, upper, true
}
