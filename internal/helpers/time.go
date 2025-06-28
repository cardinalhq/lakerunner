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

import "time"

// Converts milliseconds since epoch to (dateint, hour)
func MSToDateintHour(ms int64) (int32, int16) {
	t := UnixMillisToTime(ms).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	hour := int16(t.Hour())
	return dateint, hour
}

// Helper to convert ms since epoch to time.Time
func UnixMillisToTime(ms int64) time.Time {
	sec := ms / 1000
	nsec := (ms % 1000) * 1e6
	return time.Unix(sec, nsec).UTC()
}
