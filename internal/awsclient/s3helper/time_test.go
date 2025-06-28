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

package s3helper

import "testing"

func TestHourFromMillis(t *testing.T) {
	tests := []struct {
		millis int64
		hours  int16
	}{
		{0, 0},
		{3600000, 1},
		{7200000, 2},
		{10800000, 3},
		{1747942514321, 19},
	}

	for _, test := range tests {
		result := HourFromMillis(test.millis)
		if result != test.hours {
			t.Errorf("Expected %d hours, got %d", test.hours, result)
		}
	}
}
