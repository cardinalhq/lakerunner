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
