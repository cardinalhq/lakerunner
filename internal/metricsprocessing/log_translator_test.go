// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package metricsprocessing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getResourceFile(t *testing.T) {
	tests := []struct {
		name     string
		objectid string
		want     string
	}{
		{
			name:     "Support present with following part",
			objectid: "foo/bar/Support/ticket123/file.txt",
			want:     "ticket123",
		},
		{
			name:     "Support present at end",
			objectid: "foo/Support",
			want:     "unknown",
		},
		{
			name:     "Support present with only one after",
			objectid: "Support/abc",
			want:     "abc",
		},
		{
			name:     "Support not present",
			objectid: "foo/bar/baz",
			want:     "unknown",
		},
		{
			name:     "Support present with mixed case",
			objectid: "foo/sUpPoRt/next",
			want:     "next",
		},
		{
			name:     "Support present multiple times, returns first after",
			objectid: "a/Support/b/Support/c",
			want:     "b",
		},
		{
			name:     "Support present at start",
			objectid: "Support/first/second",
			want:     "first",
		},
		{
			name:     "Empty string",
			objectid: "",
			want:     "unknown",
		},
		{
			name:     "Only Support",
			objectid: "Support",
			want:     "unknown",
		},
		{
			name:     "Support with trailing slash",
			objectid: "foo/Support/",
			want:     "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getResourceFile(tt.objectid)
			require.Equal(t, tt.want, got)
		})
	}
}
