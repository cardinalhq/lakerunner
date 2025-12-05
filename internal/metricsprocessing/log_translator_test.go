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

package metricsprocessing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
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

func TestLogTranslatingReader_setStreamID(t *testing.T) {
	tests := []struct {
		name           string
		inputRow       pipeline.Row
		customerDomain string
		expectedID     string
		shouldBeSet    bool
	}{
		{
			name:           "customer domain takes priority",
			inputRow:       pipeline.Row{wkk.RowKeyResourceServiceName: "my-service"},
			customerDomain: "customer.example.com",
			expectedID:     "customer.example.com",
			shouldBeSet:    true,
		},
		{
			name:           "falls back to resource_service_name",
			inputRow:       pipeline.Row{wkk.RowKeyResourceServiceName: "my-service"},
			customerDomain: "",
			expectedID:     "my-service",
			shouldBeSet:    true,
		},
		{
			name:           "omits if neither present",
			inputRow:       pipeline.Row{},
			customerDomain: "",
			expectedID:     "",
			shouldBeSet:    false,
		},
		{
			name:           "omits if service name is empty string",
			inputRow:       pipeline.Row{wkk.RowKeyResourceServiceName: ""},
			customerDomain: "",
			expectedID:     "",
			shouldBeSet:    false,
		},
		{
			name:           "customer domain overrides service name",
			inputRow:       pipeline.Row{wkk.RowKeyResourceServiceName: "fallback-service"},
			customerDomain: "primary-domain",
			expectedID:     "primary-domain",
			shouldBeSet:    true,
		},
	}

	r := &LogTranslatingReader{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := make(pipeline.Row)
			for k, v := range tt.inputRow {
				row[k] = v
			}

			r.setStreamID(&row, tt.customerDomain)

			streamID, exists := row[wkk.RowKeyCStreamID]
			if tt.shouldBeSet {
				assert.True(t, exists, "stream_id should be set")
				assert.Equal(t, tt.expectedID, streamID, "stream_id value mismatch")
			} else {
				assert.False(t, exists, "stream_id should not be set")
			}
		})
	}
}
