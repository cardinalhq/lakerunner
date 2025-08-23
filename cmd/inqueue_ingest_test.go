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

package cmd

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestBatchOrgSafetyCheck(t *testing.T) {
	tests := []struct {
		name    string
		items   []lrdb.ClaimInqueueWorkBatchRow
		wantErr bool
		errMsg  string
	}{
		{
			name:    "EmptyBatch",
			items:   []lrdb.ClaimInqueueWorkBatchRow{},
			wantErr: false,
		},
		{
			name: "SingleItem",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
			},
			wantErr: false,
		},
		{
			name: "SameOrgBatch",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
			},
			wantErr: false,
		},
		{
			name: "DifferentOrgBatch",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440001")}, // Different org
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
			},
			wantErr: true,
			errMsg:  "batch safety check failed: item 1 has organization ID 550e8400-e29b-41d4-a716-446655440001, expected 550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "DifferentOrgAtEnd",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440002")}, // Different org at end
			},
			wantErr: true,
			errMsg:  "batch safety check failed: item 2 has organization ID 550e8400-e29b-41d4-a716-446655440002, expected 550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBatchOrganizationConsistency(tt.items)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), "batch safety check failed")
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
