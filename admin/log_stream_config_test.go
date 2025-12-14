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

package admin

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/adminproto"
)

func TestGetLogStreamConfig_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.GetLogStreamConfigRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.GetLogStreamConfigRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.GetLogStreamConfigRequest{
				OrganizationId: "not-a-uuid",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.GetLogStreamConfig(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestSetLogStreamConfig_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.SetLogStreamConfigRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.SetLogStreamConfigRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "missing field name",
			req: &adminproto.SetLogStreamConfigRequest{
				OrganizationId: uuid.New().String(),
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "field_name is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.SetLogStreamConfigRequest{
				OrganizationId: "not-a-uuid",
				FieldName:      "resource_customer_domain",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.SetLogStreamConfig(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}

func TestDeleteLogStreamConfig_Validation(t *testing.T) {
	s := &Service{}
	ctx := context.Background()

	tests := []struct {
		name          string
		req           *adminproto.DeleteLogStreamConfigRequest
		expectedCode  codes.Code
		expectedError string
	}{
		{
			name:          "missing organization id",
			req:           &adminproto.DeleteLogStreamConfigRequest{},
			expectedCode:  codes.InvalidArgument,
			expectedError: "organization_id is required",
		},
		{
			name: "invalid organization id",
			req: &adminproto.DeleteLogStreamConfigRequest{
				OrganizationId: "not-a-uuid",
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "invalid organization_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.DeleteLogStreamConfig(ctx, tt.req)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, tt.expectedCode, st.Code())
			assert.Contains(t, st.Message(), tt.expectedError)
		})
	}
}
