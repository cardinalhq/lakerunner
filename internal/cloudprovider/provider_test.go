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

package cloudprovider

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/pubsub"
)

func TestValidateProviderConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  ProviderConfig
		wantErr bool
	}{
		{
			name: "valid AWS config",
			config: ProviderConfig{
				Type: ProviderAWS,
				ObjectStore: ObjectStoreConfig{
					Bucket: "test-bucket",
					Region: "us-east-1",
				},
			},
			wantErr: false,
		},
		{
			name: "valid local config without bucket",
			config: ProviderConfig{
				Type: ProviderLocal,
				ObjectStore: ObjectStoreConfig{
					ProviderSettings: map[string]any{
						"base_path": "/tmp/test",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing provider type",
			config: ProviderConfig{
				ObjectStore: ObjectStoreConfig{
					Bucket: "test-bucket",
				},
			},
			wantErr: true,
		},
		{
			name: "missing bucket for non-local provider",
			config: ProviderConfig{
				Type: ProviderAWS,
				ObjectStore: ObjectStoreConfig{
					Region: "us-east-1",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateProviderConfig(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMapPubSubType(t *testing.T) {
	tests := []struct {
		input    string
		expected pubsub.BackendType
		wantErr  bool
	}{
		{"sqs", pubsub.BackendTypeSQS, false},
		{"gcp_pubsub", pubsub.BackendTypeGCPPubSub, false},
		{"http", pubsub.BackendTypeHTTP, false},
		{"local", pubsub.BackendTypeLocal, false},
		{"invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := mapPubSubType(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestNewManager(t *testing.T) {
	tests := []struct {
		name    string
		config  ProviderConfig
		wantErr bool
	}{
		{
			name: "valid config creates manager",
			config: ProviderConfig{
				Type: ProviderAWS,
				ObjectStore: ObjectStoreConfig{
					Bucket: "test-bucket",
					Region: "us-east-1",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid config returns error",
			config: ProviderConfig{
				Type: ProviderAWS,
				// Missing bucket
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewManager(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, manager)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, manager)
			}
		})
	}
}
