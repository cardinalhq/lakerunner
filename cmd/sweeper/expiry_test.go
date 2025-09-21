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

package sweeper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/config"
)

func TestGetDefaultMaxAgeDays(t *testing.T) {
	tests := []struct {
		name       string
		signalType string
		cfg        *config.Config
		expected   int
	}{
		{
			name:       "returns -1 when config not set",
			signalType: "logs",
			cfg:        nil,
			expected:   -1,
		},
		{
			name:       "returns configured value",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDays: map[string]int{
						"logs": 7,
					},
				},
			},
			expected: 7,
		},
		{
			name:       "empty map returns -1",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDays: map[string]int{},
				},
			},
			expected: -1,
		},
		{
			name:       "zero value is valid (never expire)",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDays: map[string]int{
						"logs": 0,
					},
				},
			},
			expected: 0,
		},
		{
			name:       "negative value is passed through",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDays: map[string]int{
						"logs": -5,
					},
				},
			},
			expected: -5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDefaultMaxAgeDays(tt.cfg, tt.signalType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetBatchSize(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		expected int
	}{
		{
			name:     "fallback when config not set",
			cfg:      nil,
			expected: 20000,
		},
		{
			name: "custom value from config",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					BatchSize: 5000,
				},
			},
			expected: 5000,
		},
		{
			name: "zero value uses fallback",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					BatchSize: 0,
				},
			},
			expected: 20000,
		},
		{
			name: "negative value uses fallback",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					BatchSize: -100,
				},
			},
			expected: 20000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getBatchSize(tt.cfg)
			assert.Equal(t, tt.expected, result)
		})
	}
}
