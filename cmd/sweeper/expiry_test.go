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
			name:       "returns configured value for logs",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDaysLogs: 7,
				},
			},
			expected: 7,
		},
		{
			name:       "not configured returns -1",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDaysLogs: -1,
				},
			},
			expected: -1,
		},
		{
			name:       "zero value is valid (never expire)",
			signalType: "logs",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDaysLogs: 0,
				},
			},
			expected: 0,
		},
		{
			name:       "returns configured value for metrics",
			signalType: "metrics",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDaysMetrics: 30,
				},
			},
			expected: 30,
		},
		{
			name:       "returns configured value for traces",
			signalType: "traces",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDaysTraces: 14,
				},
			},
			expected: 14,
		},
		{
			name:       "unknown signal type returns -1",
			signalType: "unknown",
			cfg: &config.Config{
				Expiry: config.ExpiryConfig{
					DefaultMaxAgeDaysLogs: 7,
				},
			},
			expected: -1,
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
