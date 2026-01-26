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

package pubsub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
)

func TestGetCleanupRetention(t *testing.T) {
	// Test default retention
	cfg := &config.Config{
		PubSub: config.PubSubConfig{
			Dedup: config.PubSubDedupConfig{
				RetentionDuration: 24 * time.Hour,
				CleanupBatchSize:  1000,
			},
		},
	}
	retention := GetCleanupRetention(cfg)
	require.Equal(t, 24*time.Hour, retention)

	// Test with custom retention
	cfg.PubSub.Dedup.RetentionDuration = 12 * time.Hour
	retention = GetCleanupRetention(cfg)
	require.Equal(t, 12*time.Hour, retention)
}

func TestGetCleanupBatchSize(t *testing.T) {
	// Test default batch size
	cfg := &config.Config{
		PubSub: config.PubSubConfig{
			Dedup: config.PubSubDedupConfig{
				RetentionDuration: 24 * time.Hour,
				CleanupBatchSize:  1000,
			},
		},
	}
	batchSize := GetCleanupBatchSize(cfg)
	require.Equal(t, 1000, batchSize)

	// Test with custom batch size
	cfg.PubSub.Dedup.CleanupBatchSize = 500
	batchSize = GetCleanupBatchSize(cfg)
	require.Equal(t, 500, batchSize)
}
