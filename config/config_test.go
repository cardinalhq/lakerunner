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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEnvOverride(t *testing.T) {
	t.Setenv("LAKERUNNER_FLY_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("LAKERUNNER_FLY_SASL_ENABLED", "true")
	t.Setenv("LAKERUNNER_FLY_SASL_USERNAME", "alice")
	t.Setenv("LAKERUNNER_METRICS_INGESTION_PROCESS_EXEMPLARS", "false")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.Fly.Brokers)
	require.True(t, cfg.Fly.SASLEnabled)
	require.Equal(t, "alice", cfg.Fly.SASLUsername)
	require.False(t, cfg.Metrics.Ingestion.ProcessExemplars)
}
