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

package ingestion

import "time"

// Config holds ingestion feature toggles.
type Config struct {
	ProcessExemplars    bool          `mapstructure:"process_exemplars"`
	SingleInstanceMode  bool          `mapstructure:"single_instance_mode"`
	MaxAccumulationTime time.Duration `mapstructure:"max_accumulation_time"`
}

// DefaultConfig returns default settings.
func DefaultConfig() Config {
	return Config{
		ProcessExemplars:    true,
		SingleInstanceMode:  false,
		MaxAccumulationTime: 10 * time.Second,
	}
}
