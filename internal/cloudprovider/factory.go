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

// NewManagerFromEnv creates a new provider manager using environment variable configuration
func NewManagerFromEnv() (*Manager, error) {
	config, err := LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}

	return NewManager(*config)
}

// GetDefaultManager returns a singleton manager instance configured from environment
var defaultManager *Manager

func GetDefaultManager() (*Manager, error) {
	if defaultManager == nil {
		var err error
		defaultManager, err = NewManagerFromEnv()
		if err != nil {
			return nil, err
		}
	}
	return defaultManager, nil
}
