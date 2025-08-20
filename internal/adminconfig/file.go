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

package adminconfig

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type fileProvider struct {
	config AdminConfig
}

var _ AdminConfigProvider = (*fileProvider)(nil)

func NewFileProvider(filename string) (AdminConfigProvider, error) {
	if after, ok := strings.CutPrefix(filename, "env:"); ok {
		envVar := after
		contents := os.Getenv(envVar)
		if contents == "" {
			return nil, fmt.Errorf("environment variable %s is not set", envVar)
		}
		return newFileProviderFromContents(filename, []byte(contents))
	}

	contents, err := os.ReadFile(filename)
	if err != nil {
		// If the file doesn't exist, create an empty provider for backward compatibility
		if os.IsNotExist(err) {
			return &fileProvider{config: AdminConfig{}}, nil
		}
		return nil, fmt.Errorf("failed to read admin config from file %s: %w", filename, err)
	}

	return newFileProviderFromContents(filename, contents)
}

func newFileProviderFromContents(filename string, contents []byte) (AdminConfigProvider, error) {
	var config AdminConfig

	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(false) // Allow unknown fields for backward compatibility
	if err := dec.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal admin config from file %s: %w", filename, err)
	}

	p := &fileProvider{
		config: config,
	}

	return p, nil
}

func (p *fileProvider) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	if len(p.config.APIKeys) == 0 {
		// If no API keys are configured, allow access for backward compatibility
		return true, nil
	}

	for _, key := range p.config.APIKeys {
		if subtle.ConstantTimeCompare([]byte(key.Key), []byte(apiKey)) == 1 {
			return true, nil
		}
	}
	return false, nil
}

func (p *fileProvider) GetAPIKeyInfo(ctx context.Context, apiKey string) (*AdminAPIKey, error) {
	for _, key := range p.config.APIKeys {
		if subtle.ConstantTimeCompare([]byte(key.Key), []byte(apiKey)) == 1 {
			// Return a copy without the actual key for security
			return &AdminAPIKey{
				Name:        key.Name,
				Description: key.Description,
			}, nil
		}
	}
	return nil, fmt.Errorf("API key not found")
}
