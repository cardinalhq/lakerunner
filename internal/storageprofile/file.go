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

package storageprofile

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type fileProvider struct {
	profiles []StorageProfile
}

var _ StorageProfileProvider = (*fileProvider)(nil)

func NewFileProvider(filename string) (StorageProfileProvider, error) {
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
		return nil, fmt.Errorf("failed to read storage profiles from file %s: %w", filename, err)
	}

	return newFileProviderFromContents(filename, contents)
}

func newFileProviderFromContents(filename string, contents []byte) (StorageProfileProvider, error) {
	var profiles []StorageProfile

	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&profiles); err != nil {
		return nil, fmt.Errorf("failed to unmarshal storage profiles from file %s: %w", filename, err)
	}

	// if there is no role, set hosted to true
	for i := range profiles {
		if profiles[i].Role == "" {
			profiles[i].Hosted = true
		}
	}

	p := &fileProvider{
		profiles: profiles,
	}

	return p, nil
}

func (p *fileProvider) Get(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (StorageProfile, error) {
	for _, profile := range p.profiles {
		if profile.OrganizationID == organizationID && profile.InstanceNum == instanceNum {
			return profile, nil
		}
	}
	return StorageProfile{}, fmt.Errorf("storage profile not found for organization %s and instance %d", organizationID, instanceNum)
}

func (p *fileProvider) GetByCollectorName(ctx context.Context, organizationID uuid.UUID, collectorName string) (StorageProfile, error) {
	for _, profile := range p.profiles {
		if profile.OrganizationID == organizationID && profile.CollectorName == collectorName {
			return profile, nil
		}
	}
	return StorageProfile{}, fmt.Errorf("storage profile not found for organization %s and collector name %s", organizationID, collectorName)
}

func (p *fileProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error) {
	ret := []StorageProfile{}
	for _, profile := range p.profiles {
		if profile.Bucket == bucketName {
			ret = append(ret, profile)
		}
	}
	return ret, nil
}
