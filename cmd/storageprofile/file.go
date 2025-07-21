// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageprofile

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert/yaml"
)

type fileProvider struct {
	profiles []StorageProfile
}

var _ StorageProfileProvider = (*fileProvider)(nil)

func NewFileProvider(filename string) (StorageProfileProvider, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage profiles from file %s: %w", filename, err)
	}

	return newFileProviderFromContents(filename, contents)
}

func newFileProviderFromContents(filename string, contents []byte) (StorageProfileProvider, error) {
	var profiles []StorageProfile
	if err := yaml.Unmarshal(contents, &profiles); err != nil {
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
	return ret, fmt.Errorf("storage profile not found for bucket %s", bucketName)
}
