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
	"regexp"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// V2 YAML structures
type configV2 struct {
	Version int              `yaml:"version"`
	Buckets []bucketConfigV2 `yaml:"buckets"`
}

type bucketConfigV2 struct {
	Name           string          `yaml:"name"`
	CloudProvider  string          `yaml:"cloud_provider"`
	Region         string          `yaml:"region"`
	Endpoint       string          `yaml:"endpoint,omitempty"`
	Role           string          `yaml:"role,omitempty"`
	Properties     map[string]any  `yaml:"properties,omitempty"`
	Organizations  []uuid.UUID     `yaml:"organizations"`
	PrefixMappings []prefixMapping `yaml:"prefix_mappings,omitempty"`
}

type prefixMapping struct {
	OrganizationID uuid.UUID `yaml:"organization_id"`
	Prefix         string    `yaml:"prefix"`
}

type fileProvider struct {
	profiles []StorageProfile
	version  int
	// v2 data structures
	bucketConfigs  map[string]bucketConfigV2
	orgToBuckets   map[uuid.UUID][]string
	prefixMappings map[string][]prefixMapping // bucket -> prefix mappings
}

var _ StorageProfileProvider = (*fileProvider)(nil)

// UUID regex for path parsing
var uuidRegex = regexp.MustCompile(`^/[^/]+/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/`)

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
	// Try to detect version first
	var versionCheck struct {
		Version int `yaml:"version"`
	}

	dec := yaml.NewDecoder(bytes.NewReader(contents))
	if err := dec.Decode(&versionCheck); err == nil && versionCheck.Version == 2 {
		return parseV2Config(filename, contents)
	}

	// Fall back to v1 format
	return parseV1Config(filename, contents)
}

func parseV1Config(filename string, contents []byte) (StorageProfileProvider, error) {
	var profiles []StorageProfile

	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&profiles); err != nil {
		return nil, fmt.Errorf("failed to unmarshal v1 storage profiles from file %s: %w", filename, err)
	}

	return &fileProvider{
		profiles: profiles,
		version:  1,
	}, nil
}

func parseV2Config(filename string, contents []byte) (StorageProfileProvider, error) {
	var config configV2

	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal v2 storage profiles from file %s: %w", filename, err)
	}

	// Build lookup maps
	bucketConfigs := make(map[string]bucketConfigV2)
	orgToBuckets := make(map[uuid.UUID][]string)
	prefixMappings := make(map[string][]prefixMapping)

	for _, bucket := range config.Buckets {
		bucketConfigs[bucket.Name] = bucket

		// Map organizations to buckets
		for _, orgID := range bucket.Organizations {
			orgToBuckets[orgID] = append(orgToBuckets[orgID], bucket.Name)
		}

		// Store prefix mappings
		if len(bucket.PrefixMappings) > 0 {
			prefixMappings[bucket.Name] = bucket.PrefixMappings
		}
	}

	return &fileProvider{
		version:        2,
		bucketConfigs:  bucketConfigs,
		orgToBuckets:   orgToBuckets,
		prefixMappings: prefixMappings,
	}, nil
}

func (p *fileProvider) Get(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (StorageProfile, error) {
	if p.version == 1 {
		for _, profile := range p.profiles {
			if profile.OrganizationID == organizationID && profile.InstanceNum == instanceNum {
				return profile, nil
			}
		}
		return StorageProfile{}, fmt.Errorf("storage profile not found for organization %s and instance %d", organizationID, instanceNum)
	}

	// V2: For backwards compatibility, return first bucket for the org
	if buckets, ok := p.orgToBuckets[organizationID]; ok && len(buckets) > 0 {
		bucketName := buckets[0]
		if bucket, exists := p.bucketConfigs[bucketName]; exists {
			return StorageProfile{
				OrganizationID: organizationID,
				InstanceNum:    1, // Default instance for v2
				CloudProvider:  bucket.CloudProvider,
				Region:         bucket.Region,
				Role:           bucket.Role,
				Bucket:         bucket.Name,
				Endpoint:       bucket.Endpoint,
			}, nil
		}
	}
	return StorageProfile{}, fmt.Errorf("storage profile not found for organization %s", organizationID)
}

func (p *fileProvider) GetByCollectorName(ctx context.Context, organizationID uuid.UUID, collectorName string) (StorageProfile, error) {
	if p.version == 1 {
		for _, profile := range p.profiles {
			if profile.OrganizationID == organizationID && profile.CollectorName == collectorName {
				return profile, nil
			}
		}
		return StorageProfile{}, fmt.Errorf("storage profile not found for organization %s and collector name %s", organizationID, collectorName)
	}

	// V2: Collector names are not used, fall back to first bucket
	return p.Get(ctx, organizationID, 1)
}

func (p *fileProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error) {
	if p.version == 1 {
		ret := []StorageProfile{}
		for _, profile := range p.profiles {
			if profile.Bucket == bucketName {
				ret = append(ret, profile)
			}
		}
		return ret, nil
	}

	// V2: Get all organizations for this bucket
	bucket, exists := p.bucketConfigs[bucketName]
	if !exists {
		return []StorageProfile{}, nil
	}

	ret := make([]StorageProfile, 0, len(bucket.Organizations))
	for _, orgID := range bucket.Organizations {
		profile := StorageProfile{
			OrganizationID: orgID,
			InstanceNum:    1,
			CloudProvider:  bucket.CloudProvider,
			Region:         bucket.Region,
			Role:           bucket.Role,
			Bucket:         bucket.Name,
			Endpoint:       bucket.Endpoint,
		}
		ret = append(ret, profile)
	}
	return ret, nil
}

func (p *fileProvider) ResolveOrganization(ctx context.Context, bucketName, objectPath string) (uuid.UUID, error) {
	if p.version == 1 {
		// V1: Use existing logic - expect exactly one profile per bucket
		profiles, err := p.GetStorageProfilesByBucketName(ctx, bucketName)
		if err != nil {
			return uuid.Nil, err
		}
		if len(profiles) != 1 {
			return uuid.Nil, fmt.Errorf("expected exactly one storage profile for bucket %s, found %d", bucketName, len(profiles))
		}
		return profiles[0].OrganizationID, nil
	}

	// V2: New resolution logic
	bucket, exists := p.bucketConfigs[bucketName]
	if !exists {
		return uuid.Nil, fmt.Errorf("bucket %s not found in configuration", bucketName)
	}

	// 1. Try to extract UUID from path
	if matches := uuidRegex.FindStringSubmatch(objectPath); len(matches) > 1 {
		orgID, err := uuid.Parse(matches[1])
		if err != nil {
			return uuid.Nil, fmt.Errorf("invalid UUID in path: %w", err)
		}

		// Verify this org has access to the bucket
		for _, validOrgID := range bucket.Organizations {
			if orgID == validOrgID {
				return orgID, nil
			}
		}
		return uuid.Nil, fmt.Errorf("organization %s does not have access to bucket %s", orgID, bucketName)
	}

	// 2. Try longest prefix match
	if prefixes, exists := p.prefixMappings[bucketName]; exists {
		var bestMatch *prefixMapping
		var bestLength int

		for _, mapping := range prefixes {
			if strings.HasPrefix(objectPath, mapping.Prefix) && len(mapping.Prefix) > bestLength {
				bestMatch = &mapping
				bestLength = len(mapping.Prefix)
			}
		}

		if bestMatch != nil {
			return bestMatch.OrganizationID, nil
		}
	}

	// 3. If single org owns the bucket, use that
	if len(bucket.Organizations) == 1 {
		return bucket.Organizations[0], nil
	}

	return uuid.Nil, fmt.Errorf("unable to resolve organization for path %s in bucket %s: ambiguous", objectPath, bucketName)
}
