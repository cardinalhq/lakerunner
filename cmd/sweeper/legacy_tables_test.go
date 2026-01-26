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
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cardinalhq/lakerunner/configdb"
)

type MockQueries struct {
	mock.Mock
}

func (m *MockQueries) GetAllCOrganizations(ctx context.Context) ([]configdb.GetAllCOrganizationsRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllCOrganizationsRow), args.Error(1)
}

func (m *MockQueries) GetAllOrganizations(ctx context.Context) ([]configdb.GetAllOrganizationsRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllOrganizationsRow), args.Error(1)
}

func (m *MockQueries) UpsertOrganizationSync(ctx context.Context, params configdb.UpsertOrganizationSyncParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockQueries) DeleteOrganizationsNotInList(ctx context.Context, ids []uuid.UUID) error {
	args := m.Called(ctx, ids)
	return args.Error(0)
}

func (m *MockQueries) GetAllCBucketData(ctx context.Context) ([]configdb.GetAllCBucketDataRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllCBucketDataRow), args.Error(1)
}

func (m *MockQueries) GetAllBucketConfigurations(ctx context.Context) ([]configdb.GetAllBucketConfigurationsRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllBucketConfigurationsRow), args.Error(1)
}

func (m *MockQueries) GetAllOrganizationBucketMappings(ctx context.Context) ([]configdb.GetAllOrganizationBucketMappingsRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllOrganizationBucketMappingsRow), args.Error(1)
}

func (m *MockQueries) UpsertBucketConfiguration(ctx context.Context, params configdb.UpsertBucketConfigurationParams) (configdb.BucketConfiguration, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(configdb.BucketConfiguration), args.Error(1)
}

func (m *MockQueries) UpsertOrganizationBucket(ctx context.Context, params configdb.UpsertOrganizationBucketParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockQueries) DeleteOrganizationBucketMappings(ctx context.Context, params configdb.DeleteOrganizationBucketMappingsParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockQueries) GetAllCOrganizationAPIKeys(ctx context.Context) ([]configdb.GetAllCOrganizationAPIKeysRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllCOrganizationAPIKeysRow), args.Error(1)
}

func (m *MockQueries) GetAllOrganizationAPIKeyMappings(ctx context.Context) ([]configdb.GetAllOrganizationAPIKeyMappingsRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetAllOrganizationAPIKeyMappingsRow), args.Error(1)
}

func (m *MockQueries) UpsertOrganizationAPIKey(ctx context.Context, params configdb.UpsertOrganizationAPIKeyParams) (configdb.OrganizationApiKey, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(configdb.OrganizationApiKey), args.Error(1)
}

func (m *MockQueries) UpsertOrganizationAPIKeyMapping(ctx context.Context, params configdb.UpsertOrganizationAPIKeyMappingParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockQueries) DeleteOrganizationAPIKeyMappingByHash(ctx context.Context, params configdb.DeleteOrganizationAPIKeyMappingByHashParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func TestSyncOrganizations(t *testing.T) {
	ctx := context.Background()
	mockQueries := new(MockQueries)

	org1ID := uuid.New()
	org2ID := uuid.New()
	org3ID := uuid.New()

	name1 := "Org1"
	name2 := "Org2"
	name3 := "Org3"

	// Mock c_ organizations
	cOrgs := []configdb.GetAllCOrganizationsRow{
		{ID: org1ID, Name: &name1, Enabled: pgtype.Bool{Bool: true, Valid: true}},
		{ID: org2ID, Name: &name2, Enabled: pgtype.Bool{Bool: false, Valid: true}},
	}

	// Mock our organizations (org1 exists with different data, org3 should be deleted)
	ourOrgs := []configdb.GetAllOrganizationsRow{
		{ID: org1ID, Name: "OldName", Enabled: true},
		{ID: org3ID, Name: name3, Enabled: true},
	}

	mockQueries.On("GetAllCOrganizations", ctx).Return(cOrgs, nil)
	mockQueries.On("GetAllOrganizations", ctx).Return(ourOrgs, nil)

	// Expect updates for org1 (name changed) and org2 (new)
	mockQueries.On("UpsertOrganizationSync", ctx, configdb.UpsertOrganizationSyncParams{
		ID:      org1ID,
		Name:    name1,
		Enabled: true,
	}).Return(nil)

	mockQueries.On("UpsertOrganizationSync", ctx, configdb.UpsertOrganizationSyncParams{
		ID:      org2ID,
		Name:    name2,
		Enabled: false,
	}).Return(nil)

	// Expect deletion of org3
	mockQueries.On("DeleteOrganizationsNotInList", ctx, []uuid.UUID{org3ID}).Return(nil)

	err := syncOrganizations(ctx, mockQueries)
	assert.NoError(t, err)
	mockQueries.AssertExpectations(t)
}

func TestSyncBucketData(t *testing.T) {
	ctx := context.Background()
	mockQueries := new(MockQueries)

	org1ID := uuid.New()
	bucket1ID := uuid.New()

	orgBytes := pgtype.UUID{Valid: true}
	copy(orgBytes.Bytes[:], org1ID[:])

	collectorName := "collector1"

	// Mock c_ bucket data
	cBucketData := []configdb.GetAllCBucketDataRow{
		{
			BucketName:     "bucket1",
			CloudProvider:  "aws",
			Region:         "us-east-1",
			Role:           nil,
			OrganizationID: orgBytes,
			InstanceNum:    pgtype.Int2{Int16: 1, Valid: true},
			CollectorName:  &collectorName,
		},
	}

	// Mock our bucket configurations
	ourBuckets := []configdb.GetAllBucketConfigurationsRow{}

	// Mock our organization bucket mappings (one that should be deleted)
	ourMappings := []configdb.GetAllOrganizationBucketMappingsRow{
		{
			OrganizationID: org1ID,
			BucketName:     "bucket2",
			InstanceNum:    2,
			CollectorName:  "collector2",
		},
	}

	mockQueries.On("GetAllCBucketData", ctx).Return(cBucketData, nil)
	mockQueries.On("GetAllBucketConfigurations", ctx).Return(ourBuckets, nil)
	mockQueries.On("GetAllOrganizationBucketMappings", ctx).Return(ourMappings, nil)

	// Expect bucket configuration upsert
	mockQueries.On("UpsertBucketConfiguration", ctx, mock.MatchedBy(func(params configdb.UpsertBucketConfigurationParams) bool {
		return params.BucketName == "bucket1" &&
			params.CloudProvider == "aws" &&
			params.Region == "us-east-1"
	})).Return(configdb.BucketConfiguration{ID: bucket1ID}, nil)

	// Expect organization bucket upsert
	mockQueries.On("UpsertOrganizationBucket", ctx, configdb.UpsertOrganizationBucketParams{
		OrganizationID: org1ID,
		BucketID:       bucket1ID,
		InstanceNum:    1,
		CollectorName:  collectorName,
	}).Return(nil)

	// Expect deletion of old mapping
	mockQueries.On("DeleteOrganizationBucketMappings", ctx, configdb.DeleteOrganizationBucketMappingsParams{
		OrgIds:         []uuid.UUID{org1ID},
		InstanceNums:   []int16{2},
		CollectorNames: []string{"collector2"},
	}).Return(nil)

	err := syncBucketData(ctx, mockQueries)
	assert.NoError(t, err)
	mockQueries.AssertExpectations(t)
}

func TestSyncOrganizationAPIKeys(t *testing.T) {
	ctx := context.Background()
	mockQueries := new(MockQueries)

	org1ID := uuid.New()
	apiKeyID := uuid.New()

	orgBytes := pgtype.UUID{Valid: true}
	copy(orgBytes.Bytes[:], org1ID[:])

	apiKey1 := "test-api-key-1"
	apiKey2 := "test-api-key-2"
	name1 := "Key1"

	// Mock c_ API keys
	cAPIKeys := []configdb.GetAllCOrganizationAPIKeysRow{
		{
			ID:             uuid.New(),
			OrganizationID: orgBytes,
			Name:           &name1,
			ApiKey:         &apiKey1,
			Enabled:        pgtype.Bool{Bool: true, Valid: true},
		},
	}

	// Mock our API key mappings (one that should be deleted)
	ourMappings := []configdb.GetAllOrganizationAPIKeyMappingsRow{
		{
			OrganizationID: org1ID,
			KeyHash:        hashAPIKey(apiKey2),
			Name:           "OldKey",
		},
	}

	mockQueries.On("GetAllCOrganizationAPIKeys", ctx).Return(cAPIKeys, nil)
	mockQueries.On("GetAllOrganizationAPIKeyMappings", ctx).Return(ourMappings, nil)

	// Expect API key upsert
	mockQueries.On("UpsertOrganizationAPIKey", ctx, configdb.UpsertOrganizationAPIKeyParams{
		KeyHash:     hashAPIKey(apiKey1),
		Name:        name1,
		Description: nil,
	}).Return(configdb.OrganizationApiKey{ID: apiKeyID}, nil)

	// Expect API key mapping upsert
	mockQueries.On("UpsertOrganizationAPIKeyMapping", ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
		ApiKeyID:       apiKeyID,
		OrganizationID: org1ID,
	}).Return(nil)

	// Expect deletion of old mapping
	mockQueries.On("DeleteOrganizationAPIKeyMappingByHash", ctx, configdb.DeleteOrganizationAPIKeyMappingByHashParams{
		OrganizationID: org1ID,
		KeyHash:        hashAPIKey(apiKey2),
	}).Return(nil)

	err := syncOrganizationAPIKeys(ctx, mockQueries)
	assert.NoError(t, err)
	mockQueries.AssertExpectations(t)
}

func TestSyncOrganizations_NoChanges(t *testing.T) {
	ctx := context.Background()
	mockQueries := new(MockQueries)

	org1ID := uuid.New()
	name1 := "Org1"

	// Same organizations in both tables
	cOrgs := []configdb.GetAllCOrganizationsRow{
		{ID: org1ID, Name: &name1, Enabled: pgtype.Bool{Bool: true, Valid: true}},
	}

	ourOrgs := []configdb.GetAllOrganizationsRow{
		{ID: org1ID, Name: name1, Enabled: true},
	}

	mockQueries.On("GetAllCOrganizations", ctx).Return(cOrgs, nil)
	mockQueries.On("GetAllOrganizations", ctx).Return(ourOrgs, nil)

	// No updates or deletes expected

	err := syncOrganizations(ctx, mockQueries)
	assert.NoError(t, err)
	mockQueries.AssertExpectations(t)
}
