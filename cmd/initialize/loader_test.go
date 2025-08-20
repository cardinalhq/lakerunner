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

package initialize

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cardinalhq/lakerunner/configdb"
)

// MockFileReader is a mock implementation of FileReader
type MockFileReader struct {
	mock.Mock
}

func (m *MockFileReader) ReadFile(filename string) ([]byte, error) {
	args := m.Called(filename)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockFileReader) Getenv(key string) string {
	args := m.Called(key)
	return args.String(0)
}

// MockDatabaseQueries is a mock implementation of DatabaseQueries
type MockDatabaseQueries struct {
	mock.Mock
}

func (m *MockDatabaseQueries) SyncOrganizations(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabaseQueries) HasExistingStorageProfiles(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *MockDatabaseQueries) ClearBucketPrefixMappings(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabaseQueries) ClearOrganizationBuckets(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabaseQueries) ClearBucketConfigurations(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabaseQueries) UpsertBucketConfiguration(ctx context.Context, arg configdb.UpsertBucketConfigurationParams) (configdb.LrconfigBucketConfiguration, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(configdb.LrconfigBucketConfiguration), args.Error(1)
}

func (m *MockDatabaseQueries) UpsertOrganizationBucket(ctx context.Context, arg configdb.UpsertOrganizationBucketParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func (m *MockDatabaseQueries) ClearOrganizationAPIKeyMappings(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabaseQueries) ClearOrganizationAPIKeys(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabaseQueries) UpsertOrganizationAPIKey(ctx context.Context, arg configdb.UpsertOrganizationAPIKeyParams) (configdb.LrconfigOrganizationApiKey, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(configdb.LrconfigOrganizationApiKey), args.Error(1)
}

func (m *MockDatabaseQueries) GetOrganizationAPIKeyByHash(ctx context.Context, keyHash string) (configdb.GetOrganizationAPIKeyByHashRow, error) {
	args := m.Called(ctx, keyHash)
	return args.Get(0).(configdb.GetOrganizationAPIKeyByHashRow), args.Error(1)
}

func (m *MockDatabaseQueries) UpsertOrganizationAPIKeyMapping(ctx context.Context, arg configdb.UpsertOrganizationAPIKeyMappingParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func TestLoadFileContentsWithReader_RegularFile(t *testing.T) {
	mockReader := new(MockFileReader)
	testContent := "test file content"

	mockReader.On("ReadFile", "test.yaml").Return([]byte(testContent), nil)

	result, err := loadFileContentsWithReader("test.yaml", mockReader)

	assert.NoError(t, err)
	assert.Equal(t, []byte(testContent), result)
	mockReader.AssertExpectations(t)
}

func TestLoadFileContentsWithReader_EnvironmentVariable(t *testing.T) {
	mockReader := new(MockFileReader)
	testContent := "env content"

	mockReader.On("Getenv", "TEST_VAR").Return(testContent)

	result, err := loadFileContentsWithReader("env:TEST_VAR", mockReader)

	assert.NoError(t, err)
	assert.Equal(t, []byte(testContent), result)
	mockReader.AssertExpectations(t)
}

func TestLoadFileContentsWithReader_EnvironmentVariableEmpty(t *testing.T) {
	mockReader := new(MockFileReader)

	mockReader.On("Getenv", "EMPTY_VAR").Return("")

	result, err := loadFileContentsWithReader("env:EMPTY_VAR", mockReader)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "environment variable EMPTY_VAR is not set")
	mockReader.AssertExpectations(t)
}

func TestLoadFileContentsWithReader_FileReadError(t *testing.T) {
	mockReader := new(MockFileReader)

	mockReader.On("ReadFile", "nonexistent.yaml").Return([]byte(nil), fmt.Errorf("file not found"))

	result, err := loadFileContentsWithReader("nonexistent.yaml", mockReader)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to read file nonexistent.yaml")
	mockReader.AssertExpectations(t)
}

func TestImportStorageProfiles_Success(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	orgID := uuid.New()
	bucketID := uuid.New()

	yamlContent := fmt.Sprintf(`
- organization_id: %s
  cloud_provider: aws
  region: us-east-1
  bucket: test-bucket
  role: test-role
`, orgID.String())

	bucketConfig := configdb.LrconfigBucketConfiguration{
		ID:            bucketID,
		BucketName:    "test-bucket",
		CloudProvider: "aws",
		Region:        "us-east-1",
	}

	mockDB.On("UpsertBucketConfiguration", ctx, mock.MatchedBy(func(params configdb.UpsertBucketConfigurationParams) bool {
		return params.BucketName == "test-bucket" &&
			params.CloudProvider == "aws" &&
			params.Region == "us-east-1"
	})).Return(bucketConfig, nil)

	mockDB.On("UpsertOrganizationBucket", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationBucketParams) bool {
		return params.OrganizationID == orgID &&
			params.BucketID == bucketID
	})).Return(nil)

	err := importStorageProfiles(ctx, []byte(yamlContent), mockDB, logger, false)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestImportStorageProfiles_WithReplace(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	orgID := uuid.New()
	bucketID := uuid.New()

	yamlContent := fmt.Sprintf(`
- organization_id: %s
  cloud_provider: gcp
  region: us-central1
  bucket: test-bucket-gcp
`, orgID.String())

	// Expect clear operations in replace mode
	mockDB.On("ClearBucketPrefixMappings", ctx).Return(nil)
	mockDB.On("ClearOrganizationBuckets", ctx).Return(nil)
	mockDB.On("ClearBucketConfigurations", ctx).Return(nil)

	bucketConfig := configdb.LrconfigBucketConfiguration{
		ID:            bucketID,
		BucketName:    "test-bucket-gcp",
		CloudProvider: "gcp",
		Region:        "us-central1",
	}

	mockDB.On("UpsertBucketConfiguration", ctx, mock.MatchedBy(func(params configdb.UpsertBucketConfigurationParams) bool {
		return params.BucketName == "test-bucket-gcp" &&
			params.CloudProvider == "gcp" &&
			params.Region == "us-central1"
	})).Return(bucketConfig, nil)

	mockDB.On("UpsertOrganizationBucket", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationBucketParams) bool {
		return params.OrganizationID == orgID &&
			params.BucketID == bucketID
	})).Return(nil)

	err := importStorageProfiles(ctx, []byte(yamlContent), mockDB, logger, true)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestImportStorageProfiles_InvalidYAML(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	invalidYaml := `invalid: yaml: content: [unclosed`

	err := importStorageProfiles(ctx, []byte(invalidYaml), mockDB, logger, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse YAML configuration")
	mockDB.AssertExpectations(t)
}

func TestImportAPIKeys_Success(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	orgID := uuid.New()
	apiKeyID := uuid.New()
	testAPIKey := "test-api-key-12345"

	apiKeysConfig := APIKeysConfig{
		{
			OrganizationID: orgID,
			Keys:           []string{testAPIKey},
		},
	}

	expectedKeyHash := hashAPIKey(testAPIKey)

	mockDB.On("SyncOrganizations", ctx).Return(nil)

	// Expect clear operations in replace mode
	mockDB.On("ClearOrganizationAPIKeyMappings", ctx).Return(nil)
	mockDB.On("ClearOrganizationAPIKeys", ctx).Return(nil)

	apiKeyRow := configdb.GetOrganizationAPIKeyByHashRow{
		ID:      apiKeyID,
		KeyHash: expectedKeyHash,
		Name:    "imported-key-test-api",
	}

	upsertAPIKeyRow := configdb.LrconfigOrganizationApiKey{
		ID:      apiKeyID,
		KeyHash: expectedKeyHash,
		Name:    "imported-key-test-api",
	}

	mockDB.On("UpsertOrganizationAPIKey", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationAPIKeyParams) bool {
		return params.KeyHash == expectedKeyHash &&
			strings.HasPrefix(params.Name, "imported-key-")
	})).Return(upsertAPIKeyRow, nil)

	mockDB.On("GetOrganizationAPIKeyByHash", ctx, expectedKeyHash).Return(apiKeyRow, nil)

	mockDB.On("UpsertOrganizationAPIKeyMapping", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationAPIKeyMappingParams) bool {
		return params.ApiKeyID == apiKeyID &&
			params.OrganizationID == orgID
	})).Return(nil)

	err := importAPIKeys(ctx, apiKeysConfig, mockDB, logger, true)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestImportAPIKeys_DatabaseError(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	orgID := uuid.New()
	testAPIKey := "test-api-key-error"

	apiKeysConfig := APIKeysConfig{
		{
			OrganizationID: orgID,
			Keys:           []string{testAPIKey},
		},
	}

	mockDB.On("SyncOrganizations", ctx).Return(fmt.Errorf("sync failed"))

	err := importAPIKeys(ctx, apiKeysConfig, mockDB, logger, false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to sync organizations before API key import")
	mockDB.AssertExpectations(t)
}

func TestInitializeConfigWithDependencies_Success(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	mockReader := new(MockFileReader)
	ctx := context.Background()
	logger := slog.Default()

	orgID := uuid.New()
	bucketID := uuid.New()

	storageProfileYaml := fmt.Sprintf(`
- organization_id: %s
  cloud_provider: aws
  region: us-west-2
  bucket: integration-test-bucket
`, orgID.String())

	apiKeysYaml := fmt.Sprintf(`
- organization_id: %s
  keys:
    - integration-test-key-123
`, orgID.String())

	// Mock file reader calls
	mockReader.On("ReadFile", "storage.yaml").Return([]byte(storageProfileYaml), nil)
	mockReader.On("ReadFile", "apikeys.yaml").Return([]byte(apiKeysYaml), nil)

	// Mock database calls
	mockDB.On("SyncOrganizations", ctx).Return(nil)

	bucketConfig := configdb.LrconfigBucketConfiguration{
		ID:            bucketID,
		BucketName:    "integration-test-bucket",
		CloudProvider: "aws",
		Region:        "us-west-2",
	}

	mockDB.On("UpsertBucketConfiguration", ctx, mock.AnythingOfType("configdb.UpsertBucketConfigurationParams")).Return(bucketConfig, nil)
	mockDB.On("UpsertOrganizationBucket", ctx, mock.AnythingOfType("configdb.UpsertOrganizationBucketParams")).Return(nil)

	// API key operations for non-replace mode
	apiKeyID := uuid.New()
	apiKeyRow := configdb.GetOrganizationAPIKeyByHashRow{
		ID:      apiKeyID,
		KeyHash: hashAPIKey("integration-test-key-123"),
		Name:    "imported-key-integrat",
	}

	upsertAPIKeyRow2 := configdb.LrconfigOrganizationApiKey{
		ID:      apiKeyID,
		KeyHash: hashAPIKey("integration-test-key-123"),
		Name:    "imported-key-integrat",
	}

	mockDB.On("UpsertOrganizationAPIKey", ctx, mock.AnythingOfType("configdb.UpsertOrganizationAPIKeyParams")).Return(upsertAPIKeyRow2, nil)
	mockDB.On("GetOrganizationAPIKeyByHash", ctx, mock.AnythingOfType("string")).Return(apiKeyRow, nil)
	mockDB.On("UpsertOrganizationAPIKeyMapping", ctx, mock.AnythingOfType("configdb.UpsertOrganizationAPIKeyMappingParams")).Return(nil)

	err := InitializeConfigWithDependencies(ctx, "storage.yaml", "apikeys.yaml", mockDB, mockReader, logger, false)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
	mockReader.AssertExpectations(t)
}

func TestHashAPIKey(t *testing.T) {
	testKey := "test-key-123"
	hash1 := hashAPIKey(testKey)
	hash2 := hashAPIKey(testKey)

	// Same input should produce same hash
	assert.Equal(t, hash1, hash2)

	// Different input should produce different hash
	differentHash := hashAPIKey("different-key")
	assert.NotEqual(t, hash1, differentHash)

	// Hash should be a hex string of expected length (SHA256 = 64 hex chars)
	assert.Len(t, hash1, 64)
}

func TestImportStorageProfiles_MultipleProfiles(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	orgID1 := uuid.New()
	orgID2 := uuid.New()
	bucketID := uuid.New()

	yamlContent := fmt.Sprintf(`
- organization_id: %s
  cloud_provider: aws
  region: us-east-1
  bucket: shared-bucket
  role: role1
- organization_id: %s
  cloud_provider: aws
  region: us-east-1
  bucket: shared-bucket
  role: role2
`, orgID1.String(), orgID2.String())

	bucketConfig := configdb.LrconfigBucketConfiguration{
		ID:            bucketID,
		BucketName:    "shared-bucket",
		CloudProvider: "aws",
		Region:        "us-east-1",
	}

	// Only one bucket config should be created for the shared bucket
	mockDB.On("UpsertBucketConfiguration", ctx, mock.MatchedBy(func(params configdb.UpsertBucketConfigurationParams) bool {
		return params.BucketName == "shared-bucket"
	})).Return(bucketConfig, nil)

	// Two organization bucket mappings should be created
	mockDB.On("UpsertOrganizationBucket", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationBucketParams) bool {
		return params.OrganizationID == orgID1 && params.BucketID == bucketID
	})).Return(nil)

	mockDB.On("UpsertOrganizationBucket", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationBucketParams) bool {
		return params.OrganizationID == orgID2 && params.BucketID == bucketID
	})).Return(nil)

	err := importStorageProfiles(ctx, []byte(yamlContent), mockDB, logger, false)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestImportAPIKeys_MultipleKeys(t *testing.T) {
	mockDB := new(MockDatabaseQueries)
	ctx := context.Background()
	logger := slog.Default()

	orgID := uuid.New()
	apiKeyID1 := uuid.New()
	apiKeyID2 := uuid.New()
	testAPIKey1 := "test-api-key-1"
	testAPIKey2 := "test-api-key-2"

	apiKeysConfig := APIKeysConfig{
		{
			OrganizationID: orgID,
			Keys:           []string{testAPIKey1, testAPIKey2},
		},
	}

	expectedKeyHash1 := hashAPIKey(testAPIKey1)
	expectedKeyHash2 := hashAPIKey(testAPIKey2)

	mockDB.On("SyncOrganizations", ctx).Return(nil)

	// First API key
	upsertAPIKeyRow1 := configdb.LrconfigOrganizationApiKey{
		ID:      apiKeyID1,
		KeyHash: expectedKeyHash1,
		Name:    "imported-key-test-api",
	}

	getAPIKeyRow1 := configdb.GetOrganizationAPIKeyByHashRow{
		ID:      apiKeyID1,
		KeyHash: expectedKeyHash1,
		Name:    "imported-key-test-api",
	}

	mockDB.On("UpsertOrganizationAPIKey", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationAPIKeyParams) bool {
		return params.KeyHash == expectedKeyHash1
	})).Return(upsertAPIKeyRow1, nil)

	mockDB.On("GetOrganizationAPIKeyByHash", ctx, expectedKeyHash1).Return(getAPIKeyRow1, nil)

	mockDB.On("UpsertOrganizationAPIKeyMapping", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationAPIKeyMappingParams) bool {
		return params.ApiKeyID == apiKeyID1 && params.OrganizationID == orgID
	})).Return(nil)

	// Second API key
	upsertAPIKeyRow2 := configdb.LrconfigOrganizationApiKey{
		ID:      apiKeyID2,
		KeyHash: expectedKeyHash2,
		Name:    "imported-key-test-api",
	}

	getAPIKeyRow2 := configdb.GetOrganizationAPIKeyByHashRow{
		ID:      apiKeyID2,
		KeyHash: expectedKeyHash2,
		Name:    "imported-key-test-api",
	}

	mockDB.On("UpsertOrganizationAPIKey", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationAPIKeyParams) bool {
		return params.KeyHash == expectedKeyHash2
	})).Return(upsertAPIKeyRow2, nil)

	mockDB.On("GetOrganizationAPIKeyByHash", ctx, expectedKeyHash2).Return(getAPIKeyRow2, nil)

	mockDB.On("UpsertOrganizationAPIKeyMapping", ctx, mock.MatchedBy(func(params configdb.UpsertOrganizationAPIKeyMappingParams) bool {
		return params.ApiKeyID == apiKeyID2 && params.OrganizationID == orgID
	})).Return(nil)

	err := importAPIKeys(ctx, apiKeysConfig, mockDB, logger, false)

	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}
