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

package sweeper

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
)

// MockExpiryQuerier is a mock implementation of ExpiryQuerier for testing
type MockExpiryQuerier struct {
	mock.Mock
}

func (m *MockExpiryQuerier) GetActiveOrganizations(ctx context.Context) ([]configdb.GetActiveOrganizationsRow, error) {
	args := m.Called(ctx)
	return args.Get(0).([]configdb.GetActiveOrganizationsRow), args.Error(1)
}

func (m *MockExpiryQuerier) GetOrganizationExpiry(ctx context.Context, arg configdb.GetOrganizationExpiryParams) (configdb.OrganizationSignalExpiry, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(configdb.OrganizationSignalExpiry), args.Error(1)
}

func (m *MockExpiryQuerier) GetExpiryLastRun(ctx context.Context, arg configdb.GetExpiryLastRunParams) (configdb.ExpiryRunTracking, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(configdb.ExpiryRunTracking), args.Error(1)
}

func (m *MockExpiryQuerier) UpsertExpiryRunTracking(ctx context.Context, arg configdb.UpsertExpiryRunTrackingParams) error {
	args := m.Called(ctx, arg)
	return args.Error(0)
}

func (m *MockExpiryQuerier) CallFindOrgPartition(ctx context.Context, arg configdb.CallFindOrgPartitionParams) (string, error) {
	args := m.Called(ctx, arg)
	return args.String(0), args.Error(1)
}

func (m *MockExpiryQuerier) CallExpirePublishedByIngestCutoff(ctx context.Context, arg configdb.CallExpirePublishedByIngestCutoffParams) (int64, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(int64), args.Error(1)
}

func TestRunExpiryCleanup_NoOrganizations(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs":    30,
				"metrics": 90,
				"traces":  7,
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations - no organizations
	mockDB.On("GetActiveOrganizations", ctx).Return([]configdb.GetActiveOrganizationsRow{}, nil)

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_ErrorFetchingOrganizations(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	cfg := &config.Config{}

	// Setup expectations - error fetching orgs
	expectedErr := errors.New("database connection failed")
	mockDB.On("GetActiveOrganizations", ctx).Return([]configdb.GetActiveOrganizationsRow{}, expectedErr)

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch active organizations")
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_NeverExpireConfiguration(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs":    0, // Never expire
				"metrics": 0, // Never expire
				"traces":  0, // Never expire
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// For each signal type, when configured to never expire (0),
	// entries with MaxAgeDays=0 mean never expire
	yesterday := time.Now().AddDate(0, 0, -1)
	for _, signalType := range []string{"logs", "metrics", "traces"} {
		expiry := configdb.OrganizationSignalExpiry{
			OrganizationID: orgID,
			SignalType:     signalType,
			MaxAgeDays:     0, // Never expire
			CreatedAt:      yesterday,
			UpdatedAt:      yesterday,
		}
		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     signalType,
		}).Return(expiry, nil)

		// No expiry operations should happen since MaxAgeDays is 0
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_WithExpiry(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs":    30,
				"metrics": 90,
				"traces":  7,
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// Setup expiry configurations that need processing
	yesterday := time.Now().AddDate(0, 0, -1)

	for _, tc := range []struct {
		signalType   string
		maxAgeDays   int32
		shouldExpire bool
	}{
		{"logs", 30, true},    // Has specific retention, should process
		{"metrics", 0, false}, // Never expire
		{"traces", -1, true},  // Use default from config
	} {
		expiry := configdb.OrganizationSignalExpiry{
			OrganizationID: orgID,
			SignalType:     tc.signalType,
			MaxAgeDays:     tc.maxAgeDays,
			CreatedAt:      yesterday,
			UpdatedAt:      yesterday,
		}

		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     tc.signalType,
		}).Return(expiry, nil)

		if tc.shouldExpire {
			// Mock the last run check - was run yesterday so needs to run today
			lastRun := configdb.ExpiryRunTracking{
				OrganizationID: orgID,
				SignalType:     tc.signalType,
				LastRunAt:      pgtype.Timestamp{Time: yesterday, Valid: true},
				CreatedAt:      pgtype.Timestamp{Time: yesterday, Valid: true},
				UpdatedAt:      pgtype.Timestamp{Time: yesterday, Valid: true},
			}
			mockDB.On("GetExpiryLastRun", ctx, configdb.GetExpiryLastRunParams{
				OrganizationID: orgID,
				SignalType:     tc.signalType,
			}).Return(lastRun, nil)

			tableName := tc.signalType[:len(tc.signalType)-1] + "_seg"
			partitionName := tableName + "_org_" + orgID.String()[:8]

			// Expect partition lookup
			mockDB.On("CallFindOrgPartition", ctx, mock.MatchedBy(func(arg configdb.CallFindOrgPartitionParams) bool {
				return arg.TableName == tableName && arg.OrganizationID == orgID
			})).Return(partitionName, nil)

			// Expect expiry call
			mockDB.On("CallExpirePublishedByIngestCutoff", ctx, mock.MatchedBy(func(arg configdb.CallExpirePublishedByIngestCutoffParams) bool {
				return arg.PartitionName == partitionName &&
					arg.OrganizationID == orgID &&
					arg.BatchSize == int32(cfg.Expiry.BatchSize)
			})).Return(int64(100), nil)

			// Expect update of run tracking
			mockDB.On("UpsertExpiryRunTracking", ctx, configdb.UpsertExpiryRunTrackingParams{
				OrganizationID: orgID,
				SignalType:     tc.signalType,
			}).Return(nil)
		}
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_PartitionNotFound(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs": 30,
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// Setup expiry configuration
	yesterday := time.Now().AddDate(0, 0, -1)
	expiry := configdb.OrganizationSignalExpiry{
		OrganizationID: orgID,
		SignalType:     "logs",
		MaxAgeDays:     30,
		CreatedAt:      yesterday,
		UpdatedAt:      yesterday,
	}

	mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(expiry, nil)

	// Mock the last run check - needs to run
	mockDB.On("GetExpiryLastRun", ctx, configdb.GetExpiryLastRunParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(configdb.ExpiryRunTracking{}, sql.ErrNoRows)

	// Partition lookup fails (org has no data yet)
	mockDB.On("CallFindOrgPartition", ctx, mock.MatchedBy(func(arg configdb.CallFindOrgPartitionParams) bool {
		return arg.TableName == "log_seg" && arg.OrganizationID == orgID
	})).Return("", errors.New("No rows for organization"))

	// Should still update run tracking
	mockDB.On("UpsertExpiryRunTracking", ctx, configdb.UpsertExpiryRunTrackingParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(nil)

	// For the other signal types that don't have policies
	for _, signalType := range []string{"metrics", "traces"} {
		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     signalType,
		}).Return(configdb.OrganizationSignalExpiry{}, sql.ErrNoRows)
		// No default configured, so these are skipped
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err) // Should not fail when partition doesn't exist
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_AlreadyCheckedToday(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs": 30,
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// Setup expiry configuration
	today := time.Now()
	expiry := configdb.OrganizationSignalExpiry{
		OrganizationID: orgID,
		SignalType:     "logs",
		MaxAgeDays:     30,
		CreatedAt:      today,
		UpdatedAt:      today,
	}

	mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(expiry, nil)

	// Mock the last run check - already run today
	lastRun := configdb.ExpiryRunTracking{
		OrganizationID: orgID,
		SignalType:     "logs",
		LastRunAt:      pgtype.Timestamp{Time: today, Valid: true}, // Already run today
		CreatedAt:      pgtype.Timestamp{Time: today, Valid: true},
		UpdatedAt:      pgtype.Timestamp{Time: today, Valid: true},
	}
	mockDB.On("GetExpiryLastRun", ctx, configdb.GetExpiryLastRunParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(lastRun, nil)

	// Should NOT call partition lookup or expiry since already checked today

	// For the other signal types
	for _, signalType := range []string{"metrics", "traces"} {
		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     signalType,
		}).Return(configdb.OrganizationSignalExpiry{}, sql.ErrNoRows)
		// No default configured, so these are skipped
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_NoDefaultConfigured(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{}, // No defaults configured
			BatchSize:         1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// For each signal type, expect GetOrganizationExpiry to return ErrNoRows
	for _, signalType := range []string{"logs", "metrics", "traces"} {
		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     signalType,
		}).Return(configdb.OrganizationSignalExpiry{}, sql.ErrNoRows)
		// No expiry will happen since no default is configured
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_ZeroDefaultNoExistingEntry(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs":    0, // Never expire
				"metrics": 0, // Never expire
				"traces":  0, // Never expire
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// For each signal type, when no policy exists and default is 0 (never expire)
	for _, signalType := range []string{"logs", "metrics", "traces"} {
		// No policy exists - returns ErrNoRows
		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     signalType,
		}).Return(configdb.OrganizationSignalExpiry{}, sql.ErrNoRows)

		// No expiry operations should happen since default is 0 (never expire)
		// No GetExpiryLastRun
		// No CallFindOrgPartition
		// No CallExpirePublishedByIngestCutoff
		// No UpsertExpiryRunTracking
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)
	mockDB.AssertExpectations(t)
}

func TestRunExpiryCleanup_ExpiryFailureNoRunTracking(t *testing.T) {
	ctx := context.Background()
	mockDB := new(MockExpiryQuerier)
	orgID := uuid.New()
	cfg := &config.Config{
		Expiry: config.ExpiryConfig{
			DefaultMaxAgeDays: map[string]int{
				"logs": 30,
			},
			BatchSize: 1000,
		},
	}

	// Setup expectations
	orgs := []configdb.GetActiveOrganizationsRow{
		{ID: orgID, Name: "TestOrg", Enabled: true},
	}
	mockDB.On("GetActiveOrganizations", ctx).Return(orgs, nil)

	// Setup expiry configuration
	yesterday := time.Now().AddDate(0, 0, -1)
	expiry := configdb.OrganizationSignalExpiry{
		OrganizationID: orgID,
		SignalType:     "logs",
		MaxAgeDays:     30,
		CreatedAt:      yesterday,
		UpdatedAt:      yesterday,
	}

	mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(expiry, nil)

	// Mock the last run check - needs to run
	mockDB.On("GetExpiryLastRun", ctx, configdb.GetExpiryLastRunParams{
		OrganizationID: orgID,
		SignalType:     "logs",
	}).Return(configdb.ExpiryRunTracking{}, sql.ErrNoRows)

	// Partition lookup succeeds
	mockDB.On("CallFindOrgPartition", ctx, mock.MatchedBy(func(arg configdb.CallFindOrgPartitionParams) bool {
		return arg.TableName == "log_seg" && arg.OrganizationID == orgID
	})).Return("log_seg_org_partition", nil)

	// Expiry call FAILS - simulate database error
	mockDB.On("CallExpirePublishedByIngestCutoff", ctx, mock.MatchedBy(func(arg configdb.CallExpirePublishedByIngestCutoffParams) bool {
		return arg.PartitionName == "log_seg_org_partition" && arg.OrganizationID == orgID
	})).Return(int64(0), errors.New("database connection lost"))

	// IMPORTANT: UpsertExpiryRunTracking should NOT be called when expiry fails

	// For the other signal types that don't have policies
	for _, signalType := range []string{"metrics", "traces"} {
		mockDB.On("GetOrganizationExpiry", ctx, configdb.GetOrganizationExpiryParams{
			OrganizationID: orgID,
			SignalType:     signalType,
		}).Return(configdb.OrganizationSignalExpiry{}, sql.ErrNoRows)
	}

	// Run the function
	err := runExpiryCleanup(ctx, mockDB, cfg)

	// Assertions
	assert.NoError(t, err)       // Function should not fail overall
	mockDB.AssertExpectations(t) // This will fail if UpsertExpiryRunTracking was called
}
