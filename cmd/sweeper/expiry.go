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
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ConfigExpiryQuerier defines the configdb operations needed for expiry cleanup
type ConfigExpiryQuerier interface {
	// GetActiveOrganizations returns all enabled organizations
	GetActiveOrganizations(ctx context.Context) ([]configdb.GetActiveOrganizationsRow, error)

	// GetOrganizationExpiry retrieves expiry configuration for an org/signal pair
	GetOrganizationExpiry(ctx context.Context, arg configdb.GetOrganizationExpiryParams) (configdb.OrganizationSignalExpiry, error)

	// GetExpiryLastRun retrieves when expiry was last run for an org/signal pair
	GetExpiryLastRun(ctx context.Context, arg configdb.GetExpiryLastRunParams) (configdb.ExpiryRunTracking, error)

	// UpsertExpiryRunTracking creates or updates expiry run tracking
	UpsertExpiryRunTracking(ctx context.Context, arg configdb.UpsertExpiryRunTrackingParams) error
}

// LRDBExpiryQuerier defines the lrdb operations needed for expiry cleanup
type LRDBExpiryQuerier interface {
	// CallFindOrgPartition finds the organization's partition for a given table
	CallFindOrgPartition(ctx context.Context, arg lrdb.CallFindOrgPartitionParams) (string, error)

	// CallExpirePublishedByIngestCutoff expires data older than the cutoff date
	CallExpirePublishedByIngestCutoff(ctx context.Context, arg lrdb.CallExpirePublishedByIngestCutoffParams) (int64, error)
}

// Compile-time checks to ensure implementations match interfaces
var _ ConfigExpiryQuerier = (configdb.QuerierFull)(nil)
var _ LRDBExpiryQuerier = (lrdb.QuerierFull)(nil)

var (
	expiryCleanupCounter  metric.Int64Counter
	expiryCleanupDuration metric.Float64Histogram
	expiryRowsCounter     metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/cmd/sweeper")

	var err error
	expiryCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.expiry_cleanup_total",
		metric.WithDescription("Count of expiry cleanup runs"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create expiry_cleanup_total counter: %w", err))
	}

	expiryCleanupDuration, err = meter.Float64Histogram(
		"lakerunner.sweeper.expiry_cleanup_duration_seconds",
		metric.WithDescription("Duration of expiry cleanup runs in seconds"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create expiry_cleanup_duration_seconds histogram: %w", err))
	}

	expiryRowsCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.expiry_rows_cleaned_total",
		metric.WithDescription("Count of rows marked as expired"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create expiry_rows_cleaned_total counter: %w", err))
	}
}

const (
	expiryCleanupPeriod = 1 * time.Hour // Check every hour, but only process once per day per org
)

// toDateInt converts a time to YYYYMMDD format
func toDateInt(t time.Time) int {
	y, m, d := t.Date()
	return y*10000 + int(m)*100 + d
}

// getDefaultMaxAgeDays returns the default max age in days for a signal type from the configuration
// Returns -1 if not configured (which means use the system default)
func getDefaultMaxAgeDays(cfg *config.Config, signalType string) int {
	if cfg != nil && cfg.Expiry.DefaultMaxAgeDays != nil {
		if days, ok := cfg.Expiry.DefaultMaxAgeDays[signalType]; ok {
			return days // Return the value as-is, even if 0 (never expire)
		}
	}
	return -1 // Not configured, will be handled as "use default"
}

// getBatchSize returns the batch size for expiry operations from the configuration
func getBatchSize(cfg *config.Config) int {
	if cfg != nil && cfg.Expiry.BatchSize > 0 {
		return cfg.Expiry.BatchSize
	}
	// This should never happen if config has proper defaults
	return 20000 // Fallback just in case
}

// runExpiryCleanup processes expired data for all organizations and signal types
func runExpiryCleanup(ctx context.Context, cdb ConfigExpiryQuerier, ldb LRDBExpiryQuerier, cfg *config.Config) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		expiryCleanupDuration.Record(ctx, duration)
	}()

	slog.Info("Starting expiry cleanup cycle")

	// Get all active organizations
	orgs, err := cdb.GetActiveOrganizations(ctx)
	if err != nil {
		expiryCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("error", "fetch_orgs"),
		))
		return fmt.Errorf("failed to fetch active organizations: %w", err)
	}

	signalTypes := []string{"logs", "metrics", "traces"}
	batchSize := getBatchSize(cfg)
	totalRowsExpired := int64(0)

	for _, org := range orgs {
		orgID := org.ID
		orgName := org.Name

		for _, signalType := range signalTypes {
			// First check if we have a retention policy for this org/signal
			expiry, err := cdb.GetOrganizationExpiry(ctx, configdb.GetOrganizationExpiryParams{
				OrganizationID: orgID,
				SignalType:     signalType,
			})

			var maxAgeDays int

			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// No policy exists - use default from config
					maxAgeDays = getDefaultMaxAgeDays(cfg, signalType)
					if maxAgeDays == -1 {
						slog.Debug("No policy and no default configured for signal type, skipping",
							slog.String("org", orgName),
							slog.String("signal_type", signalType))
						continue
					}
				} else {
					slog.Error("Failed to fetch expiry config",
						slog.String("org", orgName),
						slog.String("signal_type", signalType),
						slog.Any("error", err))
					continue
				}
			} else {
				// We have a policy
				if expiry.MaxAgeDays == 0 {
					// 0 means never expire
					slog.Debug("Skipping expiry for org/signal (policy says never expire)",
						slog.String("org", orgName),
						slog.String("signal_type", signalType))
					continue
				} else if expiry.MaxAgeDays > 0 {
					// Use the org-specific setting
					maxAgeDays = int(expiry.MaxAgeDays)
				} else {
					// -1 means use default from config
					maxAgeDays = getDefaultMaxAgeDays(cfg, signalType)
					if maxAgeDays == -1 {
						slog.Debug("Policy exists but no default configured for signal type, skipping",
							slog.String("org", orgName),
							slog.String("signal_type", signalType))
						continue
					}
				}
			}

			// Check if maxAgeDays is 0 (never expire)
			if maxAgeDays == 0 {
				slog.Debug("Skipping expiry for org/signal (never expire)",
					slog.String("org", orgName),
					slog.String("signal_type", signalType))
				continue
			}

			// Now check if we need to run expiry (haven't checked today)
			needsCheck := false
			lastRun, err := cdb.GetExpiryLastRun(ctx, configdb.GetExpiryLastRunParams{
				OrganizationID: orgID,
				SignalType:     signalType,
			})

			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// Never run before, needs check
					needsCheck = true
				} else {
					slog.Error("Failed to fetch last run info",
						slog.String("org", orgName),
						slog.String("signal_type", signalType),
						slog.Any("error", err))
					// Continue anyway - better to check than skip
					needsCheck = true
				}
			} else {
				// Check if it was already checked today
				if lastRun.LastRunAt.Time.Before(time.Now().Truncate(24 * time.Hour)) {
					needsCheck = true
				}
			}

			if !needsCheck {
				continue
			}

			// Calculate cutoff date with extra 1 day buffer
			cutoffDate := time.Now().AddDate(0, 0, -(maxAgeDays + 1))
			cutoffDateInt := toDateInt(cutoffDate)

			slog.Info("Processing expiry",
				slog.String("org", orgName),
				slog.String("signal_type", signalType),
				slog.Int("max_age_days", maxAgeDays),
				slog.Int("cutoff_dateint", cutoffDateInt))

			// Determine the table name
			tableName := fmt.Sprintf("%s_seg", signalType[:len(signalType)-1]) // Remove 's' from the end

			// Find the org partition
			partitionResult, err := ldb.CallFindOrgPartition(ctx, lrdb.CallFindOrgPartitionParams{
				TableName:      tableName,
				OrganizationID: orgID,
			})
			if err != nil {
				// Log the error and continue - partition might not exist yet
				slog.Warn("Failed to find org partition",
					slog.String("org", orgName),
					slog.String("signal_type", signalType),
					slog.String("table", tableName),
					slog.Any("error", err))

				// Update run tracking anyway to avoid repeated attempts
				_ = cdb.UpsertExpiryRunTracking(ctx, configdb.UpsertExpiryRunTrackingParams{
					OrganizationID: orgID,
					SignalType:     signalType,
				})
				continue
			}

			// Call the expiry function
			rowsExpired, err := ldb.CallExpirePublishedByIngestCutoff(ctx, lrdb.CallExpirePublishedByIngestCutoffParams{
				PartitionName:  partitionResult,
				OrganizationID: orgID,
				CutoffDateint:  int32(cutoffDateInt),
				BatchSize:      int32(batchSize),
			})
			if err != nil {
				slog.Error("Failed to expire data",
					slog.String("org", orgName),
					slog.String("signal_type", signalType),
					slog.String("partition", partitionResult),
					slog.Int("cutoff_dateint", cutoffDateInt),
					slog.Any("error", err))

				expiryCleanupCounter.Add(ctx, 1, metric.WithAttributes(
					attribute.String("status", "error"),
					attribute.String("signal_type", signalType),
					attribute.String("org", orgName),
					attribute.String("error", "expire_data"),
				))
				// Don't update run tracking on failure - allow retry on next hourly pass
				continue
			}

			totalRowsExpired += rowsExpired

			slog.Info("Successfully expired data",
				slog.String("org", orgName),
				slog.String("signal_type", signalType),
				slog.Int64("rows_expired", rowsExpired))

			expiryRowsCounter.Add(ctx, rowsExpired, metric.WithAttributes(
				attribute.String("signal_type", signalType),
				attribute.String("org", orgName),
			))

			expiryCleanupCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("status", "success"),
				attribute.String("signal_type", signalType),
				attribute.String("org", orgName),
			))

			// Only update run tracking on successful expiry
			err = cdb.UpsertExpiryRunTracking(ctx, configdb.UpsertExpiryRunTrackingParams{
				OrganizationID: orgID,
				SignalType:     signalType,
			})
			if err != nil {
				slog.Error("Failed to update run tracking",
					slog.String("org", orgName),
					slog.String("signal_type", signalType),
					slog.Any("error", err))
			}
		}
	}

	slog.Info("Expiry cleanup cycle completed",
		slog.Int64("total_rows_expired", totalRowsExpired),
		slog.Duration("duration", time.Since(start)))

	return nil
}
