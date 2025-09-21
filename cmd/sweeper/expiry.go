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
)

// ExpiryQuerier defines the database operations needed for expiry cleanup
// This interface enables easier unit testing by allowing mock implementations
type ExpiryQuerier interface {
	// GetActiveOrganizations returns all enabled organizations
	GetActiveOrganizations(ctx context.Context) ([]configdb.GetActiveOrganizationsRow, error)

	// GetOrganizationExpiry retrieves expiry configuration for an org/signal pair
	GetOrganizationExpiry(ctx context.Context, arg configdb.GetOrganizationExpiryParams) (configdb.OrganizationSignalExpiry, error)

	// UpsertOrganizationExpiry creates or updates expiry configuration
	UpsertOrganizationExpiry(ctx context.Context, arg configdb.UpsertOrganizationExpiryParams) error

	// UpdateExpiryCheckedAt updates the last checked timestamp
	UpdateExpiryCheckedAt(ctx context.Context, arg configdb.UpdateExpiryCheckedAtParams) error

	// CallFindOrgPartition finds the organization's partition for a given table
	CallFindOrgPartition(ctx context.Context, arg configdb.CallFindOrgPartitionParams) (string, error)

	// CallExpirePublishedByIngestCutoff expires data older than the cutoff date
	CallExpirePublishedByIngestCutoff(ctx context.Context, arg configdb.CallExpirePublishedByIngestCutoffParams) (int64, error)
}

// Compile-time check to ensure configdb.QuerierFull implements ExpiryQuerier
var _ ExpiryQuerier = (configdb.QuerierFull)(nil)

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
func runExpiryCleanup(ctx context.Context, cdb ExpiryQuerier, cfg *config.Config) error {
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
			// Check if we need to process this org/signal combination
			expiry, err := cdb.GetOrganizationExpiry(ctx, configdb.GetOrganizationExpiryParams{
				OrganizationID: orgID,
				SignalType:     signalType,
			})

			needsCheck := false
			var maxAgeDays int

			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// No entry exists, create one with default settings
					err = cdb.UpsertOrganizationExpiry(ctx, configdb.UpsertOrganizationExpiryParams{
						OrganizationID: orgID,
						SignalType:     signalType,
						MaxAgeDays:     int32(-1), // -1 means use default from config
					})
					if err != nil {
						slog.Error("Failed to create expiry entry",
							slog.String("org", orgName),
							slog.String("signal_type", signalType),
							slog.Any("error", err))
						continue
					}
					// Use default from config
					maxAgeDays = getDefaultMaxAgeDays(cfg, signalType)
					if maxAgeDays == -1 {
						slog.Warn("No default configured for signal type, skipping",
							slog.String("org", orgName),
							slog.String("signal_type", signalType))
						continue
					}
					needsCheck = true
				} else {
					slog.Error("Failed to fetch expiry config",
						slog.String("org", orgName),
						slog.String("signal_type", signalType),
						slog.Any("error", err))
					continue
				}
			} else {
				// Check if it was already checked today
				if expiry.LastCheckedAt.Before(time.Now().Truncate(24 * time.Hour)) {
					needsCheck = true
					if expiry.MaxAgeDays == 0 {
						// 0 means never expire
						needsCheck = false
						slog.Debug("Skipping expiry for org/signal (never expire)",
							slog.String("org", orgName),
							slog.String("signal_type", signalType))
					} else if expiry.MaxAgeDays > 0 {
						// Use the org-specific setting
						maxAgeDays = int(expiry.MaxAgeDays)
					} else {
						// -1 means use default from config
						maxAgeDays = getDefaultMaxAgeDays(cfg, signalType)
						if maxAgeDays == -1 {
							slog.Warn("No default configured for signal type, skipping",
								slog.String("org", orgName),
								slog.String("signal_type", signalType))
							continue
						} else if maxAgeDays == 0 {
							// Config says never expire
							needsCheck = false
							slog.Debug("Skipping expiry for org/signal (config says never expire)",
								slog.String("org", orgName),
								slog.String("signal_type", signalType))
						}
					}
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
			partitionResult, err := cdb.CallFindOrgPartition(ctx, configdb.CallFindOrgPartitionParams{
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

				// Update last_checked_at anyway to avoid repeated attempts
				_ = cdb.UpdateExpiryCheckedAt(ctx, configdb.UpdateExpiryCheckedAtParams{
					OrganizationID: orgID,
					SignalType:     signalType,
				})
				continue
			}

			// Call the expiry function
			rowsExpired, err := cdb.CallExpirePublishedByIngestCutoff(ctx, configdb.CallExpirePublishedByIngestCutoffParams{
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
			} else {
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
			}

			// Update last_checked_at
			err = cdb.UpdateExpiryCheckedAt(ctx, configdb.UpdateExpiryCheckedAtParams{
				OrganizationID: orgID,
				SignalType:     signalType,
			})
			if err != nil {
				slog.Error("Failed to update last_checked_at",
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
