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

package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	itemsProcessed metric.Int64Counter
	itemsSkipped   metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/pubsub")

	var err error
	itemsProcessed, err = meter.Int64Counter(
		"pubsub_items_processed_total",
		metric.WithDescription("Total number of pubsub items processed successfully"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create itemsProcessed counter: %w", err))
	}

	itemsSkipped, err = meter.Int64Counter(
		"pubsub_items_skipped_total",
		metric.WithDescription("Total number of pubsub items skipped"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create itemsSkipped counter: %w", err))
	}
}

func handleMessage(ctx context.Context, msg []byte, sp storageprofile.StorageProfileProvider, mdb InqueueInserter) error {
	if len(msg) == 0 {
		return fmt.Errorf("empty message received")
	}

	items, err := parseS3LikeEvents(msg)
	if err != nil {
		return fmt.Errorf("failed to parse S3-like events: %w", err)
	}

	for _, item := range items {
		// Skip database files
		if strings.HasPrefix(item.ObjectID, "db/") {
			slog.Info("Skipping database file", slog.String("objectID", item.ObjectID))
			itemsSkipped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("reason", "database_file"),
				attribute.String("bucket", item.Bucket),
			))
			continue
		}

		// Use new organization resolution logic
		orgID, err := sp.ResolveOrganization(ctx, item.Bucket, item.ObjectID)
		if err != nil {
			slog.Error("Failed to resolve organization",
				slog.Any("error", err),
				slog.String("bucket", item.Bucket),
				slog.String("object_id", item.ObjectID))
			itemsSkipped.Add(ctx, 1, metric.WithAttributes(
				attribute.String("reason", "resolve_org_failed"),
				attribute.String("bucket", item.Bucket),
			))
			continue
		}

		// For otel-raw paths, org and collector were already extracted in parser
		// For other paths, we now have the resolved org
		if !strings.HasPrefix(item.ObjectID, "otel-raw/") {
			item.OrganizationID = orgID
			item.CollectorName = "" // Not used in v2
		}

		// Determine collector name and instance number
		collectorName := item.CollectorName
		instanceNum := int16(1) // Default instance number

		// Lookup instance number from storage profile
		if collectorName != "" {
			slog.Info("Looking up storage profile",
				slog.String("organization_id", item.OrganizationID.String()),
				slog.String("collector_name", collectorName))
			profile, err := sp.GetStorageProfileForOrganizationAndCollector(ctx, item.OrganizationID, collectorName)
			if err != nil {
				slog.Warn("Failed to lookup storage profile for collector, using default instance",
					slog.Any("error", err),
					slog.String("organization_id", item.OrganizationID.String()),
					slog.String("collector_name", collectorName))
			} else {
				slog.Info("Found storage profile",
					slog.String("organization_id", item.OrganizationID.String()),
					slog.String("collector_name", collectorName),
					slog.Int("instance_num", int(profile.InstanceNum)))
				instanceNum = profile.InstanceNum
			}
		}

		slog.Info("Processing item",
			slog.String("bucket", item.Bucket),
			slog.String("object_id", item.ObjectID),
			slog.String("telemetry_type", item.Signal),
			slog.String("organization_id", item.OrganizationID.String()),
			slog.String("collector_name", collectorName),
			slog.Int("instance_num", int(instanceNum)))

		err = mdb.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
			OrganizationID: item.OrganizationID,
			CollectorName:  collectorName,
			InstanceNum:    instanceNum,
			Bucket:         item.Bucket,
			ObjectID:       item.ObjectID,
			Signal:         item.Signal,
			Priority:       0,
			FileSize:       item.FileSize,
		})
		if err != nil {
			return fmt.Errorf("failed to insert inqueue work: %w", err)
		}

		itemsProcessed.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bucket", item.Bucket),
			attribute.String("telemetry_type", item.Signal),
		))
	}

	return nil
}
