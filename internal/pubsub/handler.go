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

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func handleMessage(ctx context.Context, msg []byte, sp storageprofile.StorageProfileProvider, mdb InqueueInserter) error {
	if len(msg) == 0 {
		return fmt.Errorf("empty message received")
	}

	items, err := parseS3LikeEvents(msg)
	if err != nil {
		return fmt.Errorf("failed to parse S3-like events: %w", err)
	}

	for _, item := range items {
		var profile storageprofile.StorageProfile
		var err error
		if strings.HasPrefix(item.ObjectID, "otel-raw/") {
			profile, err = sp.GetByCollectorName(ctx, item.OrganizationID, item.CollectorName)
			if err != nil {
				slog.Error("Failed to get storage profile", slog.Any("error", err), slog.Any("organization_id", item.OrganizationID), slog.Int("instance_num", int(item.InstanceNum)))
				continue
			}
		} else if strings.HasPrefix(item.ObjectID, "db/") {
			// Skip database files
			slog.Info("Skipping database file", slog.String("objectID", item.ObjectID))
			continue
		} else {
			profiles, err := sp.GetStorageProfilesByBucketName(ctx, item.Bucket)
			if err != nil {
				slog.Error("Failed to get storage profile", slog.Any("error", err), slog.String("bucket", item.Bucket))
				continue
			}
			if len(profiles) != 1 {
				slog.Error("Expected exactly one storage profile for bucket", slog.String("bucket", item.Bucket), slog.Int("found", len(profiles)))
				continue
			}
			profile = profiles[0]
			item.OrganizationID = profile.OrganizationID
			item.CollectorName = profile.CollectorName
		}
		item.InstanceNum = profile.InstanceNum
		slog.Info("Processing item", slog.String("bucket", profile.Bucket), slog.String("object_id", item.ObjectID), slog.String("telemetry_type", item.TelemetryType))

		err = mdb.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
			OrganizationID: item.OrganizationID,
			CollectorName:  item.CollectorName,
			InstanceNum:    item.InstanceNum,
			Bucket:         profile.Bucket,
			ObjectID:       item.ObjectID,
			TelemetryType:  item.TelemetryType,
			Priority:       0,
		})
		if err != nil {
			return fmt.Errorf("failed to insert inqueue work: %w", err)
		}
	}

	return nil
}
