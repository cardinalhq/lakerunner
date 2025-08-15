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

package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-traces",
		Short: "Ingest traces from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

			servicename := "lakerunner-ingest-traces"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "traces"),
				attribute.String("action", "ingest"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewIngestLoopContext(doneCtx, "traces", servicename)
			if err != nil {
				return fmt.Errorf("failed to create ingest loop context: %w", err)
			}

			for {
				select {
				case <-doneCtx.Done():
					slog.Info("Ingest traces command done")
					return nil
				default:
				}

				err := IngestLoop(loop, traceIngestItem)
				if err != nil {
					slog.Error("Error in ingest loop", slog.Any("error", err))
				}
			}
		},
	}

	rootCmd.AddCommand(cmd)
}

func traceIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64) error {
	profile, err := sp.Get(ctx, inf.OrganizationID, inf.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}
	if profile.Role == "" {
		if !profile.Hosted {
			ll.Error("No role on non-hosted profile")
			return err
		}
	}
	if profile.Bucket != inf.Bucket {
		ll.Error("Bucket ID mismatch", slog.String("expected", profile.Bucket), slog.String("actual", inf.Bucket))
		return fmt.Errorf("bucket ID mismatch")
	}
	// Use the existing awsmanager for S3 operations
	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	// TODO: Use s3client to download trace files from S3
	_ = s3client // Avoid unused variable error for now

	// TODO: Implement trace ingestion logic
	// For now, just log that we received a trace item
	ll.Info("Received trace ingest item",
		slog.String("organization_id", inf.OrganizationID.String()),
		slog.String("bucket", inf.Bucket),
		slog.String("object_id", inf.ObjectID),
		slog.String("telemetry_type", inf.TelemetryType),
		slog.Int("ingest_dateint", int(ingest_dateint)),
	)

	// TODO: Add actual trace processing logic here
	// This would include:
	// 1. Downloading the trace file from S3
	// 2. Parsing the trace data
	// 3. Processing and storing the trace segments
	// 4. Updating the inqueue status

	// For now, just return success to avoid blocking the queue
	return nil
}
