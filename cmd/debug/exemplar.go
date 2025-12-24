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

package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func GetExemplarCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exemplar",
		Short: "Exemplar debugging utilities",
		Long:  `Various utilities for debugging and testing exemplar generation.`,
	}

	cmd.AddCommand(getExemplarFromParquetCmd())

	return cmd
}

func getExemplarFromParquetCmd() *cobra.Command {
	var bucket string
	var fileCount int
	var signalType string
	var skipSchemaCheck bool
	var maxExemplars int

	cmd := &cobra.Command{
		Use:   "from-parquet",
		Short: "Generate exemplars from recent parquet files",
		Long: `Downloads recent parquet files from a bucket using pubsub_message_history,
processes them through the exemplar processor, and prints exemplars to stdout as JSON lines.
Does not create segments, only generates and outputs exemplars.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExemplarFromParquet(cmd.Context(), bucket, fileCount, signalType, skipSchemaCheck, maxExemplars)
		},
	}

	cmd.Flags().StringVar(&bucket, "bucket", "", "S3 bucket name (required)")
	cmd.Flags().IntVar(&fileCount, "file-count", 100, "Number of recent files to process")
	cmd.Flags().StringVar(&signalType, "signal-type", "", "Filter by signal type: logs, metrics, or traces")
	cmd.Flags().BoolVar(&skipSchemaCheck, "skip-schema-check", false, "Skip database schema version check")
	cmd.Flags().IntVar(&maxExemplars, "max-exemplars", 0, "Maximum exemplars to output per signal type (0 = unlimited)")
	_ = cmd.MarkFlagRequired("bucket")

	return cmd
}

func runExemplarFromParquet(ctx context.Context, bucket string, fileCount int, signalTypeFilter string, skipSchemaCheck bool, maxExemplars int) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	if fileCount <= 0 {
		return fmt.Errorf("file-count must be greater than 0")
	}

	// Validate signal type filter if provided
	var filterSignalType filereader.SignalType
	var hasFilter bool
	if signalTypeFilter != "" {
		switch strings.ToLower(signalTypeFilter) {
		case "logs":
			filterSignalType = filereader.SignalTypeLogs
			hasFilter = true
		case "traces":
			filterSignalType = filereader.SignalTypeTraces
			hasFilter = true
		default:
			return fmt.Errorf("invalid signal-type: %s (must be logs, or traces)", signalTypeFilter)
		}
	}

	// Skip schema version checks if requested
	if skipSchemaCheck {
		_ = os.Setenv("LRDB_MIGRATION_CHECK_ENABLED", "false")
		_ = os.Setenv("CONFIGDB_MIGRATION_CHECK_ENABLED", "false")
	}

	// Connect to databases
	lrStore, err := lrdb.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to open lrdb: %w", err)
	}
	defer lrStore.Close()

	configPool, err := configdb.ConnectToConfigDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to open configdb: %w", err)
	}
	defer configPool.Close()

	configStore := configdb.NewStore(configPool)
	profileProvider := storageprofile.NewStorageProfileProvider(configStore)

	cloudManagers, err := cloudstorage.NewCloudManagers(ctx)
	if err != nil {
		return fmt.Errorf("failed to create cloud managers: %w", err)
	}

	// Get recent files from pubsub_message_history
	files, err := lrStore.PubSubMessageHistoryGetRecentForBucket(ctx, lrdb.PubSubMessageHistoryGetRecentForBucketParams{
		Bucket:     bucket,
		LimitCount: int32(fileCount),
	})
	if err != nil {
		return fmt.Errorf("failed to get recent files: %w", err)
	}

	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "No files found in bucket %s\n", bucket)
		return nil
	}

	fmt.Fprintf(os.Stderr, "Found %d files to process\n", len(files))

	// Create exemplar processor
	exemplarProcessor := createExemplarProcessor(maxExemplars)

	// Create fingerprint tenant manager
	fingerprintTenantManager := fingerprint.NewTenantManager(0.5)

	// Create temp directory for downloads
	tempDir, err := os.MkdirTemp("", "exemplar-debug-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Process each file
	processedCount := 0
	for i, file := range files {
		// Skip database files
		if strings.HasPrefix(file.ObjectID, "db/") {
			continue
		}

		var orgID uuid.UUID
		var signal string

		// For otel-raw paths, org and signal are extracted from the path directly
		if strings.HasPrefix(file.ObjectID, "otel-raw/") {
			var err error
			orgID, signal, err = parseOtelRawPath(file.ObjectID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: skipping file %s/%s: %v\n",
					file.Bucket, file.ObjectID, err)
				continue
			}
		} else {
			// Use storage profile provider to resolve organization
			resolution, err := profileProvider.ResolveOrganization(ctx, file.Bucket, file.ObjectID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: skipping file %s/%s: failed to resolve organization: %v\n",
					file.Bucket, file.ObjectID, err)
				continue
			}
			orgID = resolution.OrganizationID
			signal = resolution.Signal
		}

		// Convert signal string to SignalType
		var fileSignalType filereader.SignalType
		switch strings.ToLower(signal) {
		case "logs":
			fileSignalType = filereader.SignalTypeLogs
		case "traces":
			fileSignalType = filereader.SignalTypeTraces
		default:
			fmt.Fprintf(os.Stderr, "Warning: skipping file %s/%s: unknown signal type %s\n",
				file.Bucket, file.ObjectID, signal)
			continue
		}

		// Skip if not matching filter
		if hasFilter && fileSignalType != filterSignalType {
			continue
		}

		processedCount++
		fmt.Fprintf(os.Stderr, "[%d/%d] Processing %s/%s (%s)\n", i+1, len(files),
			file.Bucket, file.ObjectID, fileSignalType.String())

		if err := processFile(ctx, cloudManagers, profileProvider, exemplarProcessor,
			fingerprintTenantManager, orgID, file.Bucket, file.ObjectID, tempDir, fileSignalType); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to process file %s/%s: %v\n",
				file.Bucket, file.ObjectID, err)
			continue
		}
	}

	fmt.Fprintf(os.Stderr, "Processed %d files\n", processedCount)

	// Flush all pending exemplars
	fmt.Fprintf(os.Stderr, "Flushing pending exemplars...\n")
	if err := exemplarProcessor.Close(); err != nil {
		return fmt.Errorf("failed to flush exemplars: %w", err)
	}

	return nil
}

func createExemplarProcessor(maxExemplars int) *exemplars.Processor {
	config := exemplars.DefaultConfig()
	// Set very long report interval (1000 hours) to ensure the ticker never fires during this short-lived debug command.
	// (Can't be 0 or time.NewTicker will panic; 1000 hours is arbitrarily chosen as a value much larger than any expected runtime.)
	config.Logs.ReportInterval = 1000 * time.Hour
	config.Metrics.ReportInterval = 1000 * time.Hour
	config.Traces.ReportInterval = 1000 * time.Hour

	// Set max exemplars (0 = unlimited)
	config.Logs.MaxPublishPerSweep = maxExemplars
	config.Metrics.MaxPublishPerSweep = maxExemplars
	config.Traces.MaxPublishPerSweep = maxExemplars

	processor := exemplars.NewProcessor(config)

	// Set callbacks to print exemplars to stdout
	processor.SetLogsCallback(func(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row) error {
		return printExemplars("logs", rows)
	})

	processor.SetMetricsCallback(func(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row) error {
		return printExemplars("metrics", rows)
	})

	processor.SetTracesCallback(func(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row) error {
		return printExemplars("traces", rows)
	})

	return processor
}

func printExemplars(telemetryType string, rows []pipeline.Row) error {
	for _, row := range rows {
		// Convert row to string-keyed map for JSON marshalling
		stringRow := pipeline.ToStringMap(row)

		// Add telemetry type to the output
		output := map[string]interface{}{
			"telemetry_type": telemetryType,
			"exemplar":       stringRow,
		}
		jsonBytes, err := json.Marshal(output)
		if err != nil {
			return fmt.Errorf("failed to marshal exemplar: %w", err)
		}
		fmt.Println(string(jsonBytes))
	}
	return nil
}

func processFile(ctx context.Context, cloudManagers cloudstorage.ClientProvider,
	profileProvider storageprofile.StorageProfileProvider,
	exemplarProcessor *exemplars.Processor,
	fingerprintTenantManager *fingerprint.TenantManager,
	orgID uuid.UUID, bucket, objectID, tempDir string, signalType filereader.SignalType) error {

	// Get storage profiles for this bucket - we just need credentials to download
	profiles, err := profileProvider.GetStorageProfilesByBucketName(ctx, bucket)
	if err != nil {
		return fmt.Errorf("failed to get storage profiles: %w", err)
	}
	if len(profiles) == 0 {
		return fmt.Errorf("no storage profiles found for bucket %s", bucket)
	}
	// Use the first profile - they all have the same bucket credentials
	profile := profiles[0]

	storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Download file
	tmpFile, _, notFound, err := storageClient.DownloadObject(ctx, tempDir, bucket, objectID)
	if err != nil {
		if notFound {
			return fmt.Errorf("object not found: %s/%s", bucket, objectID)
		}
		return fmt.Errorf("failed to download object: %w", err)
	}
	defer func() { _ = os.Remove(tmpFile) }()

	// Create reader stack matching production ingestion
	reader, err := createReaderStack(tmpFile, orgID.String(), bucket, objectID, signalType, fingerprintTenantManager)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Read and process rows
	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read batch: %w", err)
		}

		if batch == nil || batch.Len() == 0 {
			break
		}

		// Process rows through exemplar processor
		for i := range batch.Len() {
			row := batch.Get(i)

			// Add fingerprint for logs (matching production)
			if signalType == filereader.SignalTypeLogs {
				if message, ok := row[wkk.RowKeyCMessage].(string); ok && message != "" {
					tenant := fingerprintTenantManager.GetTenant(orgID.String())
					trieClusterManager := tenant.GetTrieClusterManager()
					fp, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
					if err == nil {
						row[wkk.RowKeyCFingerprint] = fp
					}
				}
			}

			switch signalType {
			case filereader.SignalTypeLogs:
				if err := exemplarProcessor.ProcessLogsFromRow(ctx, orgID, row); err != nil {
					return fmt.Errorf("failed to process log row: %w", err)
				}
			case filereader.SignalTypeTraces:
				if err := exemplarProcessor.ProcessTracesFromRow(ctx, orgID, row); err != nil {
					return fmt.Errorf("failed to process trace row: %w", err)
				}
			}
		}
		pipeline.ReturnBatch(batch)
	}

	return nil
}

// createReaderStack creates a reader stack matching production ingestion
func createReaderStack(tmpFilename, orgID, bucket, objectID string, signalType filereader.SignalType, fingerprintTenantManager *fingerprint.TenantManager) (filereader.Reader, error) {
	// Create base reader
	options := filereader.ReaderOptions{
		SignalType: signalType,
		BatchSize:  1000,
		OrgID:      orgID,
	}
	reader, err := filereader.ReaderForFileWithOptions(tmpFilename, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create base reader: %w", err)
	}

	// Wrap with translator to add resource_* fields
	var translatedReader filereader.Reader
	switch signalType {
	case filereader.SignalTypeLogs:
		if strings.HasSuffix(tmpFilename, ".parquet") {
			translatedReader = metricsprocessing.NewParquetLogTranslatingReader(reader, orgID, bucket, objectID)
		} else {
			translator := metricsprocessing.NewLogTranslator(orgID, bucket, objectID, fingerprintTenantManager)
			var err error
			translatedReader, err = filereader.NewTranslatingReader(reader, translator, 1000)
			if err != nil {
				_ = reader.Close()
				return nil, fmt.Errorf("failed to create translating reader: %w", err)
			}
		}
	case filereader.SignalTypeTraces:
		translator := metricsprocessing.NewTraceTranslator(orgID, bucket, objectID)
		var err error
		translatedReader, err = filereader.NewTranslatingReader(reader, translator, 1000)
		if err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("failed to create translating reader: %w", err)
		}
	default:
		_ = reader.Close()
		return nil, fmt.Errorf("unsupported signal type: %v", signalType)
	}

	if translatedReader == nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to create translating reader: %w", err)
	}

	return translatedReader, nil
}

func parseOtelRawPath(objectID string) (uuid.UUID, string, error) {
	// otel-raw paths have format: otel-raw/{orgID}/{collector}/.../{signal}_{id}.ext
	// This matches the logic in internal/pubsub/parser.go:parseObjectKey
	parts := strings.Split(objectID, "/")
	if len(parts) < 4 {
		return uuid.UUID{}, "", fmt.Errorf("unexpected otel-raw key format: %s", objectID)
	}

	// Extract organization ID from position 1
	orgID, err := uuid.Parse(parts[1])
	if err != nil {
		return uuid.UUID{}, "", fmt.Errorf("invalid organization_id %q (key=%s): %w", parts[1], objectID, err)
	}

	// Extract signal from filename (last part)
	// The filename format is {signal}_{id}.ext, so we extract the part before the underscore
	filename := parts[len(parts)-1]
	signal := filename
	if idx := strings.Index(filename, "_"); idx != -1 {
		signal = filename[:idx]
	}

	return orgID, signal, nil
}
