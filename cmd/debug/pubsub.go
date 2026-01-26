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

package debug

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/pubsub"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

func GetPubSubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pubsub",
		Short: "PubSub debugging utilities",
		Long:  `Various utilities for debugging and replaying pubsub messages.`,
	}

	cmd.AddCommand(getReplaySubCmd())

	return cmd
}

func getReplaySubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "replay [FILE|-]",
		Short: "Replay files into the pubsub queue system",
		Long: `Replay files into the pubsub queue system by creating Kafka messages.
The file should contain tab-separated values with columns: bucket, object_id, source.
Use "-" to read from stdin.`,
		Args: cobra.ExactArgs(1),
		RunE: runReplayCmd,
	}

	cmd.Flags().Int64("file-size", 500*1024, "Default file size to assume for files (in bytes)")

	return cmd
}

func runReplayCmd(c *cobra.Command, args []string) error {
	ctx := context.Background()

	filename := args[0]
	fileSize, _ := c.Flags().GetInt64("file-size")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize storage profile provider
	cdb, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to create configdb store: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	// Initialize Kafka factory and handler
	flyFactory := fly.NewFactory(&cfg.Kafka)

	// Create a deduplicator (we'll use a no-op one for replay)
	deduplicator := &NoOpDeduplicator{}

	kafkaHandler, err := pubsub.NewKafkaHandler(ctx, cfg, flyFactory, "debug-replay", sp, deduplicator)
	if err != nil {
		return fmt.Errorf("failed to create Kafka handler: %w", err)
	}
	defer func() {
		if err := kafkaHandler.Close(); err != nil {
			slog.Error("Failed to close Kafka handler", slog.Any("error", err))
		}
	}()

	// Open input
	var reader io.Reader
	if filename == "-" {
		reader = os.Stdin
		slog.Info("Reading replay data from stdin")
	} else {
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", filename, err)
		}
		defer func() {
			if err := file.Close(); err != nil {
				slog.Error("Failed to close file", slog.String("file", filename), slog.Any("error", err))
			}
		}()
		reader = file
		slog.Info("Reading replay data", slog.String("file", filename))
	}

	scanner := bufio.NewScanner(reader)
	lineNum := 0
	processed := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and header
		if line == "" || strings.HasPrefix(line, "bucket") {
			continue
		}

		// Parse tab-separated values
		parts := strings.Split(line, "|")
		if len(parts) < 3 {
			slog.Warn("Skipping malformed line", slog.Int("line", lineNum), slog.String("content", line))
			continue
		}

		bucket := strings.TrimSpace(parts[0])
		objectID := strings.TrimSpace(parts[1])
		source := strings.TrimSpace(parts[2])

		if bucket == "" || objectID == "" {
			slog.Warn("Skipping line with empty bucket or object_id", slog.Int("line", lineNum))
			continue
		}

		// Create a simulated S3-like event message
		if err := replayFile(ctx, kafkaHandler, bucket, objectID, source, fileSize); err != nil {
			slog.Error("Failed to replay file",
				slog.String("bucket", bucket),
				slog.String("object_id", objectID),
				slog.Any("error", err))
			continue
		}

		processed++
		if processed%100 == 0 {
			slog.Info("Progress", slog.Int("processed", processed))
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	slog.Info("Replay completed", slog.Int("total_processed", processed))
	return nil
}

func replayFile(ctx context.Context, handler *pubsub.KafkaHandler, bucket, objectID, source string, fileSize int64) error {
	// Create a simulated S3 event JSON that mimics what we'd get from SQS
	s3EventJSON := fmt.Sprintf(`{
  "Records": [
    {
      "s3": {
        "bucket": {
          "name": "%s"
        },
        "object": {
          "key": "%s",
          "size": %d
        }
      }
    }
  ]
}`, bucket, objectID, fileSize)

	// Send the simulated event through the Kafka handler
	return handler.HandleMessage(ctx, []byte(s3EventJSON))
}

// NoOpDeduplicator is a deduplicator that never deduplicates (for replay scenarios)
type NoOpDeduplicator struct{}

func (d *NoOpDeduplicator) CheckAndRecord(ctx context.Context, bucket, objectID, source string) (bool, error) {
	return true, nil // Always allow processing
}

func (d *NoOpDeduplicator) CheckAndRecordBatch(ctx context.Context, items []pubsub.DedupItem, source string) ([]pubsub.DedupItem, error) {
	return items, nil // Always allow all items
}
