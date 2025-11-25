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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func GetKafkaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kafka",
		Short: "Kafka debugging commands",
		Long:  `Kafka debugging commands for troubleshooting Kafka operations.`,
	}

	cmd.AddCommand(getConsumerLagCmd())
	cmd.AddCommand(getTailCmd())
	cmd.AddCommand(getFlushConsumerCmd())

	return cmd
}

type PartitionLag struct {
	Topic         string
	Partition     int
	CurrentOffset int64
	HighWaterMark int64
	Lag           int64
	ConsumerGroup string
}

func getConsumerLagCmd() *cobra.Command {
	var groupFilter string
	var topicFilter string
	var jsonOutput bool
	var detailed bool

	cmd := &cobra.Command{
		Use:   "consumer-lag",
		Short: "Show consumer group lag for topics",
		Long:  `Display lag information for Kafka consumer groups, showing current offset vs high water mark for each partition.`,
		RunE: func(c *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			factory := fly.NewFactory(&cfg.Kafka)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			cnf, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			lags, err := getConsumerLag(ctx, cnf, factory, groupFilter, topicFilter)
			if err != nil {
				return err
			}

			if len(lags) == 0 {
				fmt.Println("No consumer groups found or no lag data available")
				return nil
			}

			if jsonOutput {
				return printLagJSON(lags)
			}
			if detailed {
				return printLagTableDetailed(lags)
			}
			return printLagSummary(lags)
		},
	}

	cmd.Flags().StringVar(&groupFilter, "group", "", "Filter by consumer group substring (optional)")
	cmd.Flags().StringVar(&topicFilter, "topic", "", "Filter by topic substring (optional)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().BoolVar(&detailed, "detailed", false, "Show detailed partition-level information")

	return cmd
}

func getConsumerLag(ctx context.Context, cnf *config.Config, factory *fly.Factory, groupFilter, topicFilter string) ([]PartitionLag, error) {
	// Create admin client using the factory's config
	conf := factory.GetConfig()
	adminClient, err := fly.NewAdminClient(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	// Use the consumer lag monitor service mappings
	monitor, err := fly.NewConsumerLagMonitor(cnf, time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer lag monitor: %w", err)
	}
	serviceMappings := monitor.GetServiceMappings()

	// Build topic groups from service mappings
	topicGroups := make(map[string]string)
	for _, mapping := range serviceMappings {
		// Apply filters
		if groupFilter != "" && !strings.Contains(mapping.ConsumerGroup, groupFilter) {
			continue
		}
		if topicFilter != "" && !strings.Contains(mapping.Topic, topicFilter) {
			continue
		}
		topicGroups[mapping.Topic] = mapping.ConsumerGroup
	}

	if len(topicGroups) == 0 {
		return nil, fmt.Errorf("no topic/group combinations found matching filters")
	}

	// Get lag information using the admin client
	lagInfos, err := adminClient.GetMultipleConsumerGroupLag(ctx, topicGroups)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group lags: %w", err)
	}

	// Convert to our output format
	var allLags []PartitionLag
	for _, lagInfo := range lagInfos {
		allLags = append(allLags, PartitionLag{
			Topic:         lagInfo.Topic,
			Partition:     lagInfo.Partition,
			CurrentOffset: lagInfo.CommittedOffset,
			HighWaterMark: lagInfo.HighWaterMark,
			Lag:           lagInfo.Lag,
			ConsumerGroup: lagInfo.GroupID,
		})
	}

	// Sort by topic, group, then partition
	sort.Slice(allLags, func(i, j int) bool {
		if allLags[i].Topic != allLags[j].Topic {
			return allLags[i].Topic < allLags[j].Topic
		}
		if allLags[i].ConsumerGroup != allLags[j].ConsumerGroup {
			return allLags[i].ConsumerGroup < allLags[j].ConsumerGroup
		}
		return allLags[i].Partition < allLags[j].Partition
	})

	return allLags, nil
}

func printLagSummary(lags []PartitionLag) error {
	if len(lags) == 0 {
		return nil
	}

	// Group by topic and consumer group
	type Summary struct {
		Topic           string
		ConsumerGroup   string
		TotalLag        int64
		PartitionCount  int
		ValidPartitions int
	}

	summaries := make(map[string]*Summary)
	warnings := []string{}

	for _, lag := range lags {
		key := fmt.Sprintf("%s:%s", lag.Topic, lag.ConsumerGroup)
		if _, exists := summaries[key]; !exists {
			summaries[key] = &Summary{
				Topic:         lag.Topic,
				ConsumerGroup: lag.ConsumerGroup,
			}
		}

		summaries[key].PartitionCount++

		// Check for committed offsets
		if lag.CurrentOffset < 0 {
			// No committed offset yet - this is normal for new consumer groups
			// Include the lag (which equals high water mark) in calculations
			summaries[key].TotalLag += lag.Lag
			summaries[key].ValidPartitions++
		} else if lag.CurrentOffset == 0 && lag.HighWaterMark > 0 {
			// Consumer has committed offset 0 but there are messages - might be an issue
			warningMsg := fmt.Sprintf("WARNING: Partition %d of topic %s has committed offset 0 but high_water_mark=%d, lag=%d",
				lag.Partition, lag.Topic, lag.HighWaterMark, lag.Lag)
			warnings = append(warnings, warningMsg)
			summaries[key].TotalLag += lag.Lag
			summaries[key].ValidPartitions++
		} else {
			// Normal case with committed offsets
			summaries[key].TotalLag += lag.Lag
			summaries[key].ValidPartitions++
		}
	}

	// Convert map to slice and sort
	var sortedSummaries []*Summary
	for _, summary := range summaries {
		sortedSummaries = append(sortedSummaries, summary)
	}
	sort.Slice(sortedSummaries, func(i, j int) bool {
		if sortedSummaries[i].Topic != sortedSummaries[j].Topic {
			return sortedSummaries[i].Topic < sortedSummaries[j].Topic
		}
		return sortedSummaries[i].ConsumerGroup < sortedSummaries[j].ConsumerGroup
	})

	// Print warnings first
	for _, warning := range warnings {
		fmt.Fprintln(os.Stderr, warning)
	}
	if len(warnings) > 0 {
		fmt.Fprintln(os.Stderr) // Add blank line after warnings
	}

	// Print summary table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TOPIC\tCONSUMER GROUP\tTOTAL LAG\tPARTITIONS")

	for _, summary := range sortedSummaries {
		partitionInfo := fmt.Sprintf("%d", summary.PartitionCount)
		if summary.ValidPartitions < summary.PartitionCount {
			partitionInfo = fmt.Sprintf("%d (%d valid)", summary.PartitionCount, summary.ValidPartitions)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
			summary.Topic,
			summary.ConsumerGroup,
			summary.TotalLag,
			partitionInfo)
	}

	return w.Flush()
}

func printLagTableDetailed(lags []PartitionLag) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TOPIC\tPARTITION\tCURRENT OFFSET\tHIGH WATER MARK\tLAG\tCONSUMER GROUP")

	for _, lag := range lags {
		currentOffsetStr := fmt.Sprintf("%d", lag.CurrentOffset)
		if lag.CurrentOffset < 0 {
			currentOffsetStr = "N/A"
		}

		_, _ = fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%s\n",
			lag.Topic,
			lag.Partition,
			currentOffsetStr,
			lag.HighWaterMark,
			lag.Lag,
			lag.ConsumerGroup)
	}

	return w.Flush()
}

func printLagJSON(lags []PartitionLag) error {
	fmt.Println("[")
	for i, lag := range lags {
		if i > 0 {
			fmt.Println(",")
		}
		fmt.Printf(`  {
    "topic": "%s",
    "partition": %d,
    "current_offset": %d,
    "high_water_mark": %d,
    "lag": %d,
    "consumer_group": "%s"
  }`, lag.Topic, lag.Partition, lag.CurrentOffset, lag.HighWaterMark, lag.Lag, lag.ConsumerGroup)
	}
	fmt.Println("\n]")
	return nil
}

func getTailCmd() *cobra.Command {
	var partition int
	var offsetStr string
	var jsonOutput bool
	var maxMessages int
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "tail --topic TOPIC [flags]",
		Short: "Tail messages from a Kafka topic",
		Long: `Tail messages from a Kafka topic, starting from a specified offset.

Offset can be:
  - "latest" (default): Start from the most recent message
  - "first": Start from the beginning of the partition
  - A number: Start from that specific offset

Examples:
  lakerunner debug kafka tail --topic my-topic
  lakerunner debug kafka tail --topic my-topic --partition 1 --offset first
  lakerunner debug kafka tail --topic my-topic --partition 0 --offset 1000
  lakerunner debug kafka tail --topic my-topic --max 10 --json`,
		RunE: func(c *cobra.Command, args []string) error {
			topic := c.Flag("topic").Value.String()
			if topic == "" {
				return fmt.Errorf("--topic is required")
			}

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			factory := fly.NewFactory(&cfg.Kafka)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Parse offset
			var offset int64
			switch offsetStr {
			case "latest":
				offset = kafka.LastOffset
			case "first":
				offset = kafka.FirstOffset
			default:
				parsed, err := strconv.ParseInt(offsetStr, 10, 64)
				if err != nil {
					return fmt.Errorf("invalid offset '%s': must be 'latest', 'first', or a number", offsetStr)
				}
				offset = parsed
			}

			return tailTopic(ctx, factory, topic, partition, offset, jsonOutput, maxMessages, timeout)
		},
	}

	cmd.Flags().StringVar(&offsetStr, "offset", "latest", "Starting offset: 'latest', 'first', or a number")
	cmd.Flags().IntVar(&partition, "partition", 0, "Partition to read from")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output messages in JSON format")
	cmd.Flags().IntVar(&maxMessages, "max", 0, "Maximum number of messages to read (0 = unlimited)")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Timeout for reading messages")
	cmd.Flags().String("topic", "", "Topic to tail (required)")
	_ = cmd.MarkFlagRequired("topic")

	return cmd
}

func tailTopic(ctx context.Context, factory *fly.Factory, topic string, partition int, offset int64, jsonOutput bool, maxMessages int, timeout time.Duration) error {
	// Get the factory config for creating the authenticated dialer
	config := factory.GetConfig()

	// Create SASL mechanism if needed
	var saslMechanism sasl.Mechanism
	if config.SASLEnabled {
		switch config.SASLMechanism {
		case "SCRAM-SHA-256":
			var err error
			saslMechanism, err = scram.Mechanism(scram.SHA256, config.SASLUsername, config.SASLPassword)
			if err != nil {
				return fmt.Errorf("failed to create SCRAM-SHA-256 mechanism: %w", err)
			}
		case "SCRAM-SHA-512":
			var err error
			saslMechanism, err = scram.Mechanism(scram.SHA512, config.SASLUsername, config.SASLPassword)
			if err != nil {
				return fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %w", err)
			}
		case "PLAIN":
			saslMechanism = plain.Mechanism{
				Username: config.SASLUsername,
				Password: config.SASLPassword,
			}
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s", config.SASLMechanism)
		}
	}

	// Create TLS config if needed
	var tlsConfig *tls.Config
	if config.TLSEnabled {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: config.TLSSkipVerify,
		}
	}

	// Create the authenticated dialer
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		SASLMechanism: saslMechanism,
		TLS:           tlsConfig,
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:   config.Brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10e6, // 10MB
		Dialer:    dialer,
	}

	reader := kafka.NewReader(readerConfig)
	defer func() { _ = reader.Close() }()

	// Seek to the desired offset
	if err := reader.SetOffset(offset); err != nil {
		return fmt.Errorf("failed to set offset to %d: %w", offset, err)
	}

	// Show connection info
	fmt.Fprintf(os.Stderr, "Connecting to Kafka brokers: %v\n", config.Brokers)
	if config.SASLEnabled {
		fmt.Fprintf(os.Stderr, "Using SASL authentication: %s (user: %s)\n", config.SASLMechanism, config.SASLUsername)
	}
	if config.TLSEnabled {
		fmt.Fprintf(os.Stderr, "TLS enabled (skip verify: %t)\n", config.TLSSkipVerify)
	}

	// Try to get the current stats to verify topic/partition exists
	stats := reader.Stats()
	fmt.Fprintf(os.Stderr, "Connected to topic: %s, partition: %s\n", stats.Topic, stats.Partition)

	// Set up timeout context if specified
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	fmt.Fprintf(os.Stderr, "Tailing topic %s partition %d from offset %s...\n", topic, partition, formatOffset(offset))
	if maxMessages > 0 {
		fmt.Fprintf(os.Stderr, "Will read at most %d messages\n", maxMessages)
	}
	fmt.Fprintf(os.Stderr, "Press Ctrl+C to stop\n\n")

	messagesRead := 0
	for maxMessages <= 0 || messagesRead < maxMessages {

		// Read message with context
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if ctx.Err() == context.DeadlineExceeded {
					fmt.Fprintf(os.Stderr, "\nTimeout reached after %v\n", timeout)
				} else {
					fmt.Fprintf(os.Stderr, "\nStopped\n")
				}
				break
			}
			// Check for common EOF-related errors
			errStr := err.Error()
			if strings.Contains(errStr, "EOF") {
				if messagesRead == 0 {
					return fmt.Errorf("failed to read any messages - this could be due to:\n"+
						"  • Topic/partition doesn't exist\n"+
						"  • No messages at the specified offset\n"+
						"  • Network connectivity issues\n"+
						"  • Authentication problems\n"+
						"Original error: %w", err)
				} else {
					// Got EOF after reading some messages, this might be normal
					fmt.Fprintf(os.Stderr, "\nConnection closed after reading %d messages\n", messagesRead)
					break
				}
			}
			return fmt.Errorf("failed to read message: %w", err)
		}

		messagesRead++

		if jsonOutput {
			outputMessageJSON(msg)
		} else {
			outputMessageHuman(msg)
		}
	}

	fmt.Fprintf(os.Stderr, "\nRead %d messages\n", messagesRead)
	return nil
}

func formatOffset(offset int64) string {
	switch offset {
	case kafka.FirstOffset:
		return "first"
	case kafka.LastOffset:
		return "latest"
	default:
		return strconv.FormatInt(offset, 10)
	}
}

func outputMessageJSON(msg kafka.Message) {
	messageData := map[string]interface{}{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
		"value":     string(msg.Value),
		"time":      msg.Time.Format(time.RFC3339),
	}

	// Add headers if present
	if len(msg.Headers) > 0 {
		headers := make(map[string]string)
		for _, header := range msg.Headers {
			headers[header.Key] = string(header.Value)
		}
		messageData["headers"] = headers
	}

	jsonBytes, err := json.MarshalIndent(messageData, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling message to JSON: %v\n", err)
		return
	}
	fmt.Println(string(jsonBytes))
}

func outputMessageHuman(msg kafka.Message) {
	fmt.Printf("=== Message ===\n")
	fmt.Printf("Topic: %s\n", msg.Topic)
	fmt.Printf("Partition: %d\n", msg.Partition)
	fmt.Printf("Offset: %d\n", msg.Offset)
	fmt.Printf("Time: %s\n", msg.Time.Format(time.RFC3339))

	if len(msg.Key) > 0 {
		fmt.Printf("Key: %s\n", string(msg.Key))
	}

	if len(msg.Headers) > 0 {
		fmt.Printf("Headers:\n")
		for _, header := range msg.Headers {
			fmt.Printf("  %s: %s\n", header.Key, string(header.Value))
		}
	}

	fmt.Printf("Value:\n%s\n\n", string(msg.Value))
}

func getFlushConsumerCmd() *cobra.Command {
	var consumerGroup string
	var topic string
	var dryRun bool
	var cleanupTracking bool

	cmd := &cobra.Command{
		Use:   "flush-consumer",
		Short: "Flush a Kafka consumer group to current high water marks",
		Long: `Sets up skip entries in the database that will cause consumers to skip
to the current high water marks for all partitions. This allows flushing
the consumer without requiring a shutdown.

Running consumers will periodically check for skip entries and will
automatically jump to the specified offsets when found.

Examples:
  # Flush all topics for a consumer group
  lakerunner debug kafka flush-consumer --group lakerunner.ingest.logs

  # Flush a specific topic
  lakerunner debug kafka flush-consumer --group lakerunner.ingest.logs --topic lakerunner.objstore.ingest.logs

  # Dry run to see what would be flushed
  lakerunner debug kafka flush-consumer --group lakerunner.ingest.logs --dry-run`,
		RunE: func(c *cobra.Command, args []string) error {
			if consumerGroup == "" {
				return fmt.Errorf("--group is required")
			}

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			return flushConsumer(ctx, cfg, consumerGroup, topic, dryRun, cleanupTracking)
		},
	}

	cmd.Flags().StringVar(&consumerGroup, "group", "", "Consumer group to flush (required)")
	cmd.Flags().StringVar(&topic, "topic", "", "Specific topic to flush (optional, defaults to all topics for group)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be flushed without making changes")
	cmd.Flags().BoolVar(&cleanupTracking, "cleanup-tracking", false, "Also cleanup kafka_offset_tracker entries below skip offsets")
	_ = cmd.MarkFlagRequired("group")

	return cmd
}

func flushConsumer(ctx context.Context, cfg *config.Config, consumerGroup, topic string, dryRun, cleanupTracking bool) error {
	factory := fly.NewFactory(&cfg.Kafka)
	adminClient, err := fly.NewAdminClient(&cfg.Kafka)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}

	// Get all topics from service mappings
	monitor, err := fly.NewConsumerLagMonitor(cfg, time.Minute)
	if err != nil {
		return fmt.Errorf("failed to create consumer lag monitor: %w", err)
	}
	serviceMappings := monitor.GetServiceMappings()

	// Determine which topics to flush
	var topicsToFlush []string
	if topic != "" {
		topicsToFlush = []string{topic}
	} else {
		// Get all topics for this consumer group from service mappings
		topicSet := make(map[string]bool)
		for _, mapping := range serviceMappings {
			if mapping.ConsumerGroup == consumerGroup {
				topicSet[mapping.Topic] = true
			}
		}
		for t := range topicSet {
			topicsToFlush = append(topicsToFlush, t)
		}
		sort.Strings(topicsToFlush)
	}

	if len(topicsToFlush) == 0 {
		return fmt.Errorf("no topics found for consumer group %s", consumerGroup)
	}

	fmt.Printf("Flushing consumer group: %s\n", consumerGroup)
	fmt.Printf("Topics to flush: %v\n", topicsToFlush)
	fmt.Printf("Kafka brokers: %v\n", factory.GetConfig().Brokers)
	if dryRun {
		fmt.Print("\n*** DRY RUN - No changes will be made ***\n\n")
	}
	fmt.Println()

	// Collect all skip entries
	type skipEntry struct {
		topic         string
		partition     int
		currentOffset int64
		highWaterMark int64
		lag           int64
	}
	var skipEntries []skipEntry

	for _, t := range topicsToFlush {
		lags, err := adminClient.GetConsumerGroupLag(ctx, t, consumerGroup)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to get lag for topic %s: %v\n", t, err)
			continue
		}

		for _, lag := range lags {
			skipEntries = append(skipEntries, skipEntry{
				topic:         lag.Topic,
				partition:     lag.Partition,
				currentOffset: lag.CommittedOffset,
				highWaterMark: lag.HighWaterMark,
				lag:           lag.Lag,
			})
		}
	}

	if len(skipEntries) == 0 {
		fmt.Println("No partitions found to flush")
		return nil
	}

	// Print what we're going to do
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TOPIC\tPARTITION\tCURRENT\tHWM\tLAG\tSKIP TO")
	totalLag := int64(0)
	for _, entry := range skipEntries {
		currentStr := fmt.Sprintf("%d", entry.currentOffset)
		if entry.currentOffset < 0 {
			currentStr = "N/A"
		}
		_, _ = fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\n",
			entry.topic,
			entry.partition,
			currentStr,
			entry.highWaterMark,
			entry.lag,
			entry.highWaterMark)
		totalLag += entry.lag
	}
	_ = w.Flush()
	fmt.Printf("\nTotal messages to skip: %d\n", totalLag)

	if dryRun {
		fmt.Println("\nDry run complete. Use without --dry-run to apply changes.")
		return nil
	}

	// Connect to database
	store, err := lrdb.LRDBStoreForAdmin(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to LRDB: %w", err)
	}
	defer store.Close()

	// Insert skip entries
	fmt.Println("\nInserting skip entries...")
	for _, entry := range skipEntries {
		err := store.InsertKafkaOffsetSkip(ctx, lrdb.InsertKafkaOffsetSkipParams{
			ConsumerGroup: consumerGroup,
			Topic:         entry.topic,
			PartitionID:   int32(entry.partition),
			SkipToOffset:  entry.highWaterMark,
			CreatedAt:     nil, // Use default (now())
		})
		if err != nil {
			return fmt.Errorf("failed to insert skip entry for %s:%d: %w", entry.topic, entry.partition, err)
		}
		fmt.Printf("  Inserted skip entry: %s partition %d -> offset %d\n", entry.topic, entry.partition, entry.highWaterMark)
	}

	// Optionally cleanup old tracking entries
	if cleanupTracking {
		fmt.Println("\nCleaning up kafka_offset_tracker entries...")
		for _, entry := range skipEntries {
			rows, err := store.CleanupKafkaOffsets(ctx, lrdb.CleanupKafkaOffsetsParams{
				ConsumerGroup: consumerGroup,
				Topic:         entry.topic,
				PartitionID:   int32(entry.partition),
				MaxOffset:     entry.highWaterMark,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to cleanup tracking for %s:%d: %v\n", entry.topic, entry.partition, err)
				continue
			}
			if rows > 0 {
				fmt.Printf("  Cleaned up %d tracking entries for %s partition %d\n", rows, entry.topic, entry.partition)
			}
		}
	}

	fmt.Println("\nFlush complete. Running consumers will pick up skip entries on their next check.")
	return nil
}
