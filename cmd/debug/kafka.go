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
)

func GetKafkaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kafka",
		Short: "Kafka debugging commands",
		Long:  `Kafka debugging commands for troubleshooting Kafka operations.`,
	}

	cmd.AddCommand(getConsumerLagCmd())
	cmd.AddCommand(getTailCmd())

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
	var useHardcodedGroups bool

	cmd := &cobra.Command{
		Use:   "consumer-lag",
		Short: "Show consumer group lag for topics",
		Long:  `Display lag information for Kafka consumer groups, showing current offset vs high water mark for each partition.`,
		RunE: func(c *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			factory := fly.NewFactory(&cfg.Fly)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			lags, err := getConsumerLag(ctx, factory, groupFilter, topicFilter, useHardcodedGroups)
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
	cmd.Flags().BoolVar(&useHardcodedGroups, "use-hardcoded-groups", false, "Use hardcoded group mappings instead of discovering all groups")

	return cmd
}

func getConsumerLag(ctx context.Context, factory *fly.Factory, groupFilter, topicFilter string, useHardcodedGroups bool) ([]PartitionLag, error) {
	// Create admin client using the factory's config
	conf := factory.GetConfig()
	adminClient := fly.NewAdminClient(conf)

	var allLags []PartitionLag

	if useHardcodedGroups {
		// Use the old hardcoded mappings for backwards compatibility
		topicGroups := map[string]string{
			"lakerunner.objstore.ingest.metrics": "lakerunner.ingest.metrics",
			"lakerunner.objstore.ingest.logs":    "lakerunner.ingest.logs",
			"lakerunner.objstore.ingest.traces":  "lakerunner.ingest.traces",
		}

		// Apply filters
		if groupFilter != "" || topicFilter != "" {
			filteredTopicGroups := make(map[string]string)
			for topic, groupID := range topicGroups {
				if groupFilter != "" && !strings.Contains(groupID, groupFilter) {
					continue
				}
				if topicFilter != "" && !strings.Contains(topic, topicFilter) {
					continue
				}
				filteredTopicGroups[topic] = groupID
			}
			topicGroups = filteredTopicGroups
		}

		// Get lag information using the admin client
		lagInfos, err := adminClient.GetMultipleConsumerGroupLag(ctx, topicGroups)
		if err != nil {
			return nil, fmt.Errorf("failed to get consumer group lag: %w", err)
		}

		// Convert to our output format
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
	} else {
		// Dynamically discover all topics and groups
		topics := config.KafkaTopics
		if topicFilter != "" {
			// Filter topics if specified
			filteredTopics := []string{}
			for _, topic := range topics {
				if strings.Contains(topic, topicFilter) {
					filteredTopics = append(filteredTopics, topic)
				}
			}
			topics = filteredTopics
		}

		if len(topics) == 0 {
			return nil, fmt.Errorf("no topics found matching filter")
		}

		// Create Kafka client to discover groups
		client, err := factory.CreateKafkaClient()
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka client: %w", err)
		}

		// Discover all consumer groups
		listGroupsResp, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{})
		if err != nil {
			return nil, fmt.Errorf("failed to list consumer groups: %w", err)
		}

		// Filter groups if specified
		groups := []string{}
		for _, group := range listGroupsResp.Groups {
			if groupFilter == "" || strings.Contains(group.GroupID, groupFilter) {
				groups = append(groups, group.GroupID)
			}
		}

		if len(groups) == 0 {
			return nil, fmt.Errorf("no consumer groups found matching filter")
		}

		// Use the new optimized batch method to get all lag information at once
		lagInfos, err := adminClient.GetAllConsumerGroupLags(ctx, topics, groups)
		if err != nil {
			return nil, fmt.Errorf("failed to get consumer group lags: %w", err)
		}

		// Convert to our output format
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

		// Check for N/A or 0 current offset
		if lag.CurrentOffset <= 0 {
			warningMsg := fmt.Sprintf("WARNING: Partition %d of topic %s has current_offset=%d, high=%d, lag=%d (excluded from calculations)",
				lag.Partition, lag.Topic, lag.CurrentOffset, lag.HighWaterMark, lag.Lag)
			warnings = append(warnings, warningMsg)
		} else {
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
	fmt.Fprintln(w, "TOPIC\tCONSUMER GROUP\tTOTAL LAG\tPARTITIONS")

	for _, summary := range sortedSummaries {
		partitionInfo := fmt.Sprintf("%d", summary.PartitionCount)
		if summary.ValidPartitions < summary.PartitionCount {
			partitionInfo = fmt.Sprintf("%d (%d valid)", summary.PartitionCount, summary.ValidPartitions)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
			summary.Topic,
			summary.ConsumerGroup,
			summary.TotalLag,
			partitionInfo)
	}

	return w.Flush()
}

func printLagTableDetailed(lags []PartitionLag) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tPARTITION\tCURRENT OFFSET\tHIGH WATER MARK\tLAG\tCONSUMER GROUP")

	for _, lag := range lags {
		currentOffsetStr := fmt.Sprintf("%d", lag.CurrentOffset)
		if lag.CurrentOffset < 0 {
			currentOffsetStr = "N/A"
		}

		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%s\n",
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

			factory := fly.NewFactory(&cfg.Fly)

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
	cmd.MarkFlagRequired("topic")

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
	defer reader.Close()

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
	for {
		// Check if we've hit our message limit
		if maxMessages > 0 && messagesRead >= maxMessages {
			break
		}

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
