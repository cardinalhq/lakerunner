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
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/segmentio/kafka-go"
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
			if !factory.IsEnabled() {
				return fmt.Errorf("Kafka is not enabled in configuration")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			lags, err := getConsumerLag(ctx, factory, groupFilter, topicFilter)
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
			return printLagTable(lags)
		},
	}

	cmd.Flags().StringVar(&groupFilter, "group", "", "Filter by consumer group (optional)")
	cmd.Flags().StringVar(&topicFilter, "topic", "", "Filter by topic (optional)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	return cmd
}

func getConsumerLag(ctx context.Context, factory *fly.Factory, groupFilter, topicFilter string) ([]PartitionLag, error) {
	cfg := factory.GetConfig()

	var allLags []PartitionLag

	// Define the topic/group mappings we're interested in
	topicGroups := map[string]string{
		"lakerunner.objstore.ingest.metrics": "lakerunner.ingest.metrics",
		"lakerunner.objstore.ingest.logs":    "lakerunner.ingest.logs",
		"lakerunner.objstore.ingest.traces":  "lakerunner.ingest.traces",
	}

	for topic, groupID := range topicGroups {
		// Apply filters
		if groupFilter != "" && groupID != groupFilter {
			continue
		}
		if topicFilter != "" && topic != topicFilter {
			continue
		}

		topicLags, err := getTopicLag(ctx, cfg, topic, groupID)
		if err != nil {
			fmt.Printf("Warning: failed to get lag for topic %s, group %s: %v\n", topic, groupID, err)
			continue
		}
		allLags = append(allLags, topicLags...)
	}

	// Sort by topic, then partition
	sort.Slice(allLags, func(i, j int) bool {
		if allLags[i].Topic != allLags[j].Topic {
			return allLags[i].Topic < allLags[j].Topic
		}
		return allLags[i].Partition < allLags[j].Partition
	})

	return allLags, nil
}

func getTopicLag(ctx context.Context, cfg *fly.Config, topic, groupID string) ([]PartitionLag, error) {
	// Connect to get topic metadata
	conn, err := kafka.Dial("tcp", cfg.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Get topic partitions
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions for topic %s: %w", topic, err)
	}

	var lags []PartitionLag

	for _, partition := range partitions {
		// Get high water mark (latest offset)
		partConn, err := kafka.DialLeader(ctx, "tcp", cfg.Brokers[0], topic, partition.ID)
		if err != nil {
			fmt.Printf("Warning: failed to connect to partition leader for %s:%d: %v\n", topic, partition.ID, err)
			continue
		}

		// Get the last offset (high water mark)
		lastOffset, err := partConn.ReadLastOffset()
		if err != nil {
			partConn.Close()
			fmt.Printf("Warning: failed to read last offset for %s:%d: %v\n", topic, partition.ID, err)
			continue
		}
		partConn.Close()

		// Get committed offset for this consumer group using a temporary reader
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   cfg.Brokers,
			Topic:     topic,
			Partition: partition.ID, // Specify exact partition
			GroupID:   groupID,
		})

		// Try to get stats which includes offset information
		stats := reader.Stats()
		committedOffset := int64(-1)

		// The Offset field in stats gives us the current position
		if stats.Offset >= 0 {
			committedOffset = stats.Offset
		}

		reader.Close()

		// Calculate lag
		lag := int64(0)
		if committedOffset >= 0 {
			lag = max(lastOffset-committedOffset, 0)
		} else {
			// If no committed offset, lag is the total messages in partition
			lag = lastOffset
		}

		lags = append(lags, PartitionLag{
			Topic:         topic,
			Partition:     partition.ID,
			CurrentOffset: committedOffset,
			HighWaterMark: lastOffset,
			Lag:           lag,
			ConsumerGroup: groupID,
		})
	}

	return lags, nil
}

func printLagTable(lags []PartitionLag) error {
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
