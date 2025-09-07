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

			factory := fly.NewFactory(&cfg.Fly)

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
			if detailed {
				return printLagTableDetailed(lags)
			}
			return printLagSummary(lags)
		},
	}

	cmd.Flags().StringVar(&groupFilter, "group", "", "Filter by consumer group (optional)")
	cmd.Flags().StringVar(&topicFilter, "topic", "", "Filter by topic (optional)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().BoolVar(&detailed, "detailed", false, "Show detailed partition-level information")

	return cmd
}

func getConsumerLag(ctx context.Context, factory *fly.Factory, groupFilter, topicFilter string) ([]PartitionLag, error) {
	// Create admin client using the factory's config
	config := factory.GetConfig()
	adminClient := fly.NewAdminClient(config)

	// Define the topic/group mappings we're interested in
	topicGroups := map[string]string{
		"lakerunner.objstore.ingest.metrics": "lakerunner.ingest.metrics",
		"lakerunner.objstore.ingest.logs":    "lakerunner.ingest.logs",
		"lakerunner.objstore.ingest.traces":  "lakerunner.ingest.traces",
	}

	// Apply filters
	if groupFilter != "" || topicFilter != "" {
		filteredTopicGroups := make(map[string]string)
		for topic, groupID := range topicGroups {
			if groupFilter != "" && groupID != groupFilter {
				continue
			}
			if topicFilter != "" && topic != topicFilter {
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
	var lags []PartitionLag
	for _, lagInfo := range lagInfos {
		lags = append(lags, PartitionLag{
			Topic:         lagInfo.Topic,
			Partition:     lagInfo.Partition,
			CurrentOffset: lagInfo.CommittedOffset,
			HighWaterMark: lagInfo.HighWaterMark,
			Lag:           lagInfo.Lag,
			ConsumerGroup: lagInfo.GroupID,
		})
	}

	// Sort by topic, then partition
	sort.Slice(lags, func(i, j int) bool {
		if lags[i].Topic != lags[j].Topic {
			return lags[i].Topic < lags[j].Topic
		}
		return lags[i].Partition < lags[j].Partition
	})

	return lags, nil
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
