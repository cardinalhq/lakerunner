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

package kafka

import (
        "context"
        "encoding/json"
        "fmt"
        "os"
        "sort"
        "text/tabwriter"
        "time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
)

var (
	apiKey    string
	adminAddr string
)

// SetAPIKey configures the API key used for auth with the admin service.
func SetAPIKey(key string) {
	apiKey = key
}

// GetKafkaCmd provides Kafka administrative commands.
func GetKafkaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kafka",
		Short: "Kafka administrative commands",
	}

	cmd.AddCommand(getConsumerLagCmd())
	return cmd
}

type PartitionLag struct {
	Topic         string
	Partition     int32
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
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			client, cleanup, err := createAdminClient()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx = createAuthContext(ctx)

			resp, err := client.GetConsumerLag(ctx, &adminproto.GetConsumerLagRequest{
				GroupFilter: groupFilter,
				TopicFilter: topicFilter,
			})
			if err != nil {
				return fmt.Errorf("failed to get consumer lag: %w", err)
			}

			lags := make([]PartitionLag, len(resp.Lags))
			for i, lag := range resp.Lags {
				lags[i] = PartitionLag{
					Topic:         lag.Topic,
					Partition:     lag.Partition,
					CurrentOffset: lag.CurrentOffset,
					HighWaterMark: lag.HighWaterMark,
					Lag:           lag.Lag,
					ConsumerGroup: lag.ConsumerGroup,
				}
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

	cmd.Flags().StringVar(&adminAddr, "addr", ":9091", "Address of the admin service")
	cmd.Flags().StringVar(&groupFilter, "group", "", "Filter by consumer group substring (optional)")
	cmd.Flags().StringVar(&topicFilter, "topic", "", "Filter by topic substring (optional)")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	cmd.Flags().BoolVar(&detailed, "detailed", false, "Show detailed partition-level information")

	return cmd
}

func createAdminClient() (adminproto.AdminServiceClient, func(), error) {
	conn, err := grpc.Dial(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to admin service: %w", err)
	}
	client := adminproto.NewAdminServiceClient(conn)
	cleanup := func() { _ = conn.Close() }
	return client, cleanup, nil
}

func createAuthContext(ctx context.Context) context.Context {
	if apiKey != "" {
		md := metadata.New(map[string]string{"authorization": apiKey})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func printLagSummary(lags []PartitionLag) error {
	type Summary struct {
		Topic          string
		ConsumerGroup  string
		TotalLag       int64
		PartitionCount int
	}

	summaries := make(map[string]*Summary)
	for _, lag := range lags {
		key := fmt.Sprintf("%s:%s", lag.Topic, lag.ConsumerGroup)
		if _, ok := summaries[key]; !ok {
			summaries[key] = &Summary{Topic: lag.Topic, ConsumerGroup: lag.ConsumerGroup}
		}
		summaries[key].TotalLag += lag.Lag
		summaries[key].PartitionCount++
	}

	var sorted []*Summary
	for _, s := range summaries {
		sorted = append(sorted, s)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Topic != sorted[j].Topic {
			return sorted[i].Topic < sorted[j].Topic
		}
		return sorted[i].ConsumerGroup < sorted[j].ConsumerGroup
	})

        w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tGROUP\tLAG\tPARTITIONS")
	for _, s := range sorted {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\n", s.Topic, s.ConsumerGroup, s.TotalLag, s.PartitionCount)
	}
	return w.Flush()
}

func printLagTableDetailed(lags []PartitionLag) error {
	sort.Slice(lags, func(i, j int) bool {
		if lags[i].Topic != lags[j].Topic {
			return lags[i].Topic < lags[j].Topic
		}
		if lags[i].ConsumerGroup != lags[j].ConsumerGroup {
			return lags[i].ConsumerGroup < lags[j].ConsumerGroup
		}
		return lags[i].Partition < lags[j].Partition
	})

        w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TOPIC\tGROUP\tPARTITION\tCURRENT\tHWM\tLAG")
	for _, l := range lags {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\n", l.Topic, l.ConsumerGroup, l.Partition, l.CurrentOffset, l.HighWaterMark, l.Lag)
	}
	return w.Flush()
}

func printLagJSON(lags []PartitionLag) error {
        enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(lags)
}
