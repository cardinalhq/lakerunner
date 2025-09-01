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
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cardinalhq/lakerunner/internal/externalscaler"
)

func GetScalingCmd() *cobra.Command {
	scalingCmd := &cobra.Command{
		Use:   "scaling",
		Short: "Scaling debugging commands",
	}

	scalingCmd.AddCommand(getPollCmd())

	return scalingCmd
}

func getPollCmd() *cobra.Command {
	var (
		address     string
		interval    time.Duration
		serviceType string
	)

	cmd := &cobra.Command{
		Use:   "poll",
		Short: "Poll external scaler gRPC endpoints for scaling metrics",
		Long: `Poll the external scaler gRPC service to test scaling functionality.

Available service types:
  - ingest-logs
  - ingest-metrics  
  - ingest-traces
  - compact-logs
  - compact-metrics
  - compact-traces
  - rollup-metrics`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPoll(cmd.Context(), address, interval, serviceType)
		},
	}

	cmd.Flags().StringVar(&address, "address", "localhost:8080", "External scaler gRPC address")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "Polling interval")
	cmd.Flags().StringVar(&serviceType, "service-type", "ingest-logs", "Service type to poll for metrics")

	return cmd
}

func runPoll(ctx context.Context, address string, interval time.Duration, serviceType string) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to external scaler: %w", err)
	}
	defer conn.Close()

	client := externalscaler.NewExternalScalerClient(conn)

	scaledObjectRef := &externalscaler.ScaledObjectRef{
		Name:      fmt.Sprintf("test-%s", serviceType),
		Namespace: "default",
		ScalerMetadata: map[string]string{
			"serviceType": serviceType,
		},
	}

	fmt.Printf("Polling external scaler at %s every %v for service type: %s\n", address, interval, serviceType)
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := pollOnce(ctx, client, scaledObjectRef, serviceType); err != nil {
				slog.Error("Poll failed", slog.Any("error", err))
			}
		}
	}
}

func pollOnce(ctx context.Context, client externalscaler.ExternalScalerClient, scaledObjectRef *externalscaler.ScaledObjectRef, serviceType string) error {
	timestamp := time.Now().Format("15:04:05")

	// Test IsActive
	isActiveResp, err := client.IsActive(ctx, scaledObjectRef)
	if err != nil {
		fmt.Printf("[%s] IsActive ERROR: %v\n", timestamp, err)
		return err
	}

	// Test GetMetricSpec
	metricSpecResp, err := client.GetMetricSpec(ctx, scaledObjectRef)
	if err != nil {
		fmt.Printf("[%s] GetMetricSpec ERROR: %v\n", timestamp, err)
		return err
	}

	// Test GetMetrics
	if len(metricSpecResp.MetricSpecs) > 0 {
		metricName := metricSpecResp.MetricSpecs[0].MetricName
		getMetricsReq := &externalscaler.GetMetricsRequest{
			ScaledObjectRef: scaledObjectRef,
			MetricName:      metricName,
		}

		metricsResp, err := client.GetMetrics(ctx, getMetricsReq)
		if err != nil {
			fmt.Printf("[%s] GetMetrics ERROR: %v\n", timestamp, err)
			return err
		}

		// Print results
		fmt.Printf("[%s] %s: IsActive=%v", timestamp, serviceType, isActiveResp.Result)

		if len(metricsResp.MetricValues) > 0 {
			metricValue := metricsResp.MetricValues[0].MetricValueFloat
			targetSize := metricSpecResp.MetricSpecs[0].TargetSizeFloat
			fmt.Printf(", Depth=%.0f, Target=%.0f", metricValue, targetSize)

			if targetSize > 0 {
				scaleRatio := metricValue / targetSize
				fmt.Printf(", Scale=%.2fx", scaleRatio)
			}
		}

		fmt.Println()
	}

	return nil
}
