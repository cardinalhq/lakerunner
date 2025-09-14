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
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/pubsub"
)

func init() {
	cmd := &cobra.Command{
		Use:   "pubsub",
		Short: "handle pubsub events",
	}

	rootCmd.AddCommand(cmd)

	httpListenCmd := &cobra.Command{
		Use:   "http",
		Short: "listen on one or more http pubsub sources",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "pubsub-http"
			addlAttrs := attribute.NewSet(
				attribute.String("action", "pubsub-http"),
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

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			kafkaFactory, err := fly.NewFactoryFromKafkaConfig(&cfg.Kafka)
			if err != nil {
				return fmt.Errorf("failed to create Kafka factory: %w", err)
			}
			service, err := pubsub.NewHTTPService(doneCtx, cfg, kafkaFactory)
			if err != nil {
				return fmt.Errorf("failed to create HTTP pubsub service: %w", err)
			}

			return service.Run(doneCtx)
		},
	}
	cmd.AddCommand(httpListenCmd)

	sqsListenCmd := &cobra.Command{
		Use:   "sqs",
		Short: "listen on SQS pubsub sources",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "pubsub-sqs"
			addlAttrs := attribute.NewSet(
				attribute.String("action", "pubsub-sqs"),
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

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			kafkaFactory, err := fly.NewFactoryFromKafkaConfig(&cfg.Kafka)
			if err != nil {
				return fmt.Errorf("failed to create Kafka factory: %w", err)
			}
			service, err := pubsub.NewSQSService(doneCtx, cfg, kafkaFactory)
			if err != nil {
				return fmt.Errorf("failed to create SQS pubsub service: %w", err)
			}
			return service.Run(doneCtx)
		},
	}
	cmd.AddCommand(sqsListenCmd)

	gcpListenCmd := &cobra.Command{
		Use:   "gcp",
		Short: "listen on GCP Pub/Sub sources",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "pubsub-gcp"
			addlAttrs := attribute.NewSet(
				attribute.String("action", "pubsub-gcp"),
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

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			kafkaFactory, err := fly.NewFactoryFromKafkaConfig(&cfg.Kafka)
			if err != nil {
				return fmt.Errorf("failed to create Kafka factory: %w", err)
			}
			backend, err := pubsub.NewBackend(doneCtx, cfg, pubsub.BackendTypeGCPPubSub, kafkaFactory)
			if err != nil {
				return fmt.Errorf("failed to create GCP Pub/Sub backend: %w", err)
			}

			return backend.Run(doneCtx)
		},
	}
	cmd.AddCommand(gcpListenCmd)

	azureListenCmd := &cobra.Command{
		Use:   "azure",
		Short: "listen on Azure Queue Storage sources",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "pubsub-azure"
			addlAttrs := attribute.NewSet(
				attribute.String("action", "pubsub-azure"),
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

			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			kafkaFactory, err := fly.NewFactoryFromKafkaConfig(&cfg.Kafka)
			if err != nil {
				return fmt.Errorf("failed to create Kafka factory: %w", err)
			}

			backend, err := pubsub.NewBackend(doneCtx, cfg, pubsub.BackendTypeAzure, kafkaFactory)
			if err != nil {
				return fmt.Errorf("failed to create Azure Queue Storage backend: %w", err)
			}

			return backend.Run(doneCtx)
		},
	}
	cmd.AddCommand(azureListenCmd)
}
