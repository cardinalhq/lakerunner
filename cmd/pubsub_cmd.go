// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/pubsub"
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
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()
			cmd, err := pubsub.NewHTTPListener()
			if err != nil {
				return fmt.Errorf("failed to create pubsub command: %w", err)
			}
			return cmd.Run(doneCtx)
		},
	}
	cmd.AddCommand(httpListenCmd)

	sqsListenCmd := &cobra.Command{
		Use:   "sqs",
		Short: "listen on one or more SQS pubsub sources",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "pubsub-sqs"
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()
			cmd, err := pubsub.NewSQS()
			if err != nil {
				return fmt.Errorf("failed to create SQS pubsub command: %w", err)
			}
			return cmd.Run(doneCtx)
		},
	}
	cmd.AddCommand(sqsListenCmd)
}
