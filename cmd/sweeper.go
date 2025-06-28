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

	"github.com/cardinalhq/lakerunner/cmd/sweeper"
)

func init() {
	cmd := &cobra.Command{
		Use:   "sweeper",
		Short: "Do general cleanup tasks",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "lakerunner-sweeper"
			doneCtx, doneFx, err := setupTelemetry(servicename)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			cmd := sweeper.New(myInstanceID, servicename)
			return cmd.Run(doneCtx)
		},
	}

	rootCmd.AddCommand(cmd)
}
