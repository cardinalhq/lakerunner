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

	"github.com/cardinalhq/lakerunner/queryworker"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
)

func init() {
	cmd := &cobra.Command{
		Use:   "query-worker",
		Short: "start query-worker service",
		RunE: func(_ *cobra.Command, _ []string) error {
			servicename := "query-worker"
			addlAttrs := attribute.NewSet()
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			service, err := queryworker.NewService()
			if err != nil {
				slog.Error("Failed to create query worker service", slog.Any("error", err))
				return fmt.Errorf("failed to create query worker service: %w", err)
			}

			return service.Run(doneCtx)
		},
	}

	rootCmd.AddCommand(cmd)
}