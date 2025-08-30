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
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
)

func GetInqueueCmd() *cobra.Command {
	inqueueCmdx := &cobra.Command{
		Use:   "inqueue",
		Short: "Inqueue debugging commands",
	}

	inqueueCmdx.AddCommand(getInqueueStatusCmd())

	return inqueueCmdx
}

func getInqueueStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show status of unclaimed inqueue items by telemetry type",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInqueueStatus(cmd.Context())
		},
	}
}

func runInqueueStatus(ctx context.Context) error {
	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.InqueueSummary(ctx)
	if err != nil {
		return fmt.Errorf("failed to query inqueue summary: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No unclaimed inqueue items found")
		return nil
	}

	colWidths := []int{len("Count"), len("Telemetry Type")}

	for _, result := range results {
		countStr := fmt.Sprintf("%d", result.Count)
		telemetryTypeStr := string(result.Signal)

		if len(countStr) > colWidths[0] {
			colWidths[0] = len(countStr)
		}
		if len(telemetryTypeStr) > colWidths[1] {
			colWidths[1] = len(telemetryTypeStr)
		}
	}

	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │\n", colWidths[0], "Count", colWidths[1], "Telemetry Type")

	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	for _, result := range results {
		fmt.Printf("│ %-*d │ %-*s │\n",
			colWidths[0], result.Count,
			colWidths[1], string(result.Signal))
	}

	fmt.Print("└")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┘")

	return nil
}
