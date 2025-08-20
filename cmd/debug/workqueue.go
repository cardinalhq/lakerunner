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

func GetWorkQueueCmd() *cobra.Command {
	workQueueCmd := &cobra.Command{
		Use:   "workqueue",
		Short: "Work queue debugging commands",
	}

	workQueueCmd.AddCommand(getStatusCmd())

	return workQueueCmd
}

func getStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show status of runnable work queue items by signal and action",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(cmd.Context())
		},
	}
}

func runStatus(ctx context.Context) error {
	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.WorkQueueSummary(ctx)
	if err != nil {
		return fmt.Errorf("failed to query work queue summary: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No runnable work queue items found")
		return nil
	}

	colWidths := []int{len("Count"), len("Signal"), len("Action")}

	for _, result := range results {
		countStr := fmt.Sprintf("%d", result.Count)
		signalStr := string(result.Signal)
		actionStr := string(result.Action)

		if len(countStr) > colWidths[0] {
			colWidths[0] = len(countStr)
		}
		if len(signalStr) > colWidths[1] {
			colWidths[1] = len(signalStr)
		}
		if len(actionStr) > colWidths[2] {
			colWidths[2] = len(actionStr)
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

	fmt.Printf("│ %-*s │ %-*s │ %-*s │\n", colWidths[0], "Count", colWidths[1], "Signal", colWidths[2], "Action")

	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	for _, result := range results {
		fmt.Printf("│ %-*d │ %-*s │ %-*s │\n",
			colWidths[0], result.Count,
			colWidths[1], string(result.Signal),
			colWidths[2], string(result.Action))
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
