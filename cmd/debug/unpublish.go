// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"strconv"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func GetUnpublishCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unpublish <table> <start-dateint> <end-dateint>",
		Short: "Mark segment rows as unpublished for a date range",
		Long: `Mark segment rows as unpublished for a given date range.

This is a debug tool for removing data from query visibility without deleting it.
The data will be cleaned up by the sweeper eventually.

Supported tables: log_seg

Dateints are integers in YYYYMMDD format.

Example:
  lakerunner debug unpublish log_seg 20260504 20260510`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUnpublish(cmd.Context(), args[0], args[1], args[2])
		},
	}

	return cmd
}

func runUnpublish(ctx context.Context, table, startDateintStr, endDateintStr string) error {
	startDateint, err := strconv.ParseInt(startDateintStr, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid start dateint %q: %w", startDateintStr, err)
	}

	endDateint, err := strconv.ParseInt(endDateintStr, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid end dateint %q: %w", endDateintStr, err)
	}

	if endDateint < startDateint {
		return fmt.Errorf("end dateint %d is before start dateint %d", endDateint, startDateint)
	}

	lrStore, err := lrdb.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to open lrdb: %w", err)
	}
	defer lrStore.Close()

	switch table {
	case "log_seg":
		return unpublishLogSeg(ctx, lrStore, int32(startDateint), int32(endDateint))
	default:
		return fmt.Errorf("unsupported table %q (supported: log_seg)", table)
	}
}

func unpublishLogSeg(ctx context.Context, store *lrdb.Store, startDateint, endDateint int32) error {
	rowsAffected, err := store.WipeLogSegsByDateRange(ctx, lrdb.WipeLogSegsByDateRangeParams{
		StartDateint: startDateint,
		EndDateint:   endDateint,
	})
	if err != nil {
		return fmt.Errorf("failed to unpublish log_seg: %w", err)
	}

	fmt.Printf("Marked %d log_seg rows as unpublished (dateint %d to %d)\n", rowsAffected, startDateint, endDateint)
	return nil
}
