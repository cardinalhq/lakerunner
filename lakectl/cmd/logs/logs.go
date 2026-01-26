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

package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/lakectl/cmd/adminclient"
)

// GetLogsCmd provides log-related administrative commands.
func GetLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Log segment administrative commands",
	}

	cmd.AddCommand(getRecompactCmd())
	return cmd
}

func getRecompactCmd() *cobra.Command {
	var (
		orgID                   string
		startDateint            int32
		endDateint              int32
		missingAggFields        bool
		sortVersionBelowCurrent bool
		dryRun                  bool
		jsonOutput              bool
	)

	cmd := &cobra.Command{
		Use:   "recompact",
		Short: "Queue log segments for recompaction",
		Long: `Queue log segments for recompaction based on filter criteria.

Segments that match the filters will be queued for reprocessing through the
compaction pipeline. This regenerates both the tbl_ and agg_ parquet files
with current sorting and aggregation.

At least one filter (--missing-agg-fields or --sort-version-below-current)
must be specified.

Examples:
  # Recompact segments missing agg_fields for a specific date
  lakectl logs recompact --org <org-id> --start-dateint 20250115 --end-dateint 20250115 --missing-agg-fields

  # Recompact segments with outdated sort version for a date range
  lakectl logs recompact --org <org-id> --start-dateint 20250101 --end-dateint 20250115 --sort-version-below-current

  # Preview what would be recompacted (dry run)
  lakectl logs recompact --org <org-id> --start-dateint 20250115 --end-dateint 20250115 --missing-agg-fields --dry-run`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if orgID == "" {
				return fmt.Errorf("--org is required")
			}
			if startDateint == 0 || endDateint == 0 {
				return fmt.Errorf("--start-dateint and --end-dateint are required")
			}
			if !missingAggFields && !sortVersionBelowCurrent {
				return fmt.Errorf("at least one filter required: --missing-agg-fields or --sort-version-below-current")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			client, cleanup, err := adminclient.CreateClient()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx = adminclient.AttachAPIKey(ctx)

			resp, err := client.QueueLogRecompact(ctx, &adminproto.QueueLogRecompactRequest{
				OrganizationId:          orgID,
				StartDateint:            startDateint,
				EndDateint:              endDateint,
				MissingAggFields:        missingAggFields,
				SortVersionBelowCurrent: sortVersionBelowCurrent,
				DryRun:                  dryRun,
			})
			if err != nil {
				return fmt.Errorf("failed to queue recompaction: %w", err)
			}

			if jsonOutput {
				return printRecompactJSON(resp)
			}
			return printRecompactTable(resp, dryRun)
		},
	}

	cmd.Flags().StringVar(&orgID, "org", "", "Organization ID (required)")
	cmd.Flags().Int32Var(&startDateint, "start-dateint", 0, "Start dateint (YYYYMMDD, required)")
	cmd.Flags().Int32Var(&endDateint, "end-dateint", 0, "End dateint (YYYYMMDD, required)")
	cmd.Flags().BoolVar(&missingAggFields, "missing-agg-fields", false, "Filter: segments where agg_fields is NULL")
	cmd.Flags().BoolVar(&sortVersionBelowCurrent, "sort-version-below-current", false, "Filter: segments with outdated sort version")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview matching segments without queueing")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	return cmd
}

func printRecompactTable(resp *adminproto.QueueLogRecompactResponse, dryRun bool) error {
	if len(resp.Segments) == 0 {
		fmt.Println("No segments found matching the filter criteria")
		return nil
	}

	if dryRun {
		fmt.Printf("Found %d segments that would be queued for recompaction:\n\n", len(resp.Segments))
	} else {
		fmt.Printf("Queued %d segments for recompaction:\n\n", resp.SegmentsQueued)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "SEGMENT_ID\tDATEINT\tINSTANCE\tRECORDS\tSORT_VER\tHAS_AGG"); err != nil {
		return err
	}
	for _, seg := range resp.Segments {
		hasAgg := "no"
		if seg.HasAggFields {
			hasAgg = "yes"
		}
		if _, err := fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\t%s\n",
			seg.SegmentId, seg.Dateint, seg.InstanceNum, seg.RecordCount, seg.SortVersion, hasAgg); err != nil {
			return err
		}
	}
	return w.Flush()
}

func printRecompactJSON(resp *adminproto.QueueLogRecompactResponse) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(resp)
}
