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
	"context"
	"fmt"
	"log/slog"
	"strings"

	bigrun "github.com/cardinalhq/lakerunner/bigquery"

	"github.com/spf13/cobra"
)

var (
	crDatasets string
	crTopN     int
)

func init() {
	cmd := &cobra.Command{
		Use:   "gen-catalog",
		Short: "Generate a query catalog from BigQuery datasets and print candidates",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			// Parse comma-separated datasets
			var datasets []string
			if s := strings.TrimSpace(crDatasets); s != "" {
				parts := strings.Split(s, ",")
				datasets = make([]string, 0, len(parts))
				for _, p := range parts {
					if v := strings.TrimSpace(p); v != "" {
						datasets = append(datasets, v)
					}
				}
			}

			cands, onto, g, err := bigrun.RunAll(ctx, datasets, crTopN)
			if err != nil {
				return err
			}

			fmt.Printf("facts=%d dims=%d nodes=%d edges=%d\n", len(onto.Facts), len(onto.Dimensions), len(g.Nodes), len(g.Edges))

			recs := bigrun.Records(cands)
			for i, r := range recs {
				fmt.Printf("%2d) %.3f  %s  [bytes≈%d  $≈%.2f]  fact=%s  dims=%v  grain=%s  stage=%s\n",
					i+1, r.Composite, r.Title, r.EstimatedBytes, r.EstimatedUSD, r.FactTable, r.Dimensions, r.Grain, r.Stage)
				fmt.Printf("-- %s\n%s\n\n", r.Description, r.SQL)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&crDatasets, "datasets", "", "Comma-separated dataset IDs (e.g., \"sales,ops\")")
	cmd.Flags().IntVar(&crTopN, "top", 50, "Max number of candidates to print")

	if err := cmd.MarkFlagRequired("datasets"); err != nil {
		slog.Warn("could not mark --datasets as required", "error", err)
	}

	rootCmd.AddCommand(cmd)
}
