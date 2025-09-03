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

package estimates

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	_ string // apiKey placeholder for future use
)

func GetSummaryCmd() *cobra.Command {
	var (
		local bool
	)

	summaryCmd := &cobra.Command{
		Use:   "summary",
		Short: "Display metric pack estimates summary",
		Long: `Display metric pack estimates summary in a table format.

Shows target record estimates by organization and frequency.
The 'default' row shows global estimates (all-zeros organization ID).`,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runMetricEstimatesSummary(local)
		},
	}

	summaryCmd.Flags().BoolVar(&local, "local", false, "Connect to local database (bypass admin API)")

	return summaryCmd
}

func SetAPIKey(key string) {
	_ = key // apiKey for future use
}

type estimateRow struct {
	orgID   uuid.UUID
	orgName string
	values  map[int32]*int64 // frequency -> target_records
}

func runMetricEstimatesSummary(local bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var store lrdb.StoreFull
	var err error
	if local {
		store, err = dbopen.LRDBStore(ctx)
	} else {
		store, err = dbopen.LRDBStoreForAdmin(ctx)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	// Get all metric pack estimates
	estimates, err := store.GetAllMetricPackEstimates(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metric pack estimates: %w", err)
	}

	if len(estimates) == 0 {
		fmt.Println("No metric pack estimates found.")
		return nil
	}

	// Organize data by organization and collect all frequencies
	rows := make(map[uuid.UUID]*estimateRow)
	frequencies := make(map[int32]bool)

	for _, est := range estimates {
		if _, exists := rows[est.OrganizationID]; !exists {
			orgName := getDisplayName(est.OrganizationID)
			rows[est.OrganizationID] = &estimateRow{
				orgID:   est.OrganizationID,
				orgName: orgName,
				values:  make(map[int32]*int64),
			}
		}
		rows[est.OrganizationID].values[est.FrequencyMs] = est.TargetRecords
		frequencies[est.FrequencyMs] = true
	}

	// Sort frequencies for consistent column ordering
	sortedFreqs := make([]int32, 0, len(frequencies))
	for freq := range frequencies {
		sortedFreqs = append(sortedFreqs, freq)
	}
	sort.Slice(sortedFreqs, func(i, j int) bool {
		return sortedFreqs[i] < sortedFreqs[j]
	})

	// Sort organizations: default first, then by UUID string
	sortedOrgs := make([]*estimateRow, 0, len(rows))
	var defaultRow *estimateRow
	for _, row := range rows {
		if isZeroUUID(row.orgID) {
			defaultRow = row
		} else {
			sortedOrgs = append(sortedOrgs, row)
		}
	}
	sort.Slice(sortedOrgs, func(i, j int) bool {
		return sortedOrgs[i].orgID.String() < sortedOrgs[j].orgID.String()
	})

	// Put default first if it exists
	if defaultRow != nil {
		sortedOrgs = append([]*estimateRow{defaultRow}, sortedOrgs...)
	}

	// Display table
	displayTable(sortedOrgs, sortedFreqs)

	return nil
}

func getDisplayName(orgID uuid.UUID) string {
	if isZeroUUID(orgID) {
		return "default"
	}
	return orgID.String()
}

func isZeroUUID(id uuid.UUID) bool {
	return id == uuid.UUID{}
}

func formatFrequency(freqMs int32) string {
	if freqMs < 1000 {
		return fmt.Sprintf("%dms", freqMs)
	}

	seconds := freqMs / 1000
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}

	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%dm", minutes)
	}

	hours := minutes / 60
	return fmt.Sprintf("%dh", hours)
}

func displayTable(rows []*estimateRow, frequencies []int32) {
	// Calculate column widths
	const orgColWidth = 38 // UUID width + "default"
	const freqColWidth = 10

	// Print header
	fmt.Printf("%-*s", orgColWidth, "Organization")
	for _, freq := range frequencies {
		fmt.Printf(" %*s", freqColWidth, formatFrequency(freq))
	}
	fmt.Println()

	// Print separator line
	fmt.Printf("%s", strings.Repeat("-", orgColWidth))
	for range frequencies {
		fmt.Printf(" %s", strings.Repeat("-", freqColWidth))
	}
	fmt.Println()

	// Print data rows
	for _, row := range rows {
		fmt.Printf("%-*s", orgColWidth, row.orgName)

		for _, freq := range frequencies {
			if val := row.values[freq]; val != nil {
				fmt.Printf(" %*s", freqColWidth, formatNumber(*val))
			} else {
				fmt.Printf(" %*s", freqColWidth, "-")
			}
		}
		fmt.Println()
	}
}

func formatNumber(n int64) string {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1000000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1000000000)
}
