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
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
)

// GetObjCleanupCmd returns the object cleanup debug command.
func GetObjCleanupCmd() *cobra.Command {
	objCleanupCmd := &cobra.Command{
		Use:   "objcleanup",
		Short: "Object cleanup debugging commands",
	}

	objCleanupCmd.AddCommand(getBucketStatusCmd())

	return objCleanupCmd
}

func getBucketStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show object cleanup counts by bucket",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBucketStatus(cmd.Context())
		},
	}
}

func runBucketStatus(ctx context.Context) error {
	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	cleanup, err := store.ObjectCleanupBucketSummary(ctx)
	if err != nil {
		return fmt.Errorf("failed to query object cleanup summary: %w", err)
	}

	type bucketInfo struct {
		cleanupPending    int64
		cleanupNotPending int64
	}

	buckets := map[string]*bucketInfo{}

	for _, r := range cleanup {
		b := r.BucketID
		bi, ok := buckets[b]
		if !ok {
			bi = &bucketInfo{}
			buckets[b] = bi
		}
		bi.cleanupPending = r.Pending
		bi.cleanupNotPending = r.NotPending
	}

	if len(buckets) == 0 {
		fmt.Println("No objects found")
		return nil
	}

	type record struct {
		bucket            string
		cleanupPending    int64
		cleanupNotPending int64
	}

	records := make([]record, 0, len(buckets))
	for b, bi := range buckets {
		records = append(records, record{
			bucket:            b,
			cleanupPending:    bi.cleanupPending,
			cleanupNotPending: bi.cleanupNotPending,
		})
	}

	sort.Slice(records, func(i, j int) bool { return records[i].bucket < records[j].bucket })

	headers := []string{"Bucket", "Cleanup Pending", "Cleanup Not Pending"}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}

	for _, r := range records {
		if len(r.bucket) > widths[0] {
			widths[0] = len(r.bucket)
		}
		if l := len(fmt.Sprintf("%d", r.cleanupPending)); l > widths[1] {
			widths[1] = l
		}
		if l := len(fmt.Sprintf("%d", r.cleanupNotPending)); l > widths[2] {
			widths[2] = l
		}
	}

	fmt.Print("┌")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │ %-*s │\n",
		widths[0], headers[0],
		widths[1], headers[1],
		widths[2], headers[2],
	)

	fmt.Print("├")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┤")

	for _, r := range records {
		fmt.Printf("│ %-*s │ %-*d │ %-*d │\n",
			widths[0], r.bucket,
			widths[1], r.cleanupPending,
			widths[2], r.cleanupNotPending,
		)
	}

	fmt.Print("└")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┘")

	return nil
}
