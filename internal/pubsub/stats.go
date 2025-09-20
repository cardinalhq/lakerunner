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

package pubsub

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// StatsAggregator collects and periodically reports pubsub processing statistics
type StatsAggregator struct {
	mu       sync.Mutex
	stats    map[string]*signalStats
	interval time.Duration
	done     chan struct{}
	wg       sync.WaitGroup
}

type signalStats struct {
	processed      int64
	failed         int64
	skipped        int64
	fileTypeCounts map[string]int64 // extension -> count
}

// NewStatsAggregator creates a new stats aggregator with the specified reporting interval
func NewStatsAggregator(interval time.Duration) *StatsAggregator {
	return &StatsAggregator{
		stats:    make(map[string]*signalStats),
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start begins periodic reporting
func (sa *StatsAggregator) Start(ctx context.Context) {
	sa.wg.Add(1)
	go func() {
		defer sa.wg.Done()
		ticker := time.NewTicker(sa.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				sa.reportStats()
				return
			case <-sa.done:
				sa.reportStats()
				return
			case <-ticker.C:
				sa.reportStats()
			}
		}
	}()
}

// Stop stops the aggregator and reports final stats
func (sa *StatsAggregator) Stop() {
	close(sa.done)
	sa.wg.Wait()
}

// RecordProcessed records successful processing of items for a signal type
func (sa *StatsAggregator) RecordProcessed(signal string, count int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.stats[signal] == nil {
		sa.stats[signal] = &signalStats{
			fileTypeCounts: make(map[string]int64),
		}
	}
	sa.stats[signal].processed += int64(count)
}

// RecordFailed records failed processing of items for a signal type
func (sa *StatsAggregator) RecordFailed(signal string, count int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.stats[signal] == nil {
		sa.stats[signal] = &signalStats{
			fileTypeCounts: make(map[string]int64),
		}
	}
	sa.stats[signal].failed += int64(count)
}

// RecordSkipped records skipped items for a signal type
func (sa *StatsAggregator) RecordSkipped(signal string, count int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.stats[signal] == nil {
		sa.stats[signal] = &signalStats{
			fileTypeCounts: make(map[string]int64),
		}
	}
	sa.stats[signal].skipped += int64(count)
}

// RecordFileTypes records file type counts for a signal type
func (sa *StatsAggregator) RecordFileTypes(signal string, fileTypeCounts map[string]int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.stats[signal] == nil {
		sa.stats[signal] = &signalStats{
			fileTypeCounts: make(map[string]int64),
		}
	}

	for ext, count := range fileTypeCounts {
		sa.stats[signal].fileTypeCounts[ext] += int64(count)
	}
}

// reportStats reports and resets statistics
func (sa *StatsAggregator) reportStats() {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	// Don't log if there's nothing to report
	if len(sa.stats) == 0 {
		return
	}

	// Calculate totals and collect signals in consistent order
	var totalProcessed, totalFailed, totalSkipped int64
	signalDetails := make([]any, 0)

	// Define consistent order for signals
	signalOrder := []string{"logs", "metrics", "traces"}

	// First process known signals in order
	for _, signal := range signalOrder {
		if stats, exists := sa.stats[signal]; exists && (stats.processed > 0 || stats.failed > 0 || stats.skipped > 0) {
			totalProcessed += stats.processed
			totalFailed += stats.failed
			totalSkipped += stats.skipped

			attrs := []any{
				slog.Int64("processed", stats.processed),
				slog.Int64("failed", stats.failed),
				slog.Int64("skipped", stats.skipped),
			}

			// Add file type counts if present
			if len(stats.fileTypeCounts) > 0 {
				fileTypes := []any{
					slog.Int64("json", stats.fileTypeCounts["json"]),
					slog.Int64("json.gz", stats.fileTypeCounts["json.gz"]),
					slog.Int64("binpb", stats.fileTypeCounts["binpb"]),
					slog.Int64("binpb.gz", stats.fileTypeCounts["binpb.gz"]),
					slog.Int64("parquet", stats.fileTypeCounts["parquet"]),
					slog.Int64("other", stats.fileTypeCounts["other"]),
				}
				attrs = append(attrs, slog.Group("file_types", fileTypes...))
			}

			signalDetails = append(signalDetails,
				slog.Group(signal, attrs...))
		}
	}

	// Then process any unknown signals (shouldn't happen but defensive)
	for signal, stats := range sa.stats {
		// Skip if already processed
		isKnown := false
		for _, known := range signalOrder {
			if signal == known {
				isKnown = true
				break
			}
		}
		if !isKnown && (stats.processed > 0 || stats.failed > 0 || stats.skipped > 0) {
			totalProcessed += stats.processed
			totalFailed += stats.failed
			totalSkipped += stats.skipped

			attrs := []any{
				slog.Int64("processed", stats.processed),
				slog.Int64("failed", stats.failed),
				slog.Int64("skipped", stats.skipped),
			}

			// Add file type counts if present
			if len(stats.fileTypeCounts) > 0 {
				fileTypes := []any{
					slog.Int64("json", stats.fileTypeCounts["json"]),
					slog.Int64("json.gz", stats.fileTypeCounts["json.gz"]),
					slog.Int64("binpb", stats.fileTypeCounts["binpb"]),
					slog.Int64("binpb.gz", stats.fileTypeCounts["binpb.gz"]),
					slog.Int64("parquet", stats.fileTypeCounts["parquet"]),
					slog.Int64("other", stats.fileTypeCounts["other"]),
				}
				attrs = append(attrs, slog.Group("file_types", fileTypes...))
			}

			signalDetails = append(signalDetails,
				slog.Group(signal, attrs...))
		}
	}

	// Only log if there's activity
	if totalProcessed > 0 || totalFailed > 0 || totalSkipped > 0 {
		attrs := []any{
			slog.Int64("total_processed", totalProcessed),
			slog.Int64("total_failed", totalFailed),
			slog.Int64("total_skipped", totalSkipped),
		}
		attrs = append(attrs, signalDetails...)

		slog.Info("Pubsub processing stats", attrs...)
	}

	// Reset stats
	sa.stats = make(map[string]*signalStats)
}
