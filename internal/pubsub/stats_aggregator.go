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
	"sort"
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
	processed int64
	failed    int64
	skipped   int64
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
		sa.stats[signal] = &signalStats{}
	}
	sa.stats[signal].processed += int64(count)
}

// RecordFailed records failed processing of items for a signal type
func (sa *StatsAggregator) RecordFailed(signal string, count int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.stats[signal] == nil {
		sa.stats[signal] = &signalStats{}
	}
	sa.stats[signal].failed += int64(count)
}

// RecordSkipped records skipped items for a signal type
func (sa *StatsAggregator) RecordSkipped(signal string, count int) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.stats[signal] == nil {
		sa.stats[signal] = &signalStats{}
	}
	sa.stats[signal].skipped += int64(count)
}

// reportStats reports and resets statistics
func (sa *StatsAggregator) reportStats() {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	// Don't log if there's nothing to report
	if len(sa.stats) == 0 {
		return
	}

	// Calculate totals and collect signals for sorting
	var totalProcessed, totalFailed, totalSkipped int64
	signals := make([]string, 0, len(sa.stats))

	for signal, stats := range sa.stats {
		if stats.processed > 0 || stats.failed > 0 || stats.skipped > 0 {
			totalProcessed += stats.processed
			totalFailed += stats.failed
			totalSkipped += stats.skipped
			signals = append(signals, signal)
		}
	}

	// Only log if there's activity
	if totalProcessed == 0 && totalFailed == 0 && totalSkipped == 0 {
		return
	}

	// Sort signals alphabetically for consistent output
	sort.Strings(signals)

	// Build log attributes with totals first
	attrs := []any{
		slog.Int64("total_processed", totalProcessed),
		slog.Int64("total_failed", totalFailed),
		slog.Int64("total_skipped", totalSkipped),
	}

	// Add signal details in sorted order
	for _, signal := range signals {
		stats := sa.stats[signal]
		attrs = append(attrs,
			slog.Group(signal,
				slog.Int64("processed", stats.processed),
				slog.Int64("failed", stats.failed),
				slog.Int64("skipped", stats.skipped),
			))
	}

	slog.Info("Pubsub processing stats", attrs...)

	// Reset stats
	sa.stats = make(map[string]*signalStats)
}
