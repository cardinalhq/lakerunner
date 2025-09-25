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

package config

const (
	TargetFileSize = int64(2 * 1024 * 1024) // 2MB

	// Service types for ingestion
	ServiceTypeIngestLogs    = "ingest-logs"
	ServiceTypeIngestMetrics = "ingest-metrics"
	ServiceTypeIngestTraces  = "ingest-traces"

	// Service types for segments processing (worker topics)
	ServiceTypeSegmentsLogsIngest    = "segments-logs-ingest"
	ServiceTypeSegmentsMetricsIngest = "segments-metrics-ingest"
	ServiceTypeSegmentsTracesIngest  = "segments-traces-ingest"

	// Service types for compaction
	ServiceTypeCompactLogs    = "compact-logs"
	ServiceTypeCompactMetrics = "compact-metrics"
	ServiceTypeCompactTraces  = "compact-traces"

	// Service types for rollup
	ServiceTypeRollupMetrics = "rollup-metrics"

	// Service types for boxer tasks
	ServiceTypeBoxerIngestLogs     = "boxer-ingest-logs"
	ServiceTypeBoxerIngestMetrics  = "boxer-ingest-metrics"
	ServiceTypeBoxerIngestTraces   = "boxer-ingest-traces"
	ServiceTypeBoxerCompactLogs    = "boxer-compact-logs"
	ServiceTypeBoxerCompactMetrics = "boxer-compact-metrics"
	ServiceTypeBoxerCompactTraces  = "boxer-compact-traces"
	ServiceTypeBoxerRollupMetrics  = "boxer-rollup-metrics"

	// Boxer service type for KEDA scaling
	ServiceTypeBoxer = "boxer"

	// Task names (used in boxer command flags and KEDA boxerTasks)
	TaskIngestLogs     = "ingest-logs"
	TaskIngestMetrics  = "ingest-metrics"
	TaskIngestTraces   = "ingest-traces"
	TaskCompactLogs    = "compact-logs"
	TaskCompactMetrics = "compact-metrics"
	TaskCompactTraces  = "compact-traces"
	TaskRollupMetrics  = "rollup-metrics"
)

// GetBoxerServiceType returns the service type for a boxer task
func GetBoxerServiceType(task string) string {
	switch task {
	case TaskIngestLogs:
		return ServiceTypeBoxerIngestLogs
	case TaskIngestMetrics:
		return ServiceTypeBoxerIngestMetrics
	case TaskIngestTraces:
		return ServiceTypeBoxerIngestTraces
	case TaskCompactLogs:
		return ServiceTypeBoxerCompactLogs
	case TaskCompactMetrics:
		return ServiceTypeBoxerCompactMetrics
	case TaskCompactTraces:
		return ServiceTypeBoxerCompactTraces
	case TaskRollupMetrics:
		return ServiceTypeBoxerRollupMetrics
	default:
		return "boxer-" + task // fallback for unknown tasks
	}
}
