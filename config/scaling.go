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

import (
	"fmt"
)

type ScalingConfig struct {
	// Default target queue size for services not explicitly configured
	DefaultTarget int `mapstructure:"default_target" yaml:"default_target"`

	// Service-specific scaling configurations for workers
	WorkerIngestLogs     ServiceScaling `mapstructure:"worker_ingest_logs" yaml:"worker_ingest_logs"`
	WorkerIngestMetrics  ServiceScaling `mapstructure:"worker_ingest_metrics" yaml:"worker_ingest_metrics"`
	WorkerIngestTraces   ServiceScaling `mapstructure:"worker_ingest_traces" yaml:"worker_ingest_traces"`
	WorkerCompactLogs    ServiceScaling `mapstructure:"worker_compact_logs" yaml:"worker_compact_logs"`
	WorkerCompactMetrics ServiceScaling `mapstructure:"worker_compact_metrics" yaml:"worker_compact_metrics"`
	WorkerCompactTraces  ServiceScaling `mapstructure:"worker_compact_traces" yaml:"worker_compact_traces"`
	WorkerRollupMetrics  ServiceScaling `mapstructure:"worker_rollup_metrics" yaml:"worker_rollup_metrics"`

	// Boxer services (batch processing)
	BoxerIngestLogs     ServiceScaling `mapstructure:"boxer_ingest_logs" yaml:"boxer_ingest_logs"`
	BoxerIngestMetrics  ServiceScaling `mapstructure:"boxer_ingest_metrics" yaml:"boxer_ingest_metrics"`
	BoxerIngestTraces   ServiceScaling `mapstructure:"boxer_ingest_traces" yaml:"boxer_ingest_traces"`
	BoxerCompactLogs    ServiceScaling `mapstructure:"boxer_compact_logs" yaml:"boxer_compact_logs"`
	BoxerCompactMetrics ServiceScaling `mapstructure:"boxer_compact_metrics" yaml:"boxer_compact_metrics"`
	BoxerCompactTraces  ServiceScaling `mapstructure:"boxer_compact_traces" yaml:"boxer_compact_traces"`
	BoxerRollupMetrics  ServiceScaling `mapstructure:"boxer_rollup_metrics" yaml:"boxer_rollup_metrics"`
}

// ServiceScaling defines scaling parameters for a specific service
type ServiceScaling struct {
	// TargetQueueSize is the target queue depth for scaling decisions
	// KEDA will try to maintain this queue depth by scaling replicas
	TargetQueueSize int `mapstructure:"target_queue_size" yaml:"target_queue_size"`
}

// GetDefaultScalingConfig returns the default scaling configuration
func GetDefaultScalingConfig() ScalingConfig {
	return ScalingConfig{
		DefaultTarget: 100,

		// Boxers delay the ACK until all boxes are emitted that a partition has
		// added a message to.  This means the numbers will get larger, but this
		// is normal.  CPU is likely a better scaling here...
		BoxerIngestLogs:     ServiceScaling{TargetQueueSize: 1500},
		BoxerIngestMetrics:  ServiceScaling{TargetQueueSize: 1500},
		BoxerIngestTraces:   ServiceScaling{TargetQueueSize: 1500},
		BoxerCompactLogs:    ServiceScaling{TargetQueueSize: 1500},
		BoxerCompactMetrics: ServiceScaling{TargetQueueSize: 1500},
		BoxerCompactTraces:  ServiceScaling{TargetQueueSize: 1500},
		BoxerRollupMetrics:  ServiceScaling{TargetQueueSize: 1500},

		// Workers should not fall behind as they are a single-work-item processor.
		WorkerIngestLogs:     ServiceScaling{TargetQueueSize: 50},
		WorkerIngestMetrics:  ServiceScaling{TargetQueueSize: 50},
		WorkerIngestTraces:   ServiceScaling{TargetQueueSize: 50},
		WorkerCompactLogs:    ServiceScaling{TargetQueueSize: 50},
		WorkerCompactMetrics: ServiceScaling{TargetQueueSize: 50},
		WorkerCompactTraces:  ServiceScaling{TargetQueueSize: 50},
		WorkerRollupMetrics:  ServiceScaling{TargetQueueSize: 50},
	}
}

// GetServiceScaling returns the scaling configuration for a specific service type
func (s *ScalingConfig) GetServiceScaling(serviceType string) (ServiceScaling, error) {
	switch serviceType {
	case ServiceTypeWorkerIngestLogs:
		return s.WorkerIngestLogs, nil
	case ServiceTypeWorkerIngestMetrics:
		return s.WorkerIngestMetrics, nil
	case ServiceTypeWorkerIngestTraces:
		return s.WorkerIngestTraces, nil
	case ServiceTypeWorkerCompactLogs:
		return s.WorkerCompactLogs, nil
	case ServiceTypeWorkerCompactMetrics:
		return s.WorkerCompactMetrics, nil
	case ServiceTypeWorkerCompactTraces:
		return s.WorkerCompactTraces, nil
	case ServiceTypeWorkerRollupMetrics:
		return s.WorkerRollupMetrics, nil
	case ServiceTypeBoxerIngestLogs:
		return s.BoxerIngestLogs, nil
	case ServiceTypeBoxerIngestMetrics:
		return s.BoxerIngestMetrics, nil
	case ServiceTypeBoxerIngestTraces:
		return s.BoxerIngestTraces, nil
	case ServiceTypeBoxerCompactLogs:
		return s.BoxerCompactLogs, nil
	case ServiceTypeBoxerCompactMetrics:
		return s.BoxerCompactMetrics, nil
	case ServiceTypeBoxerCompactTraces:
		return s.BoxerCompactTraces, nil
	case ServiceTypeBoxerRollupMetrics:
		return s.BoxerRollupMetrics, nil
	default:
		return ServiceScaling{}, fmt.Errorf("unknown service type: %s", serviceType)
	}
}

// GetTargetQueueSize returns the target queue size for a service type
func (s *ScalingConfig) GetTargetQueueSize(serviceType string) (int, error) {
	scaling, err := s.GetServiceScaling(serviceType)
	if err != nil {
		return 0, err
	}
	if scaling.TargetQueueSize <= 0 {
		return 0, fmt.Errorf("invalid target queue size for service %s: %d", serviceType, scaling.TargetQueueSize)
	}
	return scaling.TargetQueueSize, nil
}
