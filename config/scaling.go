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

	// Service-specific scaling configurations
	IngestLogs    ServiceScaling `mapstructure:"ingest_logs" yaml:"ingest_logs"`
	IngestMetrics ServiceScaling `mapstructure:"ingest_metrics" yaml:"ingest_metrics"`
	IngestTraces  ServiceScaling `mapstructure:"ingest_traces" yaml:"ingest_traces"`

	CompactLogs    ServiceScaling `mapstructure:"compact_logs" yaml:"compact_logs"`
	CompactMetrics ServiceScaling `mapstructure:"compact_metrics" yaml:"compact_metrics"`
	CompactTraces  ServiceScaling `mapstructure:"compact_traces" yaml:"compact_traces"`

	RollupMetrics ServiceScaling `mapstructure:"rollup_metrics" yaml:"rollup_metrics"`

	// Boxer services (batch processing)
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

		// Boxer and ingest behave in a similar way, and will often have what seem to be
		// consumer lag where it is just duplicate avoidance and delayed checkpointing to
		// prevent losing data.
		BoxerCompactLogs:    ServiceScaling{TargetQueueSize: 1500},
		BoxerCompactMetrics: ServiceScaling{TargetQueueSize: 1500},
		BoxerCompactTraces:  ServiceScaling{TargetQueueSize: 1500},
		BoxerRollupMetrics:  ServiceScaling{TargetQueueSize: 1500},
		IngestLogs:          ServiceScaling{TargetQueueSize: 500},
		IngestMetrics:       ServiceScaling{TargetQueueSize: 500},
		IngestTraces:        ServiceScaling{TargetQueueSize: 500},

		// Compact and rollups are single-work-unit queues.
		CompactLogs:    ServiceScaling{TargetQueueSize: 100},
		CompactMetrics: ServiceScaling{TargetQueueSize: 100},
		CompactTraces:  ServiceScaling{TargetQueueSize: 100},
		RollupMetrics:  ServiceScaling{TargetQueueSize: 100},
	}
}

// GetServiceScaling returns the scaling configuration for a specific service type
func (s *ScalingConfig) GetServiceScaling(serviceType string) (ServiceScaling, error) {
	switch serviceType {
	case ServiceTypeIngestLogs:
		return s.IngestLogs, nil
	case ServiceTypeIngestMetrics:
		return s.IngestMetrics, nil
	case ServiceTypeIngestTraces:
		return s.IngestTraces, nil
	case ServiceTypeCompactLogs:
		return s.CompactLogs, nil
	case ServiceTypeCompactMetrics:
		return s.CompactMetrics, nil
	case ServiceTypeCompactTraces:
		return s.CompactTraces, nil
	case ServiceTypeRollupMetrics:
		return s.RollupMetrics, nil
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
