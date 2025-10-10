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

package exemplarreceiver

import "encoding/json"

// ExemplarBatchRequest represents a batch of exemplars from a single source
type ExemplarBatchRequest struct {
	Source    string                   `json:"source"`
	Exemplars []map[string]interface{} `json:"exemplars"`
}

// LogsExemplar represents a single logs exemplar with explicit fingerprinting fields
type LogsExemplar struct {
	ServiceName  string                 `json:"service_name"`
	ClusterName  string                 `json:"cluster_name"`
	Namespace    string                 `json:"namespace"`
	Data         map[string]interface{} `json:"-"` // Everything else
	OriginalData map[string]interface{} `json:"-"` // Original unmarshaled data
}

// MetricsExemplar represents a single metrics exemplar with explicit fingerprinting fields
type MetricsExemplar struct {
	ServiceName  string                 `json:"service_name"`
	ClusterName  string                 `json:"cluster_name"`
	Namespace    string                 `json:"namespace"`
	MetricName   string                 `json:"metric_name"`
	MetricType   string                 `json:"metric_type"`
	Data         map[string]interface{} `json:"-"` // Everything else
	OriginalData map[string]interface{} `json:"-"` // Original unmarshaled data
}

// TracesExemplar represents a single traces exemplar with explicit fingerprinting fields
type TracesExemplar struct {
	ServiceName  string                 `json:"service_name"`
	ClusterName  string                 `json:"cluster_name"`
	Namespace    string                 `json:"namespace"`
	SpanName     string                 `json:"span_name"`
	SpanKind     interface{}            `json:"span_kind"` // Can be int or string
	Data         map[string]interface{} `json:"-"`         // Everything else
	OriginalData map[string]interface{} `json:"-"`         // Original unmarshaled data
}

// UnmarshalJSON implements custom unmarshaling for LogsExemplar
func (e *LogsExemplar) UnmarshalJSON(data []byte) error {
	// First unmarshal into a map to capture everything
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.OriginalData = raw

	// Extract required fields
	if v, ok := raw["service_name"].(string); ok {
		e.ServiceName = v
	}
	if v, ok := raw["cluster_name"].(string); ok {
		e.ClusterName = v
	}
	if v, ok := raw["namespace"].(string); ok {
		e.Namespace = v
	}

	// Store everything (including duplicates) in Data for JSONB storage
	e.Data = raw

	return nil
}

// UnmarshalJSON implements custom unmarshaling for MetricsExemplar
func (e *MetricsExemplar) UnmarshalJSON(data []byte) error {
	// First unmarshal into a map to capture everything
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.OriginalData = raw

	// Extract required fields
	if v, ok := raw["service_name"].(string); ok {
		e.ServiceName = v
	}
	if v, ok := raw["cluster_name"].(string); ok {
		e.ClusterName = v
	}
	if v, ok := raw["namespace"].(string); ok {
		e.Namespace = v
	}
	if v, ok := raw["metric_name"].(string); ok {
		e.MetricName = v
	}
	if v, ok := raw["metric_type"].(string); ok {
		e.MetricType = v
	}

	// Store everything (including duplicates) in Data for JSONB storage
	e.Data = raw

	return nil
}

// UnmarshalJSON implements custom unmarshaling for TracesExemplar
func (e *TracesExemplar) UnmarshalJSON(data []byte) error {
	// First unmarshal into a map to capture everything
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.OriginalData = raw

	// Extract required fields
	if v, ok := raw["service_name"].(string); ok {
		e.ServiceName = v
	}
	if v, ok := raw["cluster_name"].(string); ok {
		e.ClusterName = v
	}
	if v, ok := raw["namespace"].(string); ok {
		e.Namespace = v
	}
	if v, ok := raw["span_name"].(string); ok {
		e.SpanName = v
	}
	// span_kind can be int or string
	if v, ok := raw["span_kind"]; ok {
		e.SpanKind = v
	}

	// Store everything (including duplicates) in Data for JSONB storage
	e.Data = raw

	return nil
}

// ExemplarBatchResponse represents the response from a batch upsert operation
type ExemplarBatchResponse struct {
	Status   string   `json:"status"`
	Accepted int      `json:"accepted"`
	Failed   int      `json:"failed,omitempty"`
	Errors   []string `json:"errors,omitempty"`
}
