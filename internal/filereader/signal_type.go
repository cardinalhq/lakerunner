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

package filereader

// SignalType represents the type of telemetry signal being processed.
type SignalType int

const (
	// SignalTypeLogs represents log data
	SignalTypeLogs SignalType = iota
	// SignalTypeMetrics represents metric data
	SignalTypeMetrics
	// SignalTypeTraces represents trace data
	SignalTypeTraces
)

// String returns the string representation of the signal type.
func (s SignalType) String() string {
	switch s {
	case SignalTypeLogs:
		return "logs"
	case SignalTypeMetrics:
		return "metrics"
	case SignalTypeTraces:
		return "traces"
	default:
		return "unknown"
	}
}
