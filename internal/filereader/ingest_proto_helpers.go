// Copyright (C) 2025-2026 CardinalHQ, Inc
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

import (
	"bytes"
	"log/slog"
	"sync"

	"go.opentelemetry.io/collector/featuregate"
)

// protoReadPool provides reusable buffers for reading protobuf data.
var protoReadPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func init() {
	// Enable proto pooling for pmetric unmarshalling.
	// This reduces GC pressure by reusing metric structures.
	// Requires calling pref.UnrefMetrics() when done with the data.
	if err := featuregate.GlobalRegistry().Set("pdata.enableRefCounting", true); err != nil {
		slog.Info("Warning: failed to enable pdata.enableRefCounting feature gate, memory usage may be higher", "error", err)
	}
	if err := featuregate.GlobalRegistry().Set("pdata.useProtoPooling", true); err != nil {
		slog.Info("Warning: failed to enable pdata.useProtoPooling feature gate, memory usage may be higher", "error", err)
	}
}
