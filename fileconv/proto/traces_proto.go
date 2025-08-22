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

// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the GNU Affero General Public License, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR ANY PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package proto

import (
	"fmt"
	"io"
	"os"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/cmd/otel"
	"github.com/cardinalhq/lakerunner/fileconv"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/idgen"
)

type TracesProtoReader struct {
	fname  string
	file   *os.File
	traces *ptrace.Traces
	// Streaming state
	currentResourceIndex int
	spanQueue            []map[string]any
	queueIndex           int
	mapper               *translate.Mapper
	tags                 map[string]string
	translator           *otel.TableTranslator
	idg                  idgen.IDGenerator
}

var _ fileconv.Reader = (*TracesProtoReader)(nil)

func NewTracesProtoReader(fname string, mapper *translate.Mapper, tags map[string]string) (*TracesProtoReader, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fname, err)
	}
	defer file.Close()

	traces, err := parseProtoToOtelTraces(file)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL traces: %w", err)
	}

	return NewTracesProtoReaderFromTraces(traces, mapper, tags)
}

func NewTracesProtoReaderFromTraces(traces *ptrace.Traces, mapper *translate.Mapper, tags map[string]string) (*TracesProtoReader, error) {
	translator := otel.NewTableTranslator()
	idg := idgen.NewULIDGenerator()

	return &TracesProtoReader{
		fname:                "",
		file:                 nil,
		traces:               traces,
		currentResourceIndex: 0,
		spanQueue:            nil,
		queueIndex:           0,
		mapper:               mapper,
		tags:                 tags,
		translator:           translator,
		idg:                  idg,
	}, nil
}

func (r *TracesProtoReader) Close() error {
	r.traces = nil
	return nil
}

func (r *TracesProtoReader) GetRow() (row map[string]any, done bool, err error) {
	if r.traces == nil {
		return nil, true, fmt.Errorf("proto traces are not initialized")
	}

	if r.queueIndex >= len(r.spanQueue) {
		if !r.loadNextResourceSpans() {
			return nil, true, nil // No more resource spans
		}
	}

	span := r.spanQueue[r.queueIndex]

	for k, v := range r.tags {
		span[k] = v
	}

	r.queueIndex++

	return span, false, nil
}

// loadNextResourceSpans loads the next resource spans and populates the queue
func (r *TracesProtoReader) loadNextResourceSpans() bool {
	if r.currentResourceIndex >= r.traces.ResourceSpans().Len() {
		return false
	}

	resourceSpans := r.traces.ResourceSpans().At(r.currentResourceIndex)

	singleResourceSpans := ptrace.NewTraces()
	newResourceSpans := singleResourceSpans.ResourceSpans().AppendEmpty()

	resourceSpans.CopyTo(newResourceSpans)

	convertedSpans, err := r.translator.TracesFromOtel(&singleResourceSpans, nil)
	if err != nil {
		r.currentResourceIndex++
		return r.loadNextResourceSpans()
	}

	r.spanQueue = convertedSpans
	r.queueIndex = 0
	r.currentResourceIndex++

	return true
}

// parseProtoToOtelTraces parses protobuf data into OpenTelemetry traces format
func parseProtoToOtelTraces(file *os.File) (*ptrace.Traces, error) {
	unmarshaler := &ptrace.ProtoUnmarshaler{}

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	traces, err := unmarshaler.UnmarshalTraces(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf traces: %w", err)
	}

	return &traces, nil
}
