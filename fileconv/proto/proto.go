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

package proto

import (
	"fmt"
	"io"
	"os"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cardinalhq/lakerunner/cmd/otel"
	"github.com/cardinalhq/lakerunner/fileconv"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/idgen"
)

type ProtoReader struct {
	fname string
	file  *os.File
	logs  *plog.Logs
	// Streaming state
	currentResourceIndex int
	logQueue             []map[string]any
	queueIndex           int
	mapper               *translate.Mapper
	tags                 map[string]string
	translator           *otel.TableTranslator
	idg                  idgen.IDGenerator
}

var _ fileconv.Reader = (*ProtoReader)(nil)

func NewProtoReader(fname string, mapper *translate.Mapper, tags map[string]string) (*ProtoReader, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fname, err)
	}

	logs, err := parseProtoToOtelLogs(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to parse proto to OTEL logs: %w", err)
	}

	translator := otel.NewTableTranslator()
	idg := idgen.NewULIDGenerator()

	return &ProtoReader{
		fname:                fname,
		file:                 nil, // File is closed after parsing
		logs:                 logs,
		currentResourceIndex: 0,
		logQueue:             nil,
		queueIndex:           0,
		mapper:               mapper,
		tags:                 tags,
		translator:           translator,
		idg:                  idg,
	}, nil
}

func (r *ProtoReader) Close() error {
	r.logs = nil
	return nil
}

func (r *ProtoReader) GetRow() (row map[string]any, done bool, err error) {
	if r.logs == nil {
		return nil, true, fmt.Errorf("proto logs are not initialized")
	}

	if r.queueIndex >= len(r.logQueue) {
		if !r.loadNextResourceLog() {
			return nil, true, nil // No more resource logs
		}
	}

	log := r.logQueue[r.queueIndex]

	for k, v := range r.tags {
		log[k] = v
	}

	r.queueIndex++

	return log, false, nil
}

// loadNextResourceLog loads the next resource log and populates the queue
func (r *ProtoReader) loadNextResourceLog() bool {
	if r.currentResourceIndex >= r.logs.ResourceLogs().Len() {
		return false
	}

	resourceLog := r.logs.ResourceLogs().At(r.currentResourceIndex)

	singleResourceLog := plog.NewLogs()
	newResourceLog := singleResourceLog.ResourceLogs().AppendEmpty()

	resourceLog.CopyTo(newResourceLog)

	convertedLogs, err := r.translator.LogsFromOtel(&singleResourceLog, nil)
	if err != nil {
		r.currentResourceIndex++
		return r.loadNextResourceLog()
	}

	r.logQueue = convertedLogs
	r.queueIndex = 0
	r.currentResourceIndex++

	return true
}

// parseProtoToOtelLogs parses protobuf data into OpenTelemetry logs format
func parseProtoToOtelLogs(file *os.File) (*plog.Logs, error) {
	unmarshaler := &plog.ProtoUnmarshaler{}

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	logs, err := unmarshaler.UnmarshalLogs(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf logs: %w", err)
	}

	return &logs, nil
}
