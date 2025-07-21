// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package translate

import (
	"strings"
	"time"
)

type Mapper struct {
	TimestampColumns []string
	MessageColumns   []string
	ResourceColumns  []string
	ScopeColumns     []string
	TimeFormat       string
}

type MapperOption func(*Mapper)

func NewMapper(opts ...MapperOption) *Mapper {
	mapper := &Mapper{
		TimestampColumns: []string{
			"timestamp",
			"time",
			"ts",
		},
		MessageColumns: []string{
			"message",
			"msg",
		},
		ResourceColumns: nil,
		ScopeColumns:    nil,
		TimeFormat:      time.RFC3339Nano,
	}
	for _, opt := range opts {
		opt(mapper)
	}
	return mapper
}

// WithTimestampColumn adds a column to the timestamp.
// If not set, defaults to "timestamp", "time", and "ts".
func WithTimestampColumn(name string) MapperOption {
	return func(m *Mapper) {
		m.TimestampColumns = []string{strings.ToLower(name)}
	}
}

// WithMessageColumn adds a column to the message body.
// If not set, defaults to "message" and "msg".
func WithMessageColumn(name string) MapperOption {
	return func(m *Mapper) {
		m.MessageColumns = []string{strings.ToLower(name)}
	}
}

// WithResourceColumn adds a column to the resource attributes.
// If not set, defaults to an empty map.
func WithResourceColumns(names []string) MapperOption {
	return func(m *Mapper) {
		m.ResourceColumns = names
	}
}

// WithScopeColumn adds a column to the scope attributes.
// If not set, defaults to an empty map.
func WithScopeColumn(names []string) MapperOption {
	return func(m *Mapper) {
		m.ScopeColumns = names
	}
}

// WithTimeFormat sets the time format for parsing timestamps.
// See https://pkg.go.dev/time#Parse for supported formats.
// If not set, defaults to time.RFC3339Nano.
func WithTimeFormat(format string) MapperOption {
	return func(m *Mapper) {
		m.TimeFormat = format
	}
}
