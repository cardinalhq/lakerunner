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
