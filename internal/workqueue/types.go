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

package workqueue

import (
	"context"
	"time"
)

// Handler defines the common interface for work handlers
type Handler interface {
	CompleteWork(ctx context.Context) error
	RetryWork(ctx context.Context) error
}

// Config holds configuration values for workqueue handlers
type Config struct {
	MaxWorkRetries     int
	WorkFailRequeueTTL time.Duration
	LockTTL            time.Duration
	LockTTLDead        time.Duration
	MyInstanceID       int64
}

// HandlerOption configures a handler
type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	config *Config
}

// WithConfig sets the configuration for handlers
func WithConfig(config *Config) HandlerOption {
	return func(opts *handlerOptions) {
		opts.config = config
	}
}
