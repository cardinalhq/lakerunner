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
	"log/slog"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// Handler defines the common interface for work handlers
type Handler interface {
	CompleteWork()
	RetryWork()
}

// Store defines the database operations needed by handlers
type Store interface {
	// WorkQueue operations
	WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error
	WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) error
	
	// Inqueue operations
	ReleaseInqueueWork(ctx context.Context, arg lrdb.ReleaseInqueueWorkParams) error
	DeleteInqueueWork(ctx context.Context, arg lrdb.DeleteInqueueWorkParams) error
	InqueueJournalDelete(ctx context.Context, arg lrdb.InqueueJournalDeleteParams) error
	InqueueJournalUpsert(ctx context.Context, arg lrdb.InqueueJournalUpsertParams) (bool, error)
}

// Config holds configuration values for workqueue handlers
type Config struct {
	MaxWorkRetries int
	MyInstanceID   int64
}

// HandlerOption configures a handler
type HandlerOption func(*handlerOptions)

type handlerOptions struct {
	logger *slog.Logger
}

// WithLogger sets the logger for handlers
func WithLogger(logger *slog.Logger) HandlerOption {
	return func(opts *handlerOptions) {
		opts.logger = logger
	}
}