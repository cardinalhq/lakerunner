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

package logctx

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func TestWithLogger(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	newCtx := WithLogger(ctx, logger)

	retrieved := FromContext(newCtx)
	if retrieved != logger {
		t.Error("expected retrieved logger to match stored logger")
	}
}

func TestFromContext_NoLogger(t *testing.T) {
	ctx := context.Background()

	logger := FromContext(ctx)
	if logger == nil {
		t.Error("expected default logger when none stored in context")
	}
}

func TestFromContext_WithLogger(t *testing.T) {
	ctx := context.Background()
	originalLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx = WithLogger(ctx, originalLogger)
	retrievedLogger := FromContext(ctx)

	if retrievedLogger != originalLogger {
		t.Error("expected retrieved logger to match original logger")
	}
}
