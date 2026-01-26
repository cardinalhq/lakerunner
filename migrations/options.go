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

package migrations

import "time"

// CheckMode defines how migration version checking should behave
type CheckMode int

const (
	// CheckModeWait waits for migrations to complete, failing if they don't complete within timeout
	CheckModeWait CheckMode = iota
	// CheckModeWarn logs warnings about version mismatches but continues
	CheckModeWarn
	// CheckModeSkip skips migration checking entirely
	CheckModeSkip
)

// CheckOptions contains options for migration version checking
type CheckOptions struct {
	Mode          CheckMode
	Timeout       time.Duration
	RetryInterval time.Duration
	AllowDirty    bool
}

// CheckOption is a function that modifies CheckOptions
type CheckOption func(*CheckOptions)

// WithCheckMode sets the check mode
func WithCheckMode(mode CheckMode) CheckOption {
	return func(opts *CheckOptions) {
		opts.Mode = mode
	}
}

// WithTimeout sets the timeout for waiting for migrations
func WithTimeout(timeout time.Duration) CheckOption {
	return func(opts *CheckOptions) {
		opts.Timeout = timeout
	}
}

// WithRetryInterval sets the interval between migration checks
func WithRetryInterval(interval time.Duration) CheckOption {
	return func(opts *CheckOptions) {
		opts.RetryInterval = interval
	}
}

// WithAllowDirty allows proceeding even if migrations are in dirty state
func WithAllowDirty(allow bool) CheckOption {
	return func(opts *CheckOptions) {
		opts.AllowDirty = allow
	}
}

// DefaultCheckOptions returns default options for migration checking
func DefaultCheckOptions() CheckOptions {
	return CheckOptions{
		Mode:          CheckModeWait,
		Timeout:       120 * time.Second,
		RetryInterval: 5 * time.Second,
		AllowDirty:    false,
	}
}
