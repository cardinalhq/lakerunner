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

package cmd

import (
	"errors"
	"fmt"
)

// WorkerInterruptedError indicates that a worker was interrupted during processing
// at a safe point and should exit cleanly without retrying.
type WorkerInterruptedError struct {
	Reason string
}

func (e WorkerInterruptedError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("worker interrupted: %s", e.Reason)
	}
	return "worker interrupted"
}

// WorkerRetryableError indicates that a worker encountered an error that can be retried.
type WorkerRetryableError struct {
	Err error
}

func (e WorkerRetryableError) Error() string {
	return fmt.Sprintf("retryable error: %v", e.Err)
}

func (e WorkerRetryableError) Unwrap() error {
	return e.Err
}

// WorkerFatalError indicates that a worker encountered an error that should not be retried.
type WorkerFatalError struct {
	Err error
}

func (e WorkerFatalError) Error() string {
	return fmt.Sprintf("fatal error: %v", e.Err)
}

func (e WorkerFatalError) Unwrap() error {
	return e.Err
}

// Helper functions for creating and checking error types

// NewWorkerInterrupted creates a new WorkerInterruptedError
func NewWorkerInterrupted(reason string) error {
	return WorkerInterruptedError{Reason: reason}
}

// NewWorkerRetryable wraps an error as retryable
func NewWorkerRetryable(err error) error {
	if err == nil {
		return nil
	}
	return WorkerRetryableError{Err: err}
}

// NewWorkerFatal wraps an error as fatal (non-retryable)
func NewWorkerFatal(err error) error {
	if err == nil {
		return nil
	}
	return WorkerFatalError{Err: err}
}

// IsWorkerInterrupted checks if an error is a WorkerInterruptedError
func IsWorkerInterrupted(err error) bool {
	var workerErr WorkerInterruptedError
	return errors.As(err, &workerErr)
}

// IsWorkerRetryable checks if an error is retryable
func IsWorkerRetryable(err error) bool {
	var workerErr WorkerRetryableError
	return errors.As(err, &workerErr)
}

// IsWorkerFatal checks if an error is fatal (non-retryable)
func IsWorkerFatal(err error) bool {
	var workerErr WorkerFatalError
	return errors.As(err, &workerErr)
}
