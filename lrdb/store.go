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

package lrdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provides all functions to execute db queries and transactions
type Store struct {
	*Queries
	connPool  *pgxpool.Pool
	estimator MetricEstimator
}

// NewStore creates a new Store
func NewStore(connPool *pgxpool.Pool) *Store {
	queries := New(connPool)
	estimator := NewMetricPackEstimator(queries)
	return &Store{
		connPool:  connPool,
		Queries:   queries,
		estimator: estimator,
	}
}

// NewStoreWithEstimator creates a Store with a custom estimator (for testing)
func NewStoreWithEstimator(connPool *pgxpool.Pool, estimator MetricEstimator) *Store {
	return &Store{
		connPool:  connPool,
		Queries:   New(connPool),
		estimator: estimator,
	}
}

func (store *Store) Pool() *pgxpool.Pool {
	return store.connPool
}

// GetMetricEstimate returns the estimated target records for an organization and frequency.
// If no estimate is found, it returns a default value, so this value can be used directly.
func (store *Store) GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64 {
	return store.estimator.Get(ctx, orgID, frequencyMs)
}

// GetLogEstimate returns the estimated target records for an organization for logs.
// If no estimate is found, it returns a default value, so this value can be used directly.
func (store *Store) GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64 {
	if logEstimator, ok := store.estimator.(LogEstimator); ok {
		return logEstimator.GetLog(ctx, orgID)
	}
	return store.estimator.(*PackEstimator).GetLog(ctx, orgID)
}

func (store *Store) GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64 {
	if traceEstimator, ok := store.estimator.(TraceEstimator); ok {
		return traceEstimator.GetTrace(ctx, orgID)
	}
	return store.estimator.(*PackEstimator).GetTrace(ctx, orgID)
}

// Close stops the background goroutines, closes the connection pool, and cleans up resources.
func (store *Store) Close() {
	if estimator, ok := store.estimator.(*MetricPackEstimator); ok {
		estimator.Stop()
	}
	if store.connPool != nil {
		store.connPool.Close()
	}
}

func (store *Store) execTx(ctx context.Context, fn func(*Store) error) (err error) {
	tx, err := store.connPool.Begin(ctx)
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		// Use a timeout to prevent infinite hangs during cleanup.
		// Never use the caller ctx for cleanup as it may be cancelled.
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			if err != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", rbErr))
			} else {
				err = fmt.Errorf("rollback failed: %w", rbErr)
			}
		}
	}()

	txStore := &Store{
		connPool:  store.connPool,
		Queries:   New(tx),
		estimator: store.estimator,
	}

	if err = fn(txStore); err != nil {
		return err
	}

	// Use a timeout for commit to prevent hanging if DB is unresponsive.
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = tx.Commit(commitCtx); err != nil {
		return err
	}
	committed = true
	return nil
}
