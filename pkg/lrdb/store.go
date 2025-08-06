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
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provides all functions to execute db queries and transactions
type Store struct {
	*Queries
	connPool *pgxpool.Pool
}

// NewStore creates a new Store
func NewStore(connPool *pgxpool.Pool) *Store {
	return &Store{
		connPool: connPool,
		Queries:  New(connPool),
	}
}

func (store *Store) Pool() *pgxpool.Pool {
	return store.connPool
}

func (store *Store) execTx(ctx context.Context, fn func(*Store) error) (err error) {
	closed := false
	tx, err := store.connPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !closed {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				err = fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
			}
		}
	}()

	txStore := &Store{
		connPool: store.connPool,
		Queries:  New(tx),
	}

	if err = fn(txStore); err != nil {
		return
	}

	err = tx.Commit(ctx)
	if err == nil {
		closed = true
	}
	return
}
