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

package expressionindex

import (
	"sync"
	"time"
)

var (
	globalCatalogRefresher     *CatalogRefresher
	globalCatalogRefresherOnce sync.Once
)

// NewGlobalCatalogRefresher initializes a global catalog refresher singleton.
func NewGlobalCatalogRefresher(refreshInterval time.Duration) {
	globalCatalogRefresherOnce.Do(func() {
		globalCatalogRefresher = NewCatalogRefresher(
			NewExpressionCache(),
			refreshInterval,
			nil,
		)
	})
}

// GlobalCatalogRefresher returns the global refresher.
// Panics when NewGlobalCatalogRefresher has not been called.
func GlobalCatalogRefresher() *CatalogRefresher {
	if globalCatalogRefresher == nil {
		panic("expressionindex: NewGlobalCatalogRefresher must be called before GlobalCatalogRefresher")
	}
	return globalCatalogRefresher
}

// MaybeGlobalCatalogRefresher returns the global refresher when initialized.
func MaybeGlobalCatalogRefresher() *CatalogRefresher {
	return globalCatalogRefresher
}
