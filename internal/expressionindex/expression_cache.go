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

import "sync"

// ExpressionCache is an org-scoped wrapper around QueryIndex.
// It is intended to be shared by ingest and query services.
type ExpressionCache struct {
	mu    sync.RWMutex
	byOrg map[string]*QueryIndex
}

func NewExpressionCache() *ExpressionCache {
	return &ExpressionCache{
		byOrg: make(map[string]*QueryIndex),
	}
}

func (c *ExpressionCache) ReplaceOrg(orgID string, expressions []Expression) error {
	idx := NewQueryIndex()
	if err := idx.Replace(expressions); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.byOrg[orgID] = idx
	return nil
}

func (c *ExpressionCache) Upsert(orgID string, expr Expression) error {
	c.mu.Lock()
	idx := c.byOrg[orgID]
	if idx == nil {
		idx = NewQueryIndex()
		c.byOrg[orgID] = idx
	}
	c.mu.Unlock()

	return idx.Add(expr)
}

func (c *ExpressionCache) Remove(orgID string, exprID string) bool {
	c.mu.RLock()
	idx := c.byOrg[orgID]
	c.mu.RUnlock()
	if idx == nil {
		return false
	}
	return idx.Remove(exprID)
}

func (c *ExpressionCache) FindCandidates(
	orgID string,
	signal Signal,
	metric string,
	tags map[string]string,
) []Expression {
	c.mu.RLock()
	idx := c.byOrg[orgID]
	c.mu.RUnlock()
	if idx == nil {
		return nil
	}
	return idx.FindCandidates(signal, metric, tags)
}

func (c *ExpressionCache) OrgSize(orgID string) int {
	c.mu.RLock()
	idx := c.byOrg[orgID]
	c.mu.RUnlock()
	if idx == nil {
		return 0
	}
	return idx.Size()
}

func (c *ExpressionCache) SignalSize(orgID string, signal Signal) int {
	c.mu.RLock()
	idx := c.byOrg[orgID]
	c.mu.RUnlock()
	if idx == nil {
		return 0
	}
	return idx.SignalSize(signal)
}

func (c *ExpressionCache) HasSignal(orgID string, signal Signal) bool {
	return c.SignalSize(orgID, signal) > 0
}
