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

// Package configservice provides cached, hierarchical organization configuration.
//
// # Storage Model
//
// Configs are stored in PostgreSQL as (organization_id, key) -> JSONB value.
// The nil UUID (00000000-0000-0000-0000-000000000000) represents system-wide defaults.
//
// # Fallback Chain
//
// Lookups follow: org-specific -> system default (nil UUID) -> hardcoded default.
// This enables per-org overrides while maintaining sensible defaults.
//
// # Caching
//
// TTL-based caching with negative caching (ErrNoRows cached to avoid repeated misses).
// Cache invalidated on writes. Use InvalidateCache() for bulk invalidation.
package configservice
