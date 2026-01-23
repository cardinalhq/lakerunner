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
	"time"

	"github.com/jellydator/ttlcache/v3"
)

const (
	defaultTTLDuration = 30 * time.Minute
)

var (
	partitionTableCache = ttlcache.New(
		ttlcache.WithTTL[string, struct{}](defaultTTLDuration),
		ttlcache.WithDisableTouchOnHit[string, struct{}](),
		ttlcache.WithCapacity[string, struct{}](1_000_000),
	)
)

func init() {
	go partitionTableCache.Start()
}

func RememberPartitionTable(tableName string) {
	partitionTableCache.Set(tableName, struct{}{}, ttlcache.DefaultTTL)
}

func IsPartitionTableRemembered(tableName string) bool {
	return partitionTableCache.Get(tableName) != nil
}
