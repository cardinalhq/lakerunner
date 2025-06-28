// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lrdb

import (
	"time"

	"github.com/jellydator/ttlcache/v3"
)

const (
	defaultTTLDuration = 30 * time.Minute
)

type flushFunction func()

var flushFunctions []flushFunction

func AddFlushFunction(f flushFunction) {
	flushFunctions = append(flushFunctions, f)
}

func FlushCaches() {
	for _, f := range flushFunctions {
		f()
	}
}

var (
	partitionTableCache = ttlcache.New(
		ttlcache.WithTTL[string, struct{}](defaultTTLDuration),
		ttlcache.WithDisableTouchOnHit[string, struct{}](),
		ttlcache.WithCapacity[string, struct{}](1_000_000),
	)
)

func init() {
	go partitionTableCache.Start()
	AddFlushFunction(partitionTableCache.DeleteAll)
}

func RememberPartitionTable(tableName string) {
	partitionTableCache.Set(tableName, struct{}{}, ttlcache.DefaultTTL)
}

func IsPartitionTableRemembered(tableName string) bool {
	return partitionTableCache.Get(tableName) != nil
}
