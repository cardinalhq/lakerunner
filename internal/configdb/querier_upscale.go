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

package configdb

import "context"

type QuerierFull interface {
	Querier
	GetStorageProfile(ctx context.Context, arg GetStorageProfileParams) (GetStorageProfileRow, error)
	GetStorageProfileByCollectorName(ctx context.Context, arg GetStorageProfileByCollectorNameParams) (GetStorageProfileByCollectorNameRow, error)
	GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]GetStorageProfilesByBucketNameRow, error)
}
