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

package gcpclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithImpersonateServiceAccount(t *testing.T) {
	cfg := &storageConfig{}
	opt := WithImpersonateServiceAccount("test@project.iam.gserviceaccount.com")
	opt(cfg)
	assert.Equal(t, "test@project.iam.gserviceaccount.com", cfg.ServiceAccountEmail)
}

func TestStorageClientKey(t *testing.T) {
	key1 := storageClientKey{ServiceAccountEmail: "test@example.com"}
	key2 := storageClientKey{ServiceAccountEmail: "test@example.com"}
	key3 := storageClientKey{ServiceAccountEmail: "other@example.com"}

	assert.Equal(t, key1, key2)
	assert.NotEqual(t, key1, key3)
}
