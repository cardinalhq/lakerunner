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

package s3helper

import "github.com/cardinalhq/lakerunner/internal/idgen"

var flake *idgen.SonyFlakeGenerator

func init() {
	var err error
	flake, err = idgen.NewFlakeGenerator()
	if err != nil {
		panic(err)
	}
}

func GenerateID() int64 {
	if flake == nil {
		panic("flake generator is not initialized")
	}
	id := flake.NextID()
	return id
}
