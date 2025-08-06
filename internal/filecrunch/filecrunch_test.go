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

package filecrunch

import (
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

func TestSchemaForFile_Success(t *testing.T) {
	// Use a real parquet file from testdata
	filename := filepath.Join("testdata", "logs_1747427310000_667024137.parquet")
	fh, err := LoadSchemaForFile(filename)
	assert.NoError(t, err)
	assert.NotNil(t, fh)
	defer fh.File.Close()
	assert.IsType(t, &parquet.Schema{}, fh.Schema)
}
