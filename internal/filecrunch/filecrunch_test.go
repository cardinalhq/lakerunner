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
