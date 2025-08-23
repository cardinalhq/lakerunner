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

package spillers

import (
	"io"
	"testing"
)

func TestGobSpiller_Basic(t *testing.T) {
	tmpdir := t.TempDir()
	spiller := NewGobSpiller()
	
	// Test data
	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
		{"id": int64(2), "name": "bob", "score": 87.2},
		{"id": int64(3), "name": "charlie", "score": 92.1},
	}
	
	keyFunc := func(row map[string]any) any {
		return row["id"].(int64)
	}
	
	// Write spill file
	spillFile, err := spiller.WriteSpillFile(tmpdir, rows, keyFunc)
	if err != nil {
		t.Fatalf("WriteSpillFile() error = %v", err)
	}
	
	if spillFile.RowCount != 3 {
		t.Errorf("Expected RowCount = 3, got %d", spillFile.RowCount)
	}
	
	// Read back from spill file
	reader, err := spiller.OpenSpillFile(spillFile, keyFunc)
	if err != nil {
		t.Fatalf("OpenSpillFile() error = %v", err)
	}
	defer reader.Close()
	
	// Read all rows
	var readRows []map[string]any
	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reader.Next() error = %v", err)
		}
		readRows = append(readRows, row)
	}
	
	// Verify we got all rows back
	if len(readRows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(readRows))
	}
	
	// Verify content (order should be preserved from write)
	for i, row := range readRows {
		expectedID := int64(i + 1)
		if row["id"].(int64) != expectedID {
			t.Errorf("Row %d: expected id = %d, got %v", i, expectedID, row["id"])
		}
	}
	
	// Clean up
	if err := spiller.CleanupSpillFile(spillFile); err != nil {
		t.Errorf("CleanupSpillFile() error = %v", err)
	}
}

