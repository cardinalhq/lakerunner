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

package parquetwriter

import (
	"context"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter/spillers"
)

func TestSpillableOrderer_InMemoryOnly(t *testing.T) {
	tmpdir := t.TempDir()
	keyFunc := func(row map[string]any) any {
		return row["timestamp"].(int64)
	}

	// Create orderer with large buffer (won't spill for this test)
	orderer := NewSpillableOrderer(keyFunc, 1000, tmpdir, spillers.NewGobSpiller())
	defer orderer.Close()

	// Add rows out of order
	rows := []map[string]any{
		{"timestamp": int64(300), "message": "third"},
		{"timestamp": int64(100), "message": "first"},
		{"timestamp": int64(200), "message": "second"},
	}

	for _, row := range rows {
		if err := orderer.Add(row); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	// Flush and verify sorted output
	var result []map[string]any
	writer := func(batch []map[string]any) error {
		result = append(result, batch...)
		return nil
	}

	if err := orderer.Flush(context.Background(), writer); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify sorted order
	expectedOrder := []string{"first", "second", "third"}
	if len(result) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result))
	}

	for i, row := range result {
		if row["message"].(string) != expectedOrder[i] {
			t.Errorf("Row %d: expected message = %s, got %s", i, expectedOrder[i], row["message"])
		}
	}
}

func TestSpillableOrderer_WithSpilling(t *testing.T) {
	tmpdir := t.TempDir()
	keyFunc := func(row map[string]any) any {
		return row["timestamp"].(int64)
	}

	// Create orderer with small buffer (will spill)
	orderer := NewSpillableOrderer(keyFunc, 2, tmpdir, spillers.NewGobSpiller())
	defer orderer.Close()

	// Add 5 rows out of order (will cause spilling)
	rows := []map[string]any{
		{"timestamp": int64(500), "message": "fifth"},
		{"timestamp": int64(100), "message": "first"},
		{"timestamp": int64(300), "message": "third"}, // This will trigger first spill
		{"timestamp": int64(200), "message": "second"},
		{"timestamp": int64(400), "message": "fourth"}, // This will trigger second spill
	}

	for _, row := range rows {
		if err := orderer.Add(row); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	// Flush and verify sorted output
	var result []map[string]any
	writer := func(batch []map[string]any) error {
		result = append(result, batch...)
		return nil
	}

	if err := orderer.Flush(context.Background(), writer); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify sorted order across all spilled data
	expectedOrder := []string{"first", "second", "third", "fourth", "fifth"}
	if len(result) != 5 {
		t.Fatalf("Expected 5 rows, got %d", len(result))
	}

	for i, row := range result {
		if row["message"].(string) != expectedOrder[i] {
			t.Errorf("Row %d: expected message = %s, got %s", i, expectedOrder[i], row["message"])
		}
	}
}

func TestSpillableOrderer_LargeDataset(t *testing.T) {
	tmpdir := t.TempDir()
	keyFunc := func(row map[string]any) any {
		return row["timestamp"].(int64)
	}

	// Small buffer to force multiple spills
	orderer := NewSpillableOrderer(keyFunc, 10, tmpdir, spillers.NewGobSpiller())
	defer orderer.Close()

	// Add 100 rows in reverse order
	const numRows = 100
	for i := numRows; i >= 1; i-- {
		row := map[string]any{
			"timestamp": int64(i),
			"value":     i * 10,
		}
		if err := orderer.Add(row); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	// Flush and collect results
	var result []map[string]any
	writer := func(batch []map[string]any) error {
		result = append(result, batch...)
		return nil
	}

	if err := orderer.Flush(context.Background(), writer); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Verify we got all rows in sorted order
	if len(result) != numRows {
		t.Fatalf("Expected %d rows, got %d", numRows, len(result))
	}

	for i, row := range result {
		expectedTimestamp := int64(i + 1)
		expectedValue := (i + 1) * 10

		if row["timestamp"].(int64) != expectedTimestamp {
			t.Errorf("Row %d: expected timestamp = %d, got %v", i, expectedTimestamp, row["timestamp"])
		}

		if row["value"].(int) != expectedValue {
			t.Errorf("Row %d: expected value = %d, got %v", i, expectedValue, row["value"])
		}
	}
}

func TestSpillableOrderer_ContextCancellation(t *testing.T) {
	tmpdir := t.TempDir()
	keyFunc := func(row map[string]any) any {
		return row["id"].(int64)
	}

	orderer := NewSpillableOrderer(keyFunc, 5, tmpdir, spillers.NewGobSpiller())
	defer orderer.Close()

	// Add enough rows to trigger spilling
	for i := 1; i <= 20; i++ {
		row := map[string]any{"id": int64(i)}
		if err := orderer.Add(row); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	// Cancel context during flush
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	writer := func(batch []map[string]any) error {
		return nil
	}

	err := orderer.Flush(ctx, writer)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}
