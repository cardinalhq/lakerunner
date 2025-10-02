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

package filereader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestCookedLogTranslatingReader_FingerprintConversion(t *testing.T) {
	tests := []struct {
		name          string
		inputValue    any
		expectedValue any
		shouldConvert bool
	}{
		{
			name:          "string fingerprint converts to int64",
			inputValue:    "7754623969787599908",
			expectedValue: int64(7754623969787599908),
			shouldConvert: true,
		},
		{
			name:          "byte slice fingerprint converts to int64",
			inputValue:    []byte("7754623969787599908"),
			expectedValue: int64(7754623969787599908),
			shouldConvert: true,
		},
		{
			name:          "int64 fingerprint remains int64",
			inputValue:    int64(7754623969787599908),
			expectedValue: int64(7754623969787599908),
			shouldConvert: true,
		},
		{
			name:          "int fingerprint converts to int64",
			inputValue:    int(123456789),
			expectedValue: int64(123456789),
			shouldConvert: true,
		},
		{
			name:          "int32 fingerprint converts to int64",
			inputValue:    int32(123456789),
			expectedValue: int64(123456789),
			shouldConvert: true,
		},
		{
			name:          "uint32 fingerprint converts to int64",
			inputValue:    uint32(123456789),
			expectedValue: int64(123456789),
			shouldConvert: true,
		},
		{
			name:          "uint64 fingerprint converts to int64",
			inputValue:    uint64(123456789),
			expectedValue: int64(123456789),
			shouldConvert: true,
		},
		{
			name:          "invalid string fingerprint remains unchanged",
			inputValue:    "not-a-number",
			expectedValue: "not-a-number",
			shouldConvert: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock reader that returns a batch with a fingerprint
			batch := createBatchWithFingerprint(tt.inputValue)
			mockReader := &mockReaderImpl{
				batches: []*pipeline.Batch{batch},
			}

			reader := NewCookedLogTranslatingReader(mockReader)
			ctx := context.Background()

			resultBatch, err := reader.Next(ctx)
			require.NoError(t, err)
			require.NotNil(t, resultBatch)

			// Check the fingerprint was converted
			row := resultBatch.Get(0)
			fpValue, exists := row[wkk.RowKeyCFingerprint]
			require.True(t, exists)

			if tt.shouldConvert {
				require.Equal(t, tt.expectedValue, fpValue)
			} else {
				// Invalid values should remain unchanged
				require.Equal(t, tt.inputValue, fpValue)
			}
		})
	}
}

func createBatchWithFingerprint(fpValue any) *pipeline.Batch {
	batch := pipeline.GetBatch()
	row := batch.AddRow()
	row[wkk.RowKeyCFingerprint] = fpValue
	row[wkk.RowKeyCTimestamp] = int64(1234567890) // Required field
	row[wkk.RowKeyCMessage] = "test message"
	return batch
}

// mockReaderImpl implements Reader for testing
type mockReaderImpl struct {
	batches []*Batch
	index   int
}

func (m *mockReaderImpl) Next(ctx context.Context) (*Batch, error) {
	if m.index >= len(m.batches) {
		return nil, nil
	}
	batch := m.batches[m.index]
	m.index++
	return batch, nil
}

func (m *mockReaderImpl) Close() error {
	return nil
}

func (m *mockReaderImpl) TotalRowsReturned() int64 {
	return 0
}
