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

package rowcodec

import (
	"fmt"
	"testing"
)

func TestCodecComparison(t *testing.T) {
	// Create test data - typical telemetry row
	testRow := map[string]any{
		"timestamp":     int64(1704067200000),
		"service":       "api-gateway",
		"level":         "INFO",
		"message":       "Request processed successfully",
		"latency_ms":    float64(123.45),
		"status_code":   int64(200),
		"request_id":    "abc123def456",
		"user_id":       int64(987654321),
		"method":        "POST",
		"path":          "/api/v1/users/create",
		"ip_address":    "192.168.1.100",
		"user_agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"response_size": int64(2048),
		"error":         nil,
		"tags":          []byte(`{"env":"prod","region":"us-east-1"}`),
	}

	// Test Binary codec
	binaryCodec, err := NewBinary()
	if err != nil {
		t.Fatal(err)
	}
	binaryData, err := binaryCodec.Encode(testRow)
	if err != nil {
		t.Fatal(err)
	}

	// Test CBOR codec
	cborCodec, err := NewCBOR()
	if err != nil {
		t.Fatal(err)
	}
	cborData, err := cborCodec.Encode(testRow)
	if err != nil {
		t.Fatal(err)
	}

	// Test GOB codec
	gobCodec, err := NewGOB()
	if err != nil {
		t.Fatal(err)
	}
	gobData, err := gobCodec.Encode(testRow)
	if err != nil {
		t.Fatal(err)
	}

	// Print sizes
	fmt.Printf("\nCodec Size Comparison:\n")
	fmt.Printf("Binary: %d bytes\n", len(binaryData))
	fmt.Printf("CBOR:   %d bytes\n", len(cborData))
	fmt.Printf("GOB:    %d bytes\n", len(gobData))

	// Calculate size differences
	fmt.Printf("\nSize relative to Binary:\n")
	fmt.Printf("CBOR:   %.1f%%\n", float64(len(cborData))*100/float64(len(binaryData)))
	fmt.Printf("GOB:    %.1f%%\n", float64(len(gobData))*100/float64(len(binaryData)))
}
