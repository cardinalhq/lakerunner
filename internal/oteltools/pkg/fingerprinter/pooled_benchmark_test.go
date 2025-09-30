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

package fingerprinter

import (
	"testing"
)

func BenchmarkFingerprintingWithPools(b *testing.B) {
	fp := NewFingerprinter()
	clusterManager := NewTrieClusterManager(0.5)

	testInputs := []string{
		"INFO Received request for /api/v1/endpoint from userId=12345",
		"ERROR Failed to connect to database connection timeout after 30s",
		"DEBUG Processing user authentication for email user@example.com",
		"WARN High memory usage detected: 85% of 8GB used",
		"TRACE Executing SQL query: SELECT * FROM users WHERE active = true",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		input := testInputs[i%len(testInputs)]
		_, _, _ = fp.Fingerprint(input, clusterManager)
	}
}

func BenchmarkTokenizationWithPools(b *testing.B) {
	fp := NewFingerprinter()

	testInputs := []string{
		"INFO Received request for /api/v1/endpoint from userId=12345",
		"ERROR Failed to connect to database connection timeout after 30s",
		"DEBUG Processing user authentication for email user@example.com",
		"WARN High memory usage detected: 85% of 8GB used",
		"TRACE Executing SQL query: SELECT * FROM users WHERE active = true",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		input := testInputs[i%len(testInputs)]
		_, _, _ = fp.tokenizeString(input)
	}
}

// Benchmark object pool overhead
func BenchmarkObjectPoolOverhead(b *testing.B) {
	b.Run("TokenSeq", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ts := getTokenSeq()
			ts.add("test")
			putTokenSeq(ts)
		}
	})

	b.Run("StringSlice", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			slice := getStringSlice()
			slice = append(slice, "test")
			putStringSlice(slice)
		}
	})

	b.Run("StringBuilder", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sb := getStringBuilder()
			sb.WriteString("test")
			_ = sb.String()
			putStringBuilder(sb)
		}
	})
}
