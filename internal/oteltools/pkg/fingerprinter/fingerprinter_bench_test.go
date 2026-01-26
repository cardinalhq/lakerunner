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

package fingerprinter

import (
	"testing"
)

func BenchmarkFingerprint(b *testing.B) {
	fp := NewFingerprinter()
	cm := NewTrieClusterManager(0.5)

	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "PlainText",
			input: "ERROR: Connection failed to database server at 192.168.1.1:5432",
		},
		{
			name:  "WithJSON",
			input: `Request failed {"error": "timeout", "code": 500, "duration": 1234}`,
		},
		{
			name:  "LargeJSON",
			input: `Processing batch {"items": [1,2,3,4,5], "status": "success", "timestamp": 1234567890, "metadata": {"version": "1.0", "region": "us-east-1"}}`,
		},
		{
			name:  "NoJSON",
			input: "Simple log message without any JSON content at all",
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				fingerprint, _, err := fp.Fingerprint(tc.input, cm)
				if err != nil {
					b.Fatal(err)
				}
				_ = fingerprint
			}
		})
	}
}
