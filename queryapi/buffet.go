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

package queryapi

const (
	existsRegex = ".*"
)

var (
	infraDimensions = []string{
		"resource.k8s.namespace.name",
		"resource.service.name",
		"resource.file",
	}
	dimensionsToIndex = append([]string{
		"_cardinalhq.telemetry_type",
		"_cardinalhq.name",
		"_cardinalhq.level",
		//"_cardinalhq.message",
		"_cardinalhq.span_trace_id",
	}, infraDimensions...)
)

// computeFingerprint combines fieldName and trigram and hashes them.
func computeFingerprint(fieldName, trigram string) int64 {
	s := fieldName + ":" + trigram
	return computeHash(s)
}

func computeHash(str string) int64 {
	var h int64
	length := len(str)
	i := 0

	for i+3 < length {
		h = 31*31*31*31*h +
			31*31*31*int64(str[i]) +
			31*31*int64(str[i+1]) +
			31*int64(str[i+2]) +
			int64(str[i+3])
		i += 4
	}
	for ; i < length; i++ {
		h = 31*h + int64(str[i])
	}

	return h
}
