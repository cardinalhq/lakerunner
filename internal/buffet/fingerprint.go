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

package buffet

import (
	"slices"
	"unicode/utf8"

	mapset "github.com/deckarep/golang-set/v2"
)

const (
	ExistsRegex = ".*"
)

var (
	InfraDimensions = []string{
		"resource.k8s.namespace.name",
		"resource.service.name",
		"resource.file",
	}
	DimensionsToIndex = append([]string{
		//"_cardinalhq.telemetry_type",
		"_cardinalhq.name",
		"_cardinalhq.level",
		//"_cardinalhq.message",
		"_cardinalhq.span_trace_id",
	}, InfraDimensions...)
	IndexFullValueDimensions = []string{"_cardinalhq.name", "resource.file"}
)

// ToFingerprints converts a map of tagName → slice of tagValues into a set of fingerprints.
func ToFingerprints(tagValuesByName map[string]mapset.Set[string]) mapset.Set[int64] {
	fingerprints := mapset.NewSet[int64]()

	for tagName, values := range tagValuesByName {
		if !slices.Contains(DimensionsToIndex, tagName) {
			fp := ComputeFingerprint(tagName, ExistsRegex)
			fingerprints.Add(fp)
			continue
		}

		if slices.Contains(IndexFullValueDimensions, tagName) {
			fingerprints.Add(ComputeFingerprint(tagName, ExistsRegex))
			for _, tagValue := range values.ToSlice() {
				fingerprints.Add(ComputeFingerprint(tagName, tagValue))
			}
			continue
		}

		// looks like full trigrams
		for _, tagValue := range values.ToSlice() {
			trigrams := ToTrigrams(tagValue)
			for _, trigram := range trigrams {
				fp := ComputeFingerprint(tagName, trigram)
				fingerprints.Add(fp)
			}
		}
	}

	return fingerprints
}

// ToTrigrams builds the set of 3-character substrings plus the wildcard.
func ToTrigrams(str string) []string {
	ngrams := make(map[string]struct{})
	for i := 0; i < len(str); {
		j, cnt := i, 0
		for j < len(str) && cnt < 3 {
			_, size := utf8.DecodeRuneInString(str[j:])
			j += size
			cnt++
		}
		if cnt < 3 {
			break
		}
		ngrams[str[i:j]] = struct{}{}
		_, size := utf8.DecodeRuneInString(str[i:])
		i += size
	}

	res := make([]string, 0, len(ngrams)+1)
	for ngram := range ngrams {
		res = append(res, ngram)
	}
	res = append(res, ExistsRegex)
	return res
}

// ComputeFingerprint combines fieldName and trigram and hashes them.
func ComputeFingerprint(fieldName, trigram string) int64 {
	s := fieldName + ":" + trigram
	return ComputeHash(s)
}

func ComputeHash(str string) int64 {
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
