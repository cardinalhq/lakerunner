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

package fingerprint

import (
	"fmt"
	"slices"
	"unicode/utf8"

	mapset "github.com/deckarep/golang-set/v2"
)

const (
	// ExistsRegex is the wildcard pattern used for "field exists" fingerprints
	ExistsRegex = ".*"
)

var (
	// DimensionsToIndex are all dimensions that should have fingerprints generated
	DimensionsToIndex = []string{
		"chq_telemetry_type",
		"log_level",
		"metric_name",
		"resource_customer_domain",
		"resource_file",
		"resource_k8s_namespace_name",
		"resource_service_name",
		"span_trace_id",
	}

	// FullValueDimensions are dimensions that should be indexed with exact full values
	// instead of trigrams. These support exact matching efficiently but not substring/regex.
	FullValueDimensions = []string{
		"metric_name",
		"resource_file",
	}
)

// ToFingerprints converts a map of tagName → slice of tagValues into a set of fingerprints.
func ToFingerprints(tagValuesByName map[string]mapset.Set[string]) mapset.Set[int64] {
	fingerprints := mapset.NewSet[int64]()

	for tagName, values := range tagValuesByName {
		fingerprints.Add(ComputeFingerprint(tagName, ExistsRegex))

		if !slices.Contains(DimensionsToIndex, tagName) {
			continue
		}

		if slices.Contains(FullValueDimensions, tagName) {
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

	res := make([]string, 0, len(ngrams))
	for ngram := range ngrams {
		res = append(res, ngram)
	}
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
		h = 31*31*31*31*h + 31*31*31*int64(str[i]) + 31*31*int64(str[i+1]) + 31*int64(str[i+2]) + int64(str[i+3])
		i += 4
	}
	for ; i < length; i++ {
		h = 31*h + int64(str[i])
	}

	return h
}

// GenerateRowFingerprints extracts field values from a row and generates comprehensive fingerprints
func GenerateRowFingerprints(row map[string]any) mapset.Set[int64] {
	tagValuesByName := make(map[string]mapset.Set[string])

	// Extract values for each field in the row
	for fieldName, fieldValue := range row {
		if fieldValue == nil {
			continue
		}

		// Convert field value to string
		var strValue string
		switch v := fieldValue.(type) {
		case string:
			strValue = v
		case []byte:
			strValue = string(v)
		default:
			// For non-string values, convert to string representation
			strValue = fmt.Sprintf("%v", v)
		}

		if strValue != "" {
			if _, exists := tagValuesByName[fieldName]; !exists {
				tagValuesByName[fieldName] = mapset.NewSet[string]()
			}
			tagValuesByName[fieldName].Add(strValue)
		}
	}

	return ToFingerprints(tagValuesByName)
}
