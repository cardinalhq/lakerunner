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
	"unicode/utf8"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/cardinalhq/lakerunner/pipeline"
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
	// resource_customer_domain and resource_service_name are included to support efficient
	// stream/series lookups for logs.
	FullValueDimensions = []string{
		"metric_name",
		"resource_file",
	}

	// dimensionsToIndexSet is a pre-built map for O(1) lookup of indexed dimensions
	dimensionsToIndexSet map[string]struct{}

	// fullValueDimensionsSet is a pre-built map for O(1) lookup of full-value dimensions
	fullValueDimensionsSet map[string]struct{}
)

func init() {
	dimensionsToIndexSet = make(map[string]struct{}, len(DimensionsToIndex))
	for _, dim := range DimensionsToIndex {
		dimensionsToIndexSet[dim] = struct{}{}
	}

	fullValueDimensionsSet = make(map[string]struct{}, len(FullValueDimensions))
	for _, dim := range FullValueDimensions {
		fullValueDimensionsSet[dim] = struct{}{}
	}
}

// ToFingerprints converts a map of tagName → slice of tagValues into a set of fingerprints.
func ToFingerprints(tagValuesByName map[string]mapset.Set[string]) mapset.Set[int64] {
	fingerprints := mapset.NewSet[int64]()

	for tagName, values := range tagValuesByName {
		fingerprints.Add(ComputeFingerprint(tagName, ExistsRegex))

		if _, isIndexed := dimensionsToIndexSet[tagName]; !isIndexed {
			continue
		}

		if _, isFullValue := fullValueDimensionsSet[tagName]; isFullValue {
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

const (
	// maxFullValueCacheSize is the maximum number of entries in the full-value cache.
	// When exceeded, cache lookups are skipped and values are computed directly.
	// This prevents unbounded memory growth while still providing benefit for typical workloads.
	maxFullValueCacheSize = 10000
)

// FieldFingerprinter generates fingerprints for row fields with caching.
// Create once and reuse across rows for best performance.
type FieldFingerprinter struct {
	existsFpCache    map[string]int64
	fullValueFpCache map[string]int64 // Cache for fieldName:value -> fingerprint (bounded to maxFullValueCacheSize)
}

// NewFieldFingerprinter creates a new fingerprinter with empty caches.
func NewFieldFingerprinter() *FieldFingerprinter {
	return &FieldFingerprinter{
		existsFpCache:    make(map[string]int64),
		fullValueFpCache: make(map[string]int64),
	}
}

// GenerateFingerprints generates comprehensive fingerprints for a Row.
// Returns a deduplicated slice of fingerprints.
func (f *FieldFingerprinter) GenerateFingerprints(row pipeline.Row) []int64 {
	fingerprints := mapset.NewSet[int64]()

	for key, fieldValue := range row {
		if fieldValue == nil {
			continue
		}

		fieldName := string(key.Value())

		// Get or compute the "exists" fingerprint for this field (cached)
		existsFp, found := f.existsFpCache[fieldName]
		if !found {
			existsFp = ComputeFingerprint(fieldName, ExistsRegex)
			f.existsFpCache[fieldName] = existsFp
		}
		fingerprints.Add(existsFp)

		// Check if this field should be indexed for value-based fingerprints (O(1) lookup)
		if _, isIndexed := dimensionsToIndexSet[fieldName]; !isIndexed {
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

		if strValue == "" {
			continue
		}

		// Check if this is a full-value dimension (no trigrams) - O(1) lookup
		if _, isFullValue := fullValueDimensionsSet[fieldName]; isFullValue {
			// Try to use cache for full-value fingerprints
			cacheKey := fieldName + ":" + strValue
			if fp, found := f.fullValueFpCache[cacheKey]; found {
				fingerprints.Add(fp)
			} else {
				fp := ComputeFingerprint(fieldName, strValue)
				fingerprints.Add(fp)
				// Add to cache only if not full
				if len(f.fullValueFpCache) < maxFullValueCacheSize {
					f.fullValueFpCache[cacheKey] = fp
				}
			}
			continue
		}

		// Generate trigram fingerprints
		trigrams := ToTrigrams(strValue)
		for _, trigram := range trigrams {
			fp := ComputeFingerprint(fieldName, trigram)
			fingerprints.Add(fp)
		}
	}

	return fingerprints.ToSlice()
}
