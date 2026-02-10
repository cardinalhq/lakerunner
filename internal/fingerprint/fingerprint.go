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

// IndexFlags defines which fingerprinting strategies to use for a dimension.
// Use the helper methods HasExact() and HasTrigram() to check flags.
type IndexFlags uint8

const (
	// IndexExact generates a fingerprint for the exact full value.
	// This is the baseline for any indexed field and supports efficient exact matching.
	IndexExact IndexFlags = 1 << 0

	// IndexTrigramExact generates both trigram fingerprints AND the exact value fingerprint.
	// This supports both substring/regex matching (via trigrams) and exact matching.
	// Note: IndexTrigramExact includes IndexExact - trigram-only indexing is not supported.
	IndexTrigramExact IndexFlags = (1 << 1) | IndexExact
)

// HasExact returns true if the flags include exact value fingerprinting.
func (f IndexFlags) HasExact() bool {
	return f&IndexExact != 0
}

// HasTrigram returns true if the flags include trigram fingerprinting.
func (f IndexFlags) HasTrigram() bool {
	return f&IndexTrigramExact == IndexTrigramExact
}

// IndexedDimensions maps dimension names to their indexing strategy.
// Fields not in this map only get "exists" fingerprints.
var IndexedDimensions = map[string]IndexFlags{
	"chq_telemetry_type":          IndexTrigramExact,
	"log_level":                   IndexExact,
	"metric_name":                 IndexExact,
	"resource_customer_domain":    IndexTrigramExact,
	"resource_file":               IndexExact,
	"resource_k8s_cluster_name":   IndexTrigramExact,
	"resource_k8s_namespace_name": IndexTrigramExact,
	"resource_service_name":       IndexTrigramExact,
	"span_trace_id":               IndexTrigramExact,
}

// IsIndexed returns true if the field has any indexing configured.
func IsIndexed(field string) bool {
	_, ok := IndexedDimensions[field]
	return ok
}

// HasExactIndex returns true if the field has exact value fingerprinting.
func HasExactIndex(field string) bool {
	flags, ok := IndexedDimensions[field]
	return ok && flags.HasExact()
}

// HasTrigramIndex returns true if the field has trigram fingerprinting.
func HasTrigramIndex(field string) bool {
	flags, ok := IndexedDimensions[field]
	return ok && flags.HasTrigram()
}

// ToFingerprints converts a map of tagName → slice of tagValues into a set of fingerprints.
func ToFingerprints(tagValuesByName map[string]mapset.Set[string]) mapset.Set[int64] {
	fingerprints := mapset.NewSet[int64]()

	for tagName, values := range tagValuesByName {
		fingerprints.Add(ComputeFingerprint(tagName, ExistsRegex))

		flags, isIndexed := IndexedDimensions[tagName]
		if !isIndexed {
			continue
		}

		for _, tagValue := range values.ToSlice() {
			// Always add exact value fingerprint for indexed fields
			if flags.HasExact() {
				fingerprints.Add(ComputeFingerprint(tagName, tagValue))
			}

			// Add trigram fingerprints if configured
			if flags.HasTrigram() {
				for _, trigram := range toTrigrams(tagValue) {
					fingerprints.Add(ComputeFingerprint(tagName, trigram))
				}
			}
		}
	}

	return fingerprints
}

// ToTrigrams builds the set of 3-character substrings plus the wildcard.
func toTrigrams(str string) []string {
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
	streamField      string           // Org-specific stream field to treat as IndexTrigramExact
}

// NewFieldFingerprinter creates a new fingerprinter with empty caches.
// If streamField is non-empty, that field will be treated as IndexTrigramExact
// in addition to the static IndexedDimensions.
func NewFieldFingerprinter(streamField string) *FieldFingerprinter {
	return &FieldFingerprinter{
		existsFpCache:    make(map[string]int64),
		fullValueFpCache: make(map[string]int64),
		streamField:      streamField,
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

		// Check if this field should be indexed for value-based fingerprints
		// First check the static map, then check the org-specific stream field
		flags, isIndexed := IndexedDimensions[fieldName]
		if !isIndexed && f.streamField != "" && fieldName == f.streamField {
			// Org-specific stream field always gets IndexTrigramExact
			flags = IndexTrigramExact
			isIndexed = true
		}
		if !isIndexed {
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

		// Add exact value fingerprint if configured
		if flags.HasExact() {
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
		}

		// Add trigram fingerprints if configured
		if flags.HasTrigram() {
			for _, trigram := range toTrigrams(strValue) {
				fingerprints.Add(ComputeFingerprint(fieldName, trigram))
			}
		}
	}

	return fingerprints.ToSlice()
}
