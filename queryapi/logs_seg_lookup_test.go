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

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/buffet"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const wantFreq = int64(10 * time.Second / time.Millisecond)

func TestLookupLogsSegments_CoarseOnly_LineNotContains(t *testing.T) {

	// --- inputs ---
	orgID := uuid.New()
	dih := DateIntHours{DateInt: 20250101}
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)
	startTs := start.UnixMilli()
	endTs := end.UnixMilli()

	// Use a line "negative" filter so we avoid trigram compilation entirely.
	leaf := logql.LogLeaf{
		LineFilters: []logql.LineFilter{
			{Op: logql.LineNotContains, Match: "xyz"},
		},
	}

	// Expected coarse fingerprint for the message field
	expFP := buffet.ComputeFingerprint("_cardinalhq.message", buffet.ExistsRegex)

	// Capture the params we send to the DB lookup.
	var gotParams lrdb.ListLogSegmentsForQueryParams

	row1End := start.Add(30 * time.Minute).UnixMilli() // 00:30 -> "00"
	row2End := start.Add(90 * time.Minute).UnixMilli() // 01:30 -> "01"

	fakeRows := []lrdb.ListLogSegmentsForQueryRow{
		{
			Fingerprint: expFP,
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       row1End,
		},
		{
			Fingerprint: expFP,
			InstanceNum: 1,
			SegmentID:   202,
			StartTs:     startTs,
			EndTs:       row2End,
		},
	}

	// Stub lookup func
	var lookup SegmentLookupFunc = func(ctx context.Context, p lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
		gotParams = p
		return fakeRows, nil
	}

	q := &QuerierService{}

	segs, err := q.lookupLogsSegments(
		context.Background(),
		dih,
		leaf,
		startTs, endTs,
		orgID,
		lookup,
	)
	if err != nil {
		t.Fatalf("lookupLogsSegments error: %v", err)
	}

	if gotParams.OrganizationID != orgID {
		t.Fatalf("org id mismatch in params")
	}
	if int(gotParams.Dateint) != dih.DateInt {
		t.Fatalf("dateint mismatch in params: got %d want %d", gotParams.Dateint, dih.DateInt)
	}
	if gotParams.S != startTs || gotParams.E != endTs {
		t.Fatalf("time bounds mismatch in params: got [%d,%d) want [%d,%d)",
			gotParams.S, gotParams.E, startTs, endTs)
	}
	if len(gotParams.Fingerprints) != 1 || gotParams.Fingerprints[0] != expFP {
		t.Fatalf("fingerprints mismatch: got %v want [%d]", gotParams.Fingerprints, expFP)
	}

	if len(segs) != 2 {
		t.Fatalf("expected 2 segments, got %d: %#v", len(segs), segs)
	}

	byID := map[int64]SegmentInfo{}
	for _, s := range segs {
		byID[s.SegmentID] = s
	}

	s1, ok := byID[101]
	if !ok {
		t.Fatalf("missing segment tbl_101 in results: %#v", segs)
	}
	s2, ok := byID[202]
	if !ok {
		t.Fatalf("missing segment tbl_202 in results: %#v", segs)
	}

	for _, s := range []SegmentInfo{s1, s2} {
		if s.DateInt != dih.DateInt {
			t.Fatalf("DateInt mismatch: got %d want %d", s.DateInt, dih.DateInt)
		}
		if s.OrganizationID != orgID {
			t.Fatalf("OrganizationID mismatch")
		}
		// Current implementation in lookupLogsSegments sets coarse frequency to 10s.
		if s.Frequency != wantFreq {
			t.Fatalf("Frequency mismatch: got %d want %d", s.Frequency, wantFreq)
		}
	}

	if s1.Hour != "00" {
		t.Fatalf("hour(EndTs row1) mismatch: got %q want %q", s1.Hour, "00")
	}
	if s2.Hour != "00" {
		t.Fatalf("hour(EndTs row2) mismatch: got %q want %q", s2.Hour, "00")
	}

	if s1.StartTs != startTs || s1.EndTs != row1End {
		t.Fatalf("row1 ts mismatch: got [%d,%d) want [%d,%d)", s1.StartTs, s1.EndTs, startTs, row1End)
	}
	if s2.StartTs != startTs || s2.EndTs != row2End {
		t.Fatalf("row2 ts mismatch: got [%d,%d) want [%d,%d)", s2.StartTs, s2.EndTs, startTs, row2End)
	}

	if s1.InstanceNum != 1 || s2.InstanceNum != 1 {
		t.Fatalf("InstanceNum mismatch: %#v", segs)
	}

	// Ensure the lookup saw only the expected fp (sorted)
	wantedFPs := []int64{expFP}
	gotFPs := slices.Clone(gotParams.Fingerprints)
	slices.Sort(gotFPs)
	if !slices.Equal(gotFPs, wantedFPs) {
		t.Fatalf("lookup fingerprints mismatch: got %v want %v", gotFPs, wantedFPs)
	}
}
