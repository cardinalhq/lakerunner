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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

//
// chooseObjectID tests
//

func TestChooseObjectID(t *testing.T) {
	ctx := context.Background()
	const bucket = "b"
	const tmpdir = "/tmpdir"
	const good = "good-key"
	const bad = "bad-key"

	t.Run("good exists -> returns good", func(t *testing.T) {
		ff := &fakeFetcher{
			resp: map[string]struct {
				tmp      string
				sz       int64
				notFound bool
				err      error
			}{
				good: {tmp: "/tmp/good", sz: 123, notFound: false, err: nil},
			},
		}
		oid, tmp, sz, err := chooseObjectID(ctx, ff, bucket, good, bad, tmpdir)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if oid != good || tmp != "/tmp/good" || sz != 123 {
			t.Fatalf("got (%q,%q,%d), want (%q,%q,%d)", oid, tmp, sz, good, "/tmp/good", int64(123))
		}
		if len(ff.calls) != 1 || ff.calls[0] != good {
			t.Fatalf("calls=%v, want [%s]", ff.calls, good)
		}
	})

	t.Run("good notFound, bad exists -> returns bad", func(t *testing.T) {
		ff := &fakeFetcher{
			resp: map[string]struct {
				tmp      string
				sz       int64
				notFound bool
				err      error
			}{
				good: {tmp: "", sz: 0, notFound: true, err: nil},
				bad:  {tmp: "/tmp/bad", sz: 456, notFound: false, err: nil},
			},
		}
		oid, tmp, sz, err := chooseObjectID(ctx, ff, bucket, good, bad, tmpdir)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if oid != bad || tmp != "/tmp/bad" || sz != 456 {
			t.Fatalf("got (%q,%q,%d), want (%q,%q,%d)", oid, tmp, sz, bad, "/tmp/bad", int64(456))
		}
		if want := []string{good, bad}; len(ff.calls) != 2 || ff.calls[0] != want[0] || ff.calls[1] != want[1] {
			t.Fatalf("calls=%v, want %v", ff.calls, want)
		}
	})

	t.Run("good notFound, bad notFound -> returns empty, no error", func(t *testing.T) {
		ff := &fakeFetcher{
			resp: map[string]struct {
				tmp      string
				sz       int64
				notFound bool
				err      error
			}{
				good: {notFound: true},
				bad:  {notFound: true},
			},
		}
		oid, tmp, sz, err := chooseObjectID(ctx, ff, bucket, good, bad, tmpdir)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if oid != "" || tmp != "" || sz != 0 {
			t.Fatalf("got (%q,%q,%d), want empty", oid, tmp, sz)
		}
	})

	t.Run("good notFound, bad other error -> returns error", func(t *testing.T) {
		someErr := errors.New("boom")
		ff := &fakeFetcher{
			resp: map[string]struct {
				tmp      string
				sz       int64
				notFound bool
				err      error
			}{
				good: {notFound: true},
				bad:  {err: someErr},
			},
		}
		_, _, _, err := chooseObjectID(ctx, ff, bucket, good, bad, tmpdir)
		if err == nil || err.Error() != "boom" {
			t.Fatalf("want error 'boom', got %v", err)
		}
	})

	t.Run("good other error -> returns error immediately", func(t *testing.T) {
		someErr := errors.New("throttle")
		ff := &fakeFetcher{
			resp: map[string]struct {
				tmp      string
				sz       int64
				notFound bool
				err      error
			}{
				good: {err: someErr},
			},
		}
		_, _, _, err := chooseObjectID(ctx, ff, bucket, good, bad, tmpdir)
		if err == nil || err.Error() != "throttle" {
			t.Fatalf("want error 'throttle', got %v", err)
		}
	})
}

//
// downloadAndOpen tests
//

// --- helpers for building expected keys ---

func mkSeg(segID int64, startMs int64) lrdb.GetLogSegmentsForCompactionRow {
	return lrdb.GetLogSegmentsForCompactionRow{
		SegmentID:     segID,
		StartTs:       startMs,
		EndTs:         startMs + int64(time.Minute/time.Millisecond),
		FileSize:      111,
		RecordCount:   22,
		IngestDateint: 20250101,
	}
}

func goodKey(sp storageprofile.StorageProfile, dateint int32, seg lrdb.GetLogSegmentsForCompactionRow) string {
	return helpers.MakeDBObjectID(sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(seg.StartTs), seg.SegmentID, "logs")
}
func badKey(sp storageprofile.StorageProfile, dateint int32, seg lrdb.GetLogSegmentsForCompactionRow) string {
	return helpers.MakeDBObjectIDbad(sp.OrganizationID, sp.CollectorName, dateint, s3helper.HourFromMillis(seg.StartTs), seg.SegmentID, "logs")
}

// --- tests ---

func TestDownloadAndOpen_BasicAndLegacyBadFallback(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	sp := storageprofile.StorageProfile{
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		CollectorName:  "collectorA",
	}
	dateint := int32(20250102)

	// Two segments: s1 good present; s2 good 404 but BAD exists.
	s1 := mkSeg(1001, mustParseMs("2025-01-02T00:05:00Z"))
	s2 := mkSeg(1002, mustParseMs("2025-01-02T01:10:00Z"))

	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			goodKey(sp, dateint, s1): {tmp: tmpdir + "/s1", sz: 10, notFound: false, err: nil},
			goodKey(sp, dateint, s2): {tmp: "", sz: 0, notFound: true, err: nil},
			badKey(sp, dateint, s2):  {tmp: tmpdir + "/s2-bad", sz: 12, notFound: false, err: nil},
		},
	}

	// Provide nodes that include some droppable fields to verify they get removed,
	// plus unrelated dynamic fields we want to keep.
	opener := func(t *testing.T) fakeOpener {
		builder := buffet.NewNodeMapBuilder()
		err := builder.Add(
			map[string]any{
				"_cardinalhq.timestamp": int64(0), // will be forced to INT64
				"resource.foo":          "bar",    // should be kept
				"minute":                int32(0), // should be dropped
			})
		assert.NoError(t, err)

		return fakeOpener{
			nodes: builder.Build(),
		}
	}(t)

	group := []lrdb.GetLogSegmentsForCompactionRow{s1, s2}

	got, err := downloadAndOpen(ctx, sp, dateint, group, tmpdir, "bkt", fetch, opener)
	if err != nil {
		t.Fatalf("downloadAndOpen error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 opened segments, got %d", len(got))
	}

	// Order preserved: s1 then s2
	if got[0].Seg.SegmentID != s1.SegmentID || got[1].Seg.SegmentID != s2.SegmentID {
		t.Fatalf("order not preserved: %v", []int64{got[0].Seg.SegmentID, got[1].Seg.SegmentID})
	}
	// ObjectIDs are selected per path: s1 good; s2 bad fallback
	if got[0].ObjectID != goodKey(sp, dateint, s1) {
		t.Fatalf("s1 ObjectID wrong: %q", got[0].ObjectID)
	}
	if got[1].ObjectID != badKey(sp, dateint, s2) {
		t.Fatalf("s2 ObjectID wrong: %q", got[1].ObjectID)
	}

	// Nodes normalized on each handle: drop "minute", keep "resource.foo",
	// and ensure _cardinalhq.timestamp present (forced to INT64 in code).
	for i, os := range got {
		if _, ok := os.Handle.Nodes["minute"]; ok {
			t.Fatalf("handle[%d] still has dropped field 'minute'", i)
		}
		if _, ok := os.Handle.Nodes["resource.foo"]; !ok {
			t.Fatalf("handle[%d] lost kept field 'resource.foo'", i)
		}
		if _, ok := os.Handle.Nodes["_cardinalhq.timestamp"]; !ok {
			t.Fatalf("handle[%d] missing _cardinalhq.timestamp", i)
		}
	}
}

func TestDownloadAndOpen_BothMissingSkips(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	sp := storageprofile.StorageProfile{OrganizationID: uuid.MustParse("7e2b1c8e-4f3a-4b2e-9c1a-2e7d8b6f5a1c"), CollectorName: "c"}
	dateint := int32(20250103)

	s := mkSeg(2001, mustParseMs("2025-01-03T02:00:00Z"))

	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			goodKey(sp, dateint, s): {notFound: true},
			badKey(sp, dateint, s):  {notFound: true},
		},
	}
	opener := fakeOpener{nodes: map[string]parquet.Node{}}

	got, err := downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 opened segments, got %d", len(got))
	}
}

func TestDownloadAndOpen_OpenSchemaError(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	sp := storageprofile.StorageProfile{OrganizationID: uuid.MustParse("7e2b1c8e-4f3a-4b2e-9c1a-2e7d8b6f5a1c"), CollectorName: "c"}
	dateint := int32(20250104)

	s := mkSeg(3001, mustParseMs("2025-01-04T03:00:00Z"))
	good := goodKey(sp, dateint, s)

	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			good: {tmp: tmpdir + "/to-open", notFound: false},
		},
	}
	openErr := errors.New("schema boom")
	opener := fakeOpener{
		fail: map[string]error{
			tmpdir + "/to-open": openErr,
		},
	}

	_, err := downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	if err == nil || err.Error() != fmt.Errorf("open schema: %w", openErr).Error() {
		t.Fatalf("want wrapped open schema error, got %v", err)
	}
}

func TestDownloadAndOpen_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	tmpdir := t.TempDir()
	sp := storageprofile.StorageProfile{OrganizationID: uuid.MustParse("7e2b1c8e-4f3a-4b2e-9c1a-2e7d8b6f5a1c"), CollectorName: "c"}
	dateint := int32(20250105)

	s := mkSeg(4001, mustParseMs("2025-01-05T04:00:00Z"))
	fetch := &fakeFetcher{resp: map[string]struct {
		tmp      string
		sz       int64
		notFound bool
		err      error
	}{
		// Won't be called; ctx is canceled before loop.
	}}

	opener := fakeOpener{nodes: map[string]parquet.Node{"x": parquet.Leaf(parquet.Int32Type)}}

	_, err := downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// --- small time helper ---

func mustParseMs(iso string) int64 {
	t, err := time.Parse(time.RFC3339, iso)
	if err != nil {
		panic(err)
	}
	return t.UnixMilli()
}

func TestDownloadAndOpen_PrefersGoodWhenBothExist(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	sp := storageprofile.StorageProfile{
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-0000000000aa"),
		CollectorName:  "collectorA",
	}
	dateint := int32(20250106)

	s := mkSeg(5001, mustParseMs("2025-01-06T06:00:00Z"))
	good := goodKey(sp, dateint, s)
	bad := badKey(sp, dateint, s)

	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			good: {tmp: tmpdir + "/s-good", sz: 10, notFound: false},
			bad:  {tmp: tmpdir + "/s-bad", sz: 10, notFound: false}, // should NOT be used
		},
	}

	// Build minimal nodes using the builder (stable across parquet-go versions).
	builder := buffet.NewNodeMapBuilder()
	err := builder.Add(map[string]any{
		"_cardinalhq.timestamp": int64(0),
	})
	assert.NoError(t, err)
	opener := fakeOpener{nodes: builder.Build()}

	got, err := downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 opened segment, got %d", len(got))
	}
	if got[0].ObjectID != good {
		t.Fatalf("expected GOOD key, got %q", got[0].ObjectID)
	}
}

func TestDownloadAndOpen_BadReturnsNon404ErrorBubbles(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	sp := storageprofile.StorageProfile{
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-0000000000bb"),
		CollectorName:  "collectorB",
	}
	dateint := int32(20250107)

	s := mkSeg(6001, mustParseMs("2025-01-07T07:00:00Z"))
	good := goodKey(sp, dateint, s)
	bad := badKey(sp, dateint, s)

	someErr := errors.New("network boom")
	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			good: {notFound: true}, // trigger BAD fallback
			bad:  {err: someErr},   // non-404 error must bubble up
		},
	}

	builder := buffet.NewNodeMapBuilder()
	err := builder.Add(map[string]any{"_cardinalhq.timestamp": int64(0)})
	assert.NoError(t, err)
	opener := fakeOpener{nodes: builder.Build()}

	_, err = downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	if err == nil || err.Error() != "download: network boom" && err.Error() != someErr.Error() {
		// depending on your wrapping ("download: %w"), accept either exact wrap or raw
		t.Fatalf("want bubbled non-404 error, got %v", err)
	}
}

func TestDownloadAndOpen_AddsTimestampWhenMissing(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	sp := storageprofile.StorageProfile{
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-0000000000cc"),
		CollectorName:  "collectorC",
	}
	dateint := int32(20250108)

	s := mkSeg(7001, mustParseMs("2025-01-08T08:00:00Z"))
	good := goodKey(sp, dateint, s)

	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			good: {tmp: tmpdir + "/s1", sz: 10, notFound: false},
		},
	}

	// Build nodes WITHOUT _cardinalhq.timestamp; downloadAndOpen should insert it.
	builder := buffet.NewNodeMapBuilder()
	err := builder.Add(map[string]any{"resource.any": "x"})
	assert.NoError(t, err)
	opener := fakeOpener{nodes: builder.Build()}

	got, err := downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	assert.NoError(t, err)
	if len(got) != 1 {
		t.Fatalf("want 1 opened segment, got %d", len(got))
	}
	if _, ok := got[0].Handle.Nodes["_cardinalhq.timestamp"]; !ok {
		t.Fatalf("expected _cardinalhq.timestamp to be added when missing")
	}
	// Ensure we didn't drop unrelated dynamic fields
	if _, ok := got[0].Handle.Nodes["resource.any"]; !ok {
		t.Fatalf("expected resource.any to remain")
	}
}

func TestDownloadAndOpen_NoDropFields_NoOp(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	sp := storageprofile.StorageProfile{
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-0000000000dd"),
		CollectorName:  "collectorD",
	}
	dateint := int32(20250109)

	s := mkSeg(8001, mustParseMs("2025-01-09T09:00:00Z"))
	good := goodKey(sp, dateint, s)

	fetch := &fakeFetcher{
		resp: map[string]struct {
			tmp      string
			sz       int64
			notFound bool
			err      error
		}{
			good: {tmp: tmpdir + "/s1", sz: 10, notFound: false},
		},
	}

	// Nodes that have nothing in dropFieldNames
	builder := buffet.NewNodeMapBuilder()
	err := builder.Add(map[string]any{
		"_cardinalhq.timestamp": int64(0),
		"scope.x":               "y",
	})
	assert.NoError(t, err)
	opener := fakeOpener{nodes: builder.Build()}

	got, err := downloadAndOpen(ctx, sp, dateint, []lrdb.GetLogSegmentsForCompactionRow{s}, tmpdir, "bkt", fetch, opener)
	assert.NoError(t, err)
	if len(got) != 1 {
		t.Fatalf("want 1 opened segment, got %d", len(got))
	}
	// Should still have scope.x; nothing extra dropped
	if _, ok := got[0].Handle.Nodes["scope.x"]; !ok {
		t.Fatalf("expected scope.x to remain")
	}
}

//
// mergeNodes tests
//

func fhWith(nodes map[string]parquet.Node) *filecrunch.FileHandle {
	return &filecrunch.FileHandle{
		// File/Schema unused in mergeNodes
		Nodes: nodes,
	}
}

func TestMergeNodes_EmptyInput(t *testing.T) {
	got, err := mergeNodes(nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty map, got %d entries", len(got))
	}

	got, err = mergeNodes([]*filecrunch.FileHandle{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty map, got %d entries", len(got))
	}
}

func TestMergeNodes_MergesDistinctKeys(t *testing.T) {
	h1 := fhWith(buildNodes(map[string]any{
		"_cardinalhq.timestamp": int64(0),
		"resource.foo":          "x",
	}))
	h2 := fhWith(buildNodes(map[string]any{
		"scope.bar": "y",
		"log.baz":   "z",
	}))

	got, err := mergeNodes([]*filecrunch.FileHandle{h1, h2})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	wantKeys := []string{"_cardinalhq.timestamp", "resource.foo", "scope.bar", "log.baz"}
	for _, k := range wantKeys {
		if _, ok := got[k]; !ok {
			t.Fatalf("missing merged key %q", k)
		}
	}
}

func TestMergeNodes_DoesNotMutateInputs(t *testing.T) {
	orig := buildNodes(map[string]any{
		"keep.me": "v",
	})
	h := fhWith(orig)

	_, err := mergeNodes([]*filecrunch.FileHandle{h})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Ensure input Nodes still contain the same key
	if _, ok := h.Nodes["keep.me"]; !ok {
		t.Fatalf("input handle Nodes mutated unexpectedly")
	}
}

func TestMergeNodes_ConflictingTypesReturnsError(t *testing.T) {
	// Same logical field different physical types to force a conflict.
	h1 := fhWith(buildNodes(map[string]any{
		"conflict": int64(0), // INT64
	}))
	h2 := fhWith(buildNodes(map[string]any{
		"conflict": int32(0), // INT32
	}))

	_, err := mergeNodes([]*filecrunch.FileHandle{h1, h2})
	if err == nil {
		t.Fatalf("expected error on conflicting types, got nil")
	}
}

func TestMergeNodes_IgnoresNilHandles(t *testing.T) {
	h1 := fhWith(buildNodes(map[string]any{"a": int64(0)}))
	var hNil *filecrunch.FileHandle
	h2 := fhWith(buildNodes(map[string]any{"b": "x"}))

	got, err := mergeNodes([]*filecrunch.FileHandle{h1, hNil, h2})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := got["a"]; !ok {
		t.Fatalf("missing 'a' from merged nodes")
	}
	if _, ok := got["b"]; !ok {
		t.Fatalf("missing 'b' from merged nodes")
	}
}

//
// computeDropSet tests
//

func TestComputeDropSet_AllPresent(t *testing.T) {
	b := buffet.NewNodeMapBuilder()
	fields := map[string]any{
		"_cardinalhq.timestamp": int64(0),
		"minute":                int32(0),
		"hour":                  int32(0),
		"day":                   int32(0),
		"month":                 int32(0),
		"year":                  int32(0),
		"resource.foo":          "x", // should be ignored by drop set
	}
	assert.NoError(t, b.Add(fields))
	nodes := b.Build()

	got := computeDropSet(nodes)
	if len(got) != len(dropFieldNames) {
		t.Fatalf("want %d drop keys, got %d", len(dropFieldNames), len(got))
	}
	for _, k := range dropFieldNames {
		if _, ok := got[k]; !ok {
			t.Fatalf("missing %q in drop set", k)
		}
	}
}

func TestComputeDropSet_SomePresent(t *testing.T) {
	b := buffet.NewNodeMapBuilder()
	err := b.Add(map[string]any{
		"hour":         int32(0),
		"year":         int32(0),
		"resource.bar": "y",
	})
	assert.NoError(t, err)
	nodes := b.Build()

	got := computeDropSet(nodes)
	if len(got) != 2 {
		t.Fatalf("want 2 drop keys, got %d", len(got))
	}
	for _, k := range []string{"hour", "year"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("missing %q in drop set", k)
		}
	}
}

func TestComputeDropSet_NonePresent(t *testing.T) {
	b := buffet.NewNodeMapBuilder()
	err := b.Add(map[string]any{
		"_cardinalhq.timestamp": int64(0),
		"resource.foo":          "x",
		"scope.bar":             "z",
	})
	assert.NoError(t, err)
	nodes := b.Build()

	got := computeDropSet(nodes)
	if len(got) != 0 {
		t.Fatalf("want 0 drop keys, got %d", len(got))
	}
}

func TestComputeDropSet_EmptyOrNilNodes(t *testing.T) {
	got := computeDropSet(nil)
	if len(got) != 0 {
		t.Fatalf("nil nodes: want 0 drop keys, got %d", len(got))
	}

	got = computeDropSet(map[string]parquet.Node{})
	if len(got) != 0 {
		t.Fatalf("empty nodes: want 0 drop keys, got %d", len(got))
	}
}

func TestComputeDropSet_CaseSensitive_NoFalsePositives(t *testing.T) {
	b := buffet.NewNodeMapBuilder()
	err := b.Add(map[string]any{
		"Minute": int32(0), // different case; should NOT match
	})
	assert.NoError(t, err)
	nodes := b.Build()

	got := computeDropSet(nodes)
	if len(got) != 0 {
		t.Fatalf("want 0 drop keys for case-mismatch, got %d", len(got))
	}
}

func TestComputeDropSet_DoesNotMutateInput(t *testing.T) {
	b := buffet.NewNodeMapBuilder()
	err := b.Add(map[string]any{
		"minute": int32(0),
		"keep":   "v",
	})
	assert.NoError(t, err)
	nodes := b.Build()

	_ = computeDropSet(nodes)

	// Ensure original nodes still have their keys
	if _, ok := nodes["minute"]; !ok {
		t.Fatalf("input nodes mutated: missing 'minute'")
	}
	if _, ok := nodes["keep"]; !ok {
		t.Fatalf("input nodes mutated: missing 'keep'")
	}
}

//
// normalizeRecord tests
//

func makeDrop(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

func TestNormalizeRecord_Int64_NoDrop(t *testing.T) {
	rec := map[string]any{
		"_cardinalhq.timestamp": int64(123),
		"keep":                  "v",
	}
	out, err := normalizeRecord(rec, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// same map mutated in place
	if out["_cardinalhq.timestamp"].(int64) != 123 {
		t.Fatalf("timestamp changed unexpectedly: %#v", out["_cardinalhq.timestamp"])
	}
	if _, ok := out["keep"]; !ok {
		t.Fatalf("expected 'keep' to remain")
	}
}

func TestNormalizeRecord_Int32_Coerces_And_Drops(t *testing.T) {
	rec := map[string]any{
		"_cardinalhq.timestamp": int32(42),
		"minute":                1,
		"hour":                  2,
		"keep":                  true,
	}
	out, err := normalizeRecord(rec, makeDrop("minute", "hour"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	ts, ok := out["_cardinalhq.timestamp"].(int64)
	if !ok || ts != 42 {
		t.Fatalf("timestamp not coerced to int64 correctly: %#v", out["_cardinalhq.timestamp"])
	}
	if _, ok := out["minute"]; ok {
		t.Fatalf("'minute' should have been dropped")
	}
	if _, ok := out["hour"]; ok {
		t.Fatalf("'hour' should have been dropped")
	}
	if _, ok := out["keep"]; !ok {
		t.Fatalf("'keep' should remain")
	}
}

func TestNormalizeRecord_Float64_Coerces(t *testing.T) {
	rec := map[string]any{
		"_cardinalhq.timestamp": float64(123.9), // will truncate via int64()
	}
	out, err := normalizeRecord(rec, nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out["_cardinalhq.timestamp"].(int64) != int64(123) {
		t.Fatalf("timestamp not coerced/truncated as expected: %#v", out["_cardinalhq.timestamp"])
	}
}

func TestNormalizeRecord_MissingTimestamp(t *testing.T) {
	rec := map[string]any{
		"other": 1,
	}
	_, err := normalizeRecord(rec, nil)
	if err == nil || err.Error() != "missing _cardinalhq.timestamp" {
		t.Fatalf("want 'missing _cardinalhq.timestamp' error, got %v", err)
	}
}

func TestNormalizeRecord_UnexpectedType(t *testing.T) {
	rec := map[string]any{
		"_cardinalhq.timestamp": "1650000000",
	}
	_, err := normalizeRecord(rec, nil)
	if err == nil {
		t.Fatalf("expected error for unexpected type, got nil")
	}
	// don't assert exact message text; just make sure it mentions 'unexpected'
	if !strings.Contains(err.Error(), "unexpected _cardinalhq.timestamp type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeRecord_DropSetEmptyIsNoOp(t *testing.T) {
	rec := map[string]any{
		"_cardinalhq.timestamp": int64(1),
		"minute":                59, // should remain because drop set is empty
	}
	out, err := normalizeRecord(rec, make(map[string]struct{})) // empty drop set
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := out["minute"]; !ok {
		t.Fatalf("'minute' should not be deleted when drop set is empty")
	}
}

//
// copyAll tests
//

func TestCopyAll_WritesAndNormalizes(t *testing.T) {
	ctx := context.Background()

	// Build a nodes map that includes droppable fields so computeDropSet() finds them.
	builder := buffet.NewNodeMapBuilder()
	err := builder.Add(map[string]any{
		"_cardinalhq.timestamp": int64(0),
		"minute":                int32(0),
		"hour":                  int32(0),
		"scope.keep":            "x",
	})
	assert.NoError(t, err)
	nodes := builder.Build()

	handles := []*filecrunch.FileHandle{
		{File: &os.File{}, Schema: &parquet.Schema{}, Nodes: nodes},
		{File: &os.File{}, Schema: &parquet.Schema{}, Nodes: nodes},
	}

	// Readers: first yields 2 rows, second yields 1 row.
	r1 := &fakeBatchReader{
		batches: [][]map[string]any{
			{
				{"_cardinalhq.timestamp": int64(101), "minute": 9, "hour": 1, "scope.keep": "a"},
				{"_cardinalhq.timestamp": int32(102), "minute": 10, "scope.keep": "b"},
			},
		},
	}
	r2 := &fakeBatchReader{
		batches: [][]map[string]any{
			{
				{"_cardinalhq.timestamp": float64(103.7), "scope.keep": "c"},
			},
		},
	}

	open := &fakeFileOpenerWithReaders{
		readers: []GenericMapReader{r1, r2},
		nodes:   nodes,
	}
	out := &fakeWriter{}

	total, err := copyAll(ctx, open, out, handles)
	if err != nil {
		t.Fatalf("copyAll error: %v", err)
	}
	if total != 3 {
		t.Fatalf("want 3 rows copied, got %d", total)
	}
	if len(out.recs) != 3 {
		t.Fatalf("writer saw %d rows, want 3", len(out.recs))
	}

	// Assert normalization per row
	for i, rec := range out.recs {
		if _, ok := rec["_cardinalhq.timestamp"]; !ok {
			t.Fatalf("row %d missing timestamp", i)
		}
		if _, ok := rec["minute"]; ok {
			t.Fatalf("row %d still has 'minute'", i)
		}
		if _, ok := rec["hour"]; ok {
			t.Fatalf("row %d still has 'hour'", i)
		}
		if _, ok := rec["scope.keep"]; !ok {
			t.Fatalf("row %d missing 'scope.keep'", i)
		}
		if _, ok := rec["_cardinalhq.timestamp"].(int64); !ok {
			t.Fatalf("row %d timestamp not coerced to int64: %#v", i, rec["_cardinalhq.timestamp"])
		}
	}
}

func TestCopyAll_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	nodes := buffet.NewNodeMapBuilder().Build()
	h := &filecrunch.FileHandle{File: &os.File{}, Schema: &parquet.Schema{}, Nodes: nodes}

	open := &fakeFileOpenerWithReaders{
		readers: []GenericMapReader{&fakeBatchReader{batches: nil}}, // would EOF; canceled first
		nodes:   nodes,
	}
	out := &fakeWriter{}

	_, err := copyAll(ctx, open, out, []*filecrunch.FileHandle{h})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if len(out.recs) != 0 {
		t.Fatalf("writer should see 0 rows on cancel, saw %d", len(out.recs))
	}
}

func TestCopyAll_ReaderErrorBubblesAfterSomeRows(t *testing.T) {
	ctx := context.Background()

	nodes := buffet.NewNodeMapBuilder().Build()
	h := &filecrunch.FileHandle{File: &os.File{}, Schema: &parquet.Schema{}, Nodes: nodes}

	rErr := errors.New("read boom")
	r := &fakeBatchReader{
		batches: [][]map[string]any{
			{{"_cardinalhq.timestamp": int64(1)}}, // first batch OK
		},
		err: rErr, // then fail once
	}

	open := &fakeFileOpenerWithReaders{
		readers: []GenericMapReader{r},
		nodes:   nodes,
	}
	out := &fakeWriter{}

	total, err := copyAll(ctx, open, out, []*filecrunch.FileHandle{h})
	if err == nil {
		t.Fatalf("want error from reader, got nil")
	}
	// We expect 1 row to have been written before the error surfaced.
	if total != 1 {
		t.Fatalf("want 1 row written before error, got %d", total)
	}
	if len(out.recs) != 1 {
		t.Fatalf("writer should have 1 row before error, got %d", len(out.recs))
	}
}

//
// statsFor
//

func TestStatsFor_Empty(t *testing.T) {
	got := statsFor(nil)
	if got.CountRecords != 0 || got.SizeBytes != 0 || got.FirstTS != 0 || got.LastTS != 0 || got.IngestDate != 0 {
		t.Fatalf("empty input -> zero stats, got %+v", got)
	}

	got = statsFor([]lrdb.GetLogSegmentsForCompactionRow{})
	if got.CountRecords != 0 || got.SizeBytes != 0 || got.FirstTS != 0 || got.LastTS != 0 || got.IngestDate != 0 {
		t.Fatalf("empty slice -> zero stats, got %+v", got)
	}
}

func TestStatsFor_Single(t *testing.T) {
	seg := lrdb.GetLogSegmentsForCompactionRow{
		SegmentID:     1,
		StartTs:       mustParseMs("2025-01-02T00:00:00Z"),
		EndTs:         mustParseMs("2025-01-02T00:05:00Z"),
		FileSize:      111,
		RecordCount:   22,
		IngestDateint: 20250102,
	}
	got := statsFor([]lrdb.GetLogSegmentsForCompactionRow{seg})

	if got.CountRecords != 22 {
		t.Fatalf("CountRecords=%d, want 22", got.CountRecords)
	}
	if got.SizeBytes != 111 {
		t.Fatalf("SizeBytes=%d, want 111", got.SizeBytes)
	}
	if got.FirstTS != seg.StartTs {
		t.Fatalf("FirstTS=%d, want %d", got.FirstTS, seg.StartTs)
	}
	if got.LastTS != seg.EndTs {
		t.Fatalf("LastTS=%d, want %d", got.LastTS, seg.EndTs)
	}
	if got.IngestDate != seg.IngestDateint {
		t.Fatalf("IngestDate=%d, want %d", got.IngestDate, seg.IngestDateint)
	}
}

func TestStatsFor_MultipleUnordered(t *testing.T) {
	// Deliberately out of order, overlapping, and different ingest dates
	a := lrdb.GetLogSegmentsForCompactionRow{
		SegmentID:     10,
		StartTs:       mustParseMs("2025-01-02T01:00:00Z"),
		EndTs:         mustParseMs("2025-01-02T01:10:00Z"),
		FileSize:      100,
		RecordCount:   1,
		IngestDateint: 20250101,
	}
	b := lrdb.GetLogSegmentsForCompactionRow{
		SegmentID:     11,
		StartTs:       mustParseMs("2025-01-02T00:30:00Z"), // earliest start
		EndTs:         mustParseMs("2025-01-02T00:45:00Z"),
		FileSize:      200,
		RecordCount:   2,
		IngestDateint: 20250102,
	}
	c := lrdb.GetLogSegmentsForCompactionRow{
		SegmentID:     12,
		StartTs:       mustParseMs("2025-01-02T02:00:00Z"),
		EndTs:         mustParseMs("2025-01-02T02:30:00Z"), // latest end
		FileSize:      300,
		RecordCount:   3,
		IngestDateint: 20250103, // max ingest date
	}

	got := statsFor([]lrdb.GetLogSegmentsForCompactionRow{a, b, c})

	if got.CountRecords != (1 + 2 + 3) {
		t.Fatalf("CountRecords=%d, want 6", got.CountRecords)
	}
	if got.SizeBytes != (100 + 200 + 300) {
		t.Fatalf("SizeBytes=%d, want 600", got.SizeBytes)
	}
	if got.FirstTS != b.StartTs {
		t.Fatalf("FirstTS=%d, want earliest %d", got.FirstTS, b.StartTs)
	}
	if got.LastTS != c.EndTs {
		t.Fatalf("LastTS=%d, want latest %d", got.LastTS, c.EndTs)
	}
	if got.IngestDate != 20250103 {
		t.Fatalf("IngestDate=%d, want 20250103", got.IngestDate)
	}
}
