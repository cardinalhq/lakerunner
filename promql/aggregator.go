package promql

import (
	"errors"
	"fmt"
	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/store"
	"lakequery/promql/planner"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/axiomhq/hyperloglog"
)

// ---- Helpers you likely already have elsewhere ----

// SUM/COUNT/MIN/MAX keys for MAP sketches:
const (
	SUM   = "sum"
	COUNT = "count"
	MIN   = "min"
	MAX   = "max"
)

// If you already have encode/decode utilities, use those and delete these.
func decodeHLL(b []byte) (*hyperloglog.Sketch, error) {
	if len(b) == 0 {
		return nil, errors.New("empty HLL bytes")
	}
	var h hyperloglog.Sketch
	if err := h.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return &h, nil
}
func encodeHLL(h *hyperloglog.Sketch) ([]byte, error) {
	return h.MarshalBinary()
}

func decodeDDS(b []byte, m mapping.IndexMapping) (*ddsketch.DDSketch, error) {
	sk, err := ddsketch.DecodeDDSketch(b, store.DefaultProvider, m)
	if err != nil {
		return nil, err
	}
	return sk, nil
}

func encodeDDS(d *ddsketch.DDSketch) []byte {
	var buf []byte
	d.Encode(&buf, false)
	return buf
}

func tagsKey(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	ks := slices.Collect(maps.Keys(m))
	slices.Sort(ks)
	var b strings.Builder
	for i, k := range ks {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		// store as strings; if you have numbers/booleans, format them explicitly
		b.WriteString(fmt.Sprint(m[k]))
	}
	return b.String()
}

// ---- Mergers ----

type sketchMerger interface {
	merge(planner.SketchInput, mapping.IndexMapping)
	dataPoints() []planner.SketchInput
}

type simpleSketchMerger struct {
	init planner.SketchInput
	// merged state
	hll *hyperloglog.Sketch
	dds *ddsketch.DDSketch
	agg map[string]float64
}

func newSimpleSketchMerger(init planner.SketchInput, mapping mapping.IndexMapping) *simpleSketchMerger {
	sm := &simpleSketchMerger{init: init}
	switch init.SketchTags.SketchType {
	case planner.SketchHLL:
		if len(init.SketchTags.Bytes) > 0 {
			if h, err := decodeHLL(init.SketchTags.Bytes); err == nil {
				sm.hll = h
			}
		}
	case planner.SketchDDS:
		if len(init.SketchTags.Bytes) > 0 {
			if d, err := decodeDDS(init.SketchTags.Bytes, mapping); err == nil {
				sm.dds = d
			}
		}
	case planner.SketchMAP:
		cp := make(map[string]float64, len(init.SketchTags.Agg))
		for k, v := range init.SketchTags.Agg {
			cp[k] = v
		}
		sm.agg = cp
	}
	return sm
}

func (m *simpleSketchMerger) merge(si planner.SketchInput, indexMapping mapping.IndexMapping) {
	switch si.SketchTags.SketchType {
	case planner.SketchHLL:
		in, err := decodeHLL(si.SketchTags.Bytes)
		if err != nil || in == nil {
			return
		}
		if m.hll == nil {
			m.hll = hyperloglog.New14()
		}
		_ = m.hll.Merge(in)

	case planner.SketchDDS:
		in, err := decodeDDS(si.SketchTags.Bytes, indexMapping)
		if err != nil || in == nil {
			return
		}
		if m.dds == nil {
			if d, e := ddsketch.NewDefaultDDSketch(0.01); e == nil {
				m.dds = d
			} else {
				return
			}
		}
		_ = m.dds.MergeWith(in)

	case planner.SketchMAP:
		if m.agg == nil {
			m.agg = map[string]float64{}
		}
		for k, v := range si.SketchTags.Agg {
			switch k {
			case SUM, COUNT:
				m.agg[k] += v
			case MIN:
				if cur, ok := m.agg[MIN]; !ok || v < cur {
					m.agg[MIN] = v
				}
			case MAX:
				if cur, ok := m.agg[MAX]; !ok || v > cur {
					m.agg[MAX] = v
				}
			default:
				// ignore other keys for now
			}
		}
	}
}

func (m *simpleSketchMerger) dataPoints() []planner.SketchInput {
	out := m.init // copy
	switch m.init.SketchTags.SketchType {
	case planner.SketchHLL:
		if m.hll != nil {
			if b, err := encodeHLL(m.hll); err == nil {
				out.SketchTags.Bytes = b
			}
		}
	case planner.SketchDDS:
		if m.dds != nil {
			out.SketchTags.Bytes = encodeDDS(m.dds)
		}
	case planner.SketchMAP:
		if m.agg != nil {
			out.SketchTags.Agg = maps.Clone(m.agg)
		}
	}
	return []planner.SketchInput{out}
}

type groupBySketchMerger struct {
	byTags map[string]*simpleSketchMerger // key = stable tagsKey
}

func newGroupBySketchMerger() *groupBySketchMerger {
	return &groupBySketchMerger{byTags: map[string]*simpleSketchMerger{}}
}

func (g *groupBySketchMerger) merge(si planner.SketchInput, indexMapping mapping.IndexMapping) {
	key := tagsKey(si.SketchTags.Tags)
	if acc, ok := g.byTags[key]; ok {
		acc.merge(si, indexMapping)
		return
	}
	acc := newSimpleSketchMerger(si, indexMapping)
	g.byTags[key] = acc
}

func (g *groupBySketchMerger) dataPoints() []planner.SketchInput {
	var out []planner.SketchInput
	for _, acc := range g.byTags {
		out = append(out, acc.dataPoints()...)
	}
	return out
}

// ---- TimeGroupedSketchAggregator ----

type BaseExprLookup func(si planner.SketchInput) (planner.BaseExpr, bool)

// TimeGroupedSketchAggregator groups by timestamp across a small ring of buffers,
// and within each time bucket it groups by BaseExpr.ID, merging compatible sketches.
// When a new timestamp bumps an occupied slot, the completed group is flushed.
type TimeGroupedSketchAggregator struct {
	mapping    mapping.IndexMapping
	mu         sync.Mutex
	numBuf     int
	buffers    []map[string]sketchMerger // per time-slot: by BaseExpr.ID
	timestamps []int64
	cutoff     int64
	lookup     BaseExprLookup
}

func NewTimeGroupedSketchAggregator(numBuffers int, lookup BaseExprLookup) *TimeGroupedSketchAggregator {
	if numBuffers < 2 {
		numBuffers = 2
	}
	bufs := make([]map[string]sketchMerger, numBuffers)
	ts := make([]int64, numBuffers)
	for i := range bufs {
		bufs[i] = map[string]sketchMerger{}
	}
	m, err := mapping.NewLogarithmicMapping(0.01)
	if err != nil {
		return nil
	}
	return &TimeGroupedSketchAggregator{
		mapping:    m,
		numBuf:     numBuffers,
		buffers:    bufs,
		timestamps: ts,
		lookup:     lookup,
	}
}

// findBuffer returns index of existing slot for t, or a negative insertion point
// (-pos-1) for the least-recent slot to be flushed/overwritten.
func (a *TimeGroupedSketchAggregator) findBuffer(t int64) int {
	// Keep slots roughly sorted; pick the "oldest" to evict.
	minIdx := 0
	for i := 0; i < len(a.timestamps); i++ {
		if a.timestamps[i] == t {
			return i
		}
		if i > 0 && a.timestamps[i] < a.timestamps[i-1] {
			minIdx = i
		}
	}
	return -minIdx - 1
}

func (a *TimeGroupedSketchAggregator) flush(i int) planner.SketchGroup {
	t := a.timestamps[i]
	grp := planner.SketchGroup{
		Timestamp: t,
		Group:     map[string][]planner.SketchInput{},
	}
	for beid, merger := range a.buffers[i] {
		grp.Group[beid] = merger.dataPoints()
	}
	a.cutoff = t
	a.buffers[i] = map[string]sketchMerger{}
	a.timestamps[i] = 0
	return grp
}

// AddBatch ingests a batch and returns any completed time-groups that got flushed.
// You can call this repeatedly as you stream data in order.
func (a *TimeGroupedSketchAggregator) AddBatch(in []planner.SketchInput) (out []planner.SketchGroup) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, si := range in {
		t := si.Timestamp
		if t <= a.cutoff {
			// Drop late data; metrics omitted for brevity
			continue
		}

		slot := a.findBuffer(t)
		if slot >= 0 {
			a.aggregate(slot, si)
			continue
		}
		// rotate
		pos := -slot - 1
		if a.timestamps[pos] > 0 {
			flushed := a.flush(pos)
			if flushed.Timestamp > 0 {
				out = append(out, flushed)
			}
		}
		a.aggregate(pos, si)
		a.timestamps[pos] = t
	}
	return out
}

// FlushAll flushes all non-empty buffers (end-of-stream).
func (a *TimeGroupedSketchAggregator) FlushAll() (out []planner.SketchGroup) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i := range a.buffers {
		if a.timestamps[i] > 0 {
			out = append(out, a.flush(i))
		}
	}
	// Keep a deterministic order
	slices.SortFunc(out, func(x, y planner.SketchGroup) int {
		switch {
		case x.Timestamp < y.Timestamp:
			return -1
		case x.Timestamp > y.Timestamp:
			return 1
		default:
			return 0
		}
	})
	return out
}

func (a *TimeGroupedSketchAggregator) aggregate(i int, si planner.SketchInput) {
	be, ok := a.lookup(si)
	if !ok {
		// unknown base expr → drop or log
		return
	}
	beid := be.ID

	// Choose merger strategy. If the BaseExpr has explicit GroupBy (non-HLL/MAP nuance),
	// we preserve separate tag buckets via GroupBySketchMerger; otherwise we collapse.
	mergers := a.buffers[i]
	mer, ok := mergers[beid]
	if !ok {
		if len(be.GroupBy) > 0 {
			// HLL: we generally want to union across all tag buckets for the same group key
			// (your pipeline already applied the grouping; if you do need per-tag buckets
			// for HLL too, switch to GroupBySketchMerger here).
			gb := newGroupBySketchMerger()
			gb.merge(si, a.mapping)
			mergers[beid] = gb
			return
		}
		sm := newSimpleSketchMerger(si, a.mapping)
		mergers[beid] = sm
		return
	}
	mer.merge(si, a.mapping)
}
