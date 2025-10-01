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
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

// ----------------------------------------------------------------------------
// internal cluster & leaf clusterer
// ----------------------------------------------------------------------------

type cluster struct {
	Fingerprint int64               `json:"fp"`
	TokenSet    map[string]struct{} `json:"tokens"`
	MatchCount  int                 `json:"mc"`
	Total       int                 `json:"tot"`
	LastUpdated time.Time           `json:"upd"`
}

func (c *cluster) matchRate() float64 {
	if c.Total == 0 {
		return 0
	}
	return float64(c.MatchCount) / float64(c.Total)
}

func (c *cluster) record(incoming map[string]struct{}, matched bool) {
	if matched {
		// intersect token set
		common := getStringSet()
		for tok := range c.TokenSet {
			if _, ok := incoming[tok]; ok {
				common[tok] = struct{}{}
			}
		}
		if c.TokenSet != nil {
			putStringSet(c.TokenSet)
		}
		c.TokenSet = common
		c.MatchCount++
	}
	c.Total++
	c.LastUpdated = time.Now()
}

// leafClusterer keeps ordered clusters for one token-prefix node.
type leafClusterer struct {
	threshold float64
	mu        sync.Mutex
	clusters  []*cluster // descending by match rate
}

func newLeafClusterer(threshold float64) *leafClusterer {
	return &leafClusterer{
		threshold: threshold,
		clusters:  make([]*cluster, 0),
	}
}

// Add returns either an existing cluster fingerprint (if jaccard >= threshold)
// or computes & returns a new one.
func (lc *leafClusterer) add(ts *tokenSeq) int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	// build incoming set
	incoming := getStringSet()
	defer putStringSet(incoming)
	for _, tok := range ts.items {
		incoming[tok] = struct{}{}
	}

	// try to match
	for idx, cl := range lc.clusters {
		// Jaccard
		score := jaccardSimilarity(cl.TokenSet, incoming)

		cl.record(incoming, score >= lc.threshold)
		if score >= lc.threshold {
			// bubble up
			for i := idx; i > 0 && lc.clusters[i].matchRate() > lc.clusters[i-1].matchRate(); i-- {
				lc.clusters[i], lc.clusters[i-1] = lc.clusters[i-1], lc.clusters[i]
			}
			return cl.Fingerprint
		}
	}

	// no match → new fingerprint
	newFP := fingerprintItemsAndJSONKeys(ts)

	// copy incoming to a new pooled set since we're keeping it
	tokenSet := getStringSet()
	for tok := range incoming {
		tokenSet[tok] = struct{}{}
	}

	cl := getCluster()
	cl.Fingerprint = newFP
	cl.TokenSet = tokenSet
	cl.MatchCount = 1
	cl.Total = 1
	cl.LastUpdated = time.Now()
	lc.clusters = append([]*cluster{cl}, lc.clusters...)
	return newFP
}

// ----------------------------------------------------------------------------
// trie + manager + serialization
// ----------------------------------------------------------------------------

type seqNode struct {
	children map[string]*seqNode
	leaf     *leafClusterer
	sync.RWMutex
}

func newSeqNode() *seqNode {
	return &seqNode{children: make(map[string]*seqNode)}
}

// TrieClusterManager maps full token-prefixes to per-node leafClusterers.
type TrieClusterManager struct {
	root      *seqNode
	threshold float64
}

func NewTrieClusterManager(threshold float64) *TrieClusterManager {
	return &TrieClusterManager{
		root:      newSeqNode(),
		threshold: threshold,
	}
}

func (m *TrieClusterManager) getOrCreateLeaf(n *seqNode) *leafClusterer {
	n.Lock()
	defer n.Unlock()

	if n.leaf == nil {
		n.leaf = newLeafClusterer(m.threshold)
	}
	return n.leaf
}

func collectLeafers(n *seqNode) []*leafClusterer {
	var out []*leafClusterer
	children := getSeqNodeSlice()
	defer putSeqNodeSlice(children)

	var dfs func(x *seqNode)
	dfs = func(x *seqNode) {
		x.RLock()
		if x.leaf != nil {
			out = append(out, x.leaf)
		}
		// snapshot children pointers under the same lock
		children = children[:0] // reset for reuse
		for _, c := range x.children {
			children = append(children, c)
		}
		x.RUnlock()

		// recurse after unlocking
		for _, c := range children {
			dfs(c)
		}
	}
	dfs(n)
	return out
}

// jaccardSimilarity computes jaccard similarity between two token sets
func jaccardSimilarity(set1, set2 map[string]struct{}) float64 {
	inter := 0
	for tok := range set1 {
		if _, ok := set2[tok]; ok {
			inter++
		}
	}
	union := len(set1) + len(set2) - inter
	if union == 0 {
		return 0.0
	}
	return float64(inter) / float64(union)
}

// Cluster walks as far down ts.Items as possible in the trie.
// If it consumes all tokens → exact leaf.Add.
// On divergence, it Jaccard-scans every cluster under the current subtree,
// picks the best above threshold, or else creates a new branch + leaf.
func (m *TrieClusterManager) cluster(ts *tokenSeq) int64 {
	cur := m.root
	i := 0
	for ; i < len(ts.items); i++ {
		cur.RLock()
		nxt, ok := cur.children[ts.items[i]]
		cur.RUnlock()
		if !ok {
			break
		}
		cur = nxt
	}

	// exact
	if i == len(ts.items) {
		return m.getOrCreateLeaf(cur).add(ts)
	}

	// divergence → scan under cur
	incoming := getStringSet()
	defer putStringSet(incoming)
	for _, tok := range ts.items {
		incoming[tok] = struct{}{}
	}

	bestScore := -1.0
	var bestCl *cluster
	var bestLeaf *leafClusterer
	for _, leaf := range collectLeafers(cur) {
		leaf.mu.Lock()
		for _, cl := range leaf.clusters {
			score := jaccardSimilarity(cl.TokenSet, incoming)
			if score >= leaf.threshold && score > bestScore {
				bestScore = score
				bestCl = cl
				bestLeaf = leaf
			}
		}
		leaf.mu.Unlock()
	}

	if bestLeaf != nil && bestCl != nil {
		bestLeaf.mu.Lock()
		bestCl.record(incoming, true)
		bestLeaf.mu.Unlock()
		return bestCl.Fingerprint
	}

	// no match → carve out the remainder
	for ; i < len(ts.items); i++ {
		n := newSeqNode()
		cur.Lock()
		cur.children[ts.items[i]] = n
		cur.Unlock()
		cur = n
	}
	return m.getOrCreateLeaf(cur).add(ts)
}

func fingerprintItemsAndJSONKeys(t *tokenSeq) int64 {
	h := xxhash.New()
	for i, item := range t.items {
		if i > 0 {
			_, _ = h.Write([]byte(":"))
		}
		_, _ = h.WriteString(item)
	}
	for _, key := range t.jsonKeys {
		_, _ = h.Write([]byte(":"))
		_, _ = h.WriteString(key)
	}
	return int64(h.Sum64())
}
