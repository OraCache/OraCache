// Package leaderhint implements advisory, RAM-only Raft term high-water marks.
// It has no clock, address catalog, membership authority or persistence.
package leaderhint

import "github.com/OraCache/oracache/internal/cpconfig"

type Key struct {
	Region    string
	Partition uint32
}

type Observation struct {
	NodeID string // empty is a sticky same-term conflict
	Term   uint64
}

// Merge is monotonic in term. Conflicts cannot be undone at the same term,
// including by delayed responses from a CP that has lost its RAM state.
func Merge(old, next Observation) Observation {
	if next.Term < old.Term {
		return old
	}
	if next.Term > old.Term {
		return next
	}
	if old.NodeID != next.NodeID {
		old.NodeID = ""
	}
	return old
}

// Eligible resolves identity strictly through a configured regional assignment.
func Eligible(cfg *cpconfig.Config, k Key, nodeID string) bool {
	a, ok := cfg.Assignment(k.Region, k.Partition)
	if !ok {
		return false
	}
	n, ok := cfg.NodeByID(nodeID)
	if !ok || n.Region != k.Region {
		return false
	}
	for _, id := range a.ReplicaNodeIDs {
		if id == nodeID {
			return true
		}
	}
	return false
}
