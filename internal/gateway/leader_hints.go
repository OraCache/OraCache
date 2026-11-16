package gateway

import (
	"fmt"

	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/leaderhint"
)

// ApplyLeaderHints merges term-ordered observations independently of config
// revision. Empty batches do nothing (including after a CP restart). Invalid
// batches are rejected atomically; no address is ever accepted from a hint.
func (r *Router) ApplyLeaderHints(hints []*pb.LeaderHint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	if old == nil {
		return ErrNoConfig
	}
	if err := validateHints(old.config, hints); err != nil {
		return err
	}
	next := *old
	next.hints = mergeHints(old.hints, hints)
	r.cur.Store(&next)
	return nil
}

func validateHints(cfg *cpconfig.Config, hints []*pb.LeaderHint) error {
	for _, h := range hints {
		if h == nil || h.Term == 0 {
			return fmt.Errorf("gateway: invalid leader hint")
		}
		k := leaderhint.Key{Region: h.Region, Partition: h.PartitionId}
		if _, ok := cfg.Assignment(k.Region, k.Partition); !ok {
			return fmt.Errorf("gateway: unknown hint assignment")
		}
		if h.NodeId != "" && !leaderhint.Eligible(cfg, k, h.NodeId) {
			return fmt.Errorf("gateway: hint node is not a configured regional replica")
		}
	}
	return nil
}

func mergeHints(old map[leaderhint.Key]leaderhint.Observation, hints []*pb.LeaderHint) map[leaderhint.Key]leaderhint.Observation {
	next := make(map[leaderhint.Key]leaderhint.Observation, len(old)+len(hints))
	for k, h := range old {
		next[k] = h
	}
	for _, h := range hints {
		k := leaderhint.Key{Region: h.Region, Partition: h.PartitionId}
		next[k] = leaderhint.Merge(next[k], leaderhint.Observation{NodeID: h.NodeId, Term: h.Term})
	}
	return next
}

func hintedLeader(s *snapshot, routes map[uint32]Leader, replicas map[uint32][]Leader, partition uint32) (Leader, error) {
	fallback, err := routeFor(routes, partition, s.partitionCount)
	if err != nil {
		return Leader{}, err
	}
	h := s.hints[leaderhint.Key{Region: fallback.Region, Partition: partition}]
	// Revalidate against this snapshot: config updates cannot resurrect removed
	// replicas merely because a high-water mark is retained.
	for _, n := range replicas[partition] {
		if h.NodeID != "" && n.NodeID == h.NodeID {
			return n, nil
		}
	}
	return fallback, nil
}
