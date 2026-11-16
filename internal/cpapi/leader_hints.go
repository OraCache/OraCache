package cpapi

import (
	"context"
	"sort"

	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/leaderhint"
)

// Only eligible leader self-reports affect routing. Other validated heartbeats
// remain diagnostic telemetry. The static config revision never changes here.
func (s *Server) observeLeaders(ctx context.Context, req *pb.HeartbeatRequest) {
	snapshot, err := s.store.Config(ctx)
	if err != nil || snapshot.Config == nil {
		return
	}
	s.hintsMu.Lock()
	defer s.hintsMu.Unlock()
	if s.hints == nil {
		s.hints = make(map[leaderhint.Key]leaderhint.Observation)
	}
	for _, p := range req.Partitions {
		k := leaderhint.Key{Region: req.Region, Partition: p.PartitionId}
		if p.Role != pb.RaftRole_RAFT_ROLE_LEADER || !p.Ready || p.LeaderId != req.NodeId || p.Term == 0 || !leaderhint.Eligible(snapshot.Config, k, req.NodeId) {
			continue
		}
		s.hints[k] = leaderhint.Merge(s.hints[k], leaderhint.Observation{NodeID: req.NodeId, Term: p.Term})
	}
}

// CP restart intentionally loses these observations. Gateways keep their own
// term high-water marks; an empty CP response does not revoke static replicas.
func (s *Server) leaderHints(cfg *cpconfig.Config) []*pb.LeaderHint {
	s.hintsMu.Lock()
	defer s.hintsMu.Unlock()
	var out []*pb.LeaderHint
	for k, h := range s.hints {
		if _, ok := cfg.Assignment(k.Region, k.Partition); !ok {
			continue
		}
		if h.NodeID != "" && !leaderhint.Eligible(cfg, k, h.NodeID) {
			continue
		}
		out = append(out, &pb.LeaderHint{Region: k.Region, PartitionId: k.Partition, NodeId: h.NodeID, Term: h.Term})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Region != out[j].Region {
			return out[i].Region < out[j].Region
		}
		return out[i].PartitionId < out[j].PartitionId
	})
	return out
}
