package gateway

import (
	"fmt"
	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/discovery"
	"sort"
	"time"
)

// ApplyDiscovery replaces a full PoC observation snapshot. started is the
// local request start, not response receipt: network time cannot extend leases.
func (r *Router) ApplyDiscovery(d *pb.DiscoverySnapshot, revision uint64, started time.Time) error {
	if d == nil || d.Region != r.region || d.GroupId == "" || revision == 0 || started.IsZero() {
		return fmt.Errorf("gateway: invalid discovery snapshot")
	}
	s := &snapshot{revision: revision, partitionCount: 1, masterRegion: d.Region, discovery: true, local: map[uint32]Leader{}, write: map[uint32]Leader{}, localReplicas: map[uint32][]Leader{}, writeReplicas: map[uint32][]Leader{}}
	nodes := map[string]Leader{}
	addresses := map[string]bool{}
	var maxTerm uint64
	for _, o := range d.Observations {
		if o == nil || o.Heartbeat == nil || o.RemainingMs > uint64(discovery.MaxTTL/time.Millisecond) {
			return fmt.Errorf("gateway: invalid discovery lease")
		}
		h := o.Heartbeat
		if err := discovery.Validate(h, d.Region, d.GroupId); err != nil {
			return err
		}
		p := h.Partitions[0]
		if !p.Ready || p.Draining || o.RemainingMs == 0 {
			return fmt.Errorf("gateway: ineligible observation")
		}
		if _, ok := nodes[h.NodeId]; ok || addresses[h.ClientAddress] {
			return fmt.Errorf("gateway: duplicate discovery node or address")
		}
		nodes[h.NodeId] = Leader{NodeID: h.NodeId, Address: h.ClientAddress, Region: h.Region, ExpiresAt: started.Add(time.Duration(o.RemainingMs) * time.Millisecond)}
		addresses[h.ClientAddress] = true
		if p.Term > maxTerm {
			maxTerm = p.Term
		}
	}
	// Only agreeing reports at the highest observed term choose the initial target.
	leader := ""
	conflict := false
	for _, o := range d.Observations {
		p := o.Heartbeat.Partitions[0]
		if p.Term == maxTerm && p.LeaderId != "" {
			if leader != "" && leader != p.LeaderId {
				conflict = true
			}
			leader = p.LeaderId
		}
	}
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if !conflict {
		for i, id := range ids {
			if id == leader {
				ids = append([]string{id}, append(ids[:i], ids[i+1:]...)...)
				break
			}
		}
	}
	for _, id := range ids {
		s.localReplicas[0] = append(s.localReplicas[0], nodes[id])
	}
	s.writeReplicas[0] = s.localReplicas[0]
	r.mu.Lock()
	defer r.mu.Unlock()
	if old := r.cur.Load(); old != nil {
		if revision <= old.revision {
			return ErrStaleRevision
		}
		if old.partitionCount != 1 {
			return ErrPartitionCountChanged
		}
	}
	r.cur.Store(s)
	return nil
}
func live(l Leader) bool           { return l.ExpiresAt.IsZero() || time.Now().Before(l.ExpiresAt) }
func (r *Router) discovered() bool { s := r.cur.Load(); return s != nil && s.discovery }
func (r *Router) eligible(partition uint32, write bool, addr string) bool {
	if !r.discovered() {
		return true
	} // legacy static mode retains its existing hint contract
	rs, err := r.Replicas(partition, write)
	if err != nil {
		return false
	}
	for _, r := range rs {
		if r.Address == addr {
			return true
		}
	}
	return false
}
func snapshotLeader(s *snapshot, p uint32, write bool) (Leader, error) {
	if !s.discovery {
		if write {
			return routeFor(s.write, p, s.partitionCount)
		}
		return routeFor(s.local, p, s.partitionCount)
	}
	if p >= s.partitionCount {
		return Leader{}, ErrPartitionOutOfRange
	}
	rs := s.localReplicas
	if write {
		rs = s.writeReplicas
	}
	for _, l := range rs[p] {
		if live(l) {
			return l, nil
		}
	}
	return Leader{}, ErrNoLeader
}
