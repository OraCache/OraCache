// Package discovery implements only the trusted-network, single-group PoC
// observation catalog. It has no access to Raft or its membership API.
package discovery

import (
	"fmt"
	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"google.golang.org/protobuf/proto"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultTTL = 20 * time.Second

// MaxTTL bounds leases accepted by both CP and gateway.
const MaxTTL = 24 * time.Hour
const MaxNodes = 1024

type record struct {
	heartbeat *pb.HeartbeatRequest
	received  time.Time
}
type Catalog struct {
	mu            sync.Mutex
	region, group string
	ttl           time.Duration
	records       map[string]record
	revision      uint64
}

func New(region, group string, ttl time.Duration) (*Catalog, error) {
	if !validID(region) || !validID(group) || ttl < time.Millisecond || ttl > MaxTTL {
		return nil, fmt.Errorf("discovery: invalid region, group or TTL")
	}
	return &Catalog{region: region, group: group, ttl: ttl, records: map[string]record{}}, nil
}
func validID(s string) bool {
	return len(s) > 0 && len(s) <= 256 && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\x00\r\n\t ")
}

// Validate checks declarations, not authenticity or actual group membership.
func Validate(h *pb.HeartbeatRequest, region, group string) error {
	if h == nil || !validID(h.NodeId) || h.Region != region || len(h.Instance) > 256 || h.Sequence == 0 {
		return fmt.Errorf("discovery: invalid node identity or sequence")
	}
	host, port, err := net.SplitHostPort(h.ClientAddress)
	p, e := strconv.Atoi(port)
	if err != nil || host == "" || len(h.ClientAddress) > 512 || strings.ContainsAny(host, " /\t\r\n\x00") || e != nil || p < 1 || p > 65535 {
		return fmt.Errorf("discovery: invalid client host:port")
	}
	if len(h.Partitions) != 1 || h.Partitions[0] == nil {
		return fmt.Errorf("discovery: exactly one partition required")
	}
	s := h.Partitions[0]
	if s.PartitionId != 0 || s.GroupId != group || len(s.LastError) > 1024 || s.Role < pb.RaftRole_RAFT_ROLE_UNSPECIFIED || s.Role > pb.RaftRole_RAFT_ROLE_LEARNER {
		return fmt.Errorf("discovery: invalid partition, group or role")
	}
	if s.LeaderId != "" && !validID(s.LeaderId) {
		return fmt.Errorf("discovery: invalid leader ID")
	}
	if s.Role == pb.RaftRole_RAFT_ROLE_LEADER && (s.LeaderId != h.NodeId || s.Term == 0) {
		return fmt.Errorf("discovery: inconsistent leader report")
	}
	return nil
}
func (c *Catalog) Report(h *pb.HeartbeatRequest, now time.Time) error {
	if err := Validate(h, c.region, c.group); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.records[h.NodeId]; ok {
		if h.Sequence <= old.heartbeat.Sequence || h.Partitions[0].Term < old.heartbeat.Partitions[0].Term {
			return fmt.Errorf("discovery: reordered sequence or regressed term")
		}
	} else if len(c.records) >= MaxNodes {
		return fmt.Errorf("discovery: catalog full")
	}
	c.records[h.NodeId] = record{proto.Clone(h).(*pb.HeartbeatRequest), now}
	return nil
}

// Snapshot returns eligible reports and monotonically ordered full snapshots.
// High-water marks remain after expiry; expiry never changes membership.
func (c *Catalog) Snapshot(now time.Time) (*pb.DiscoverySnapshot, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revision++
	out := &pb.DiscoverySnapshot{Region: c.region, GroupId: c.group}
	ids := make([]string, 0, len(c.records))
	for id := range c.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := c.records[id]
		s := r.heartbeat.Partitions[0]
		remaining := r.received.Add(c.ttl).Sub(now)
		if remaining < time.Millisecond || !s.Ready || s.Draining {
			continue
		}
		out.Observations = append(out.Observations, &pb.DiscoveryObservation{Heartbeat: proto.Clone(r.heartbeat).(*pb.HeartbeatRequest), RemainingMs: uint64(remaining / time.Millisecond)})
	}
	return out, c.revision
}
