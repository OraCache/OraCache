// Package gateway implements the target stateless API Gateway routing layer.
//
// It is intentionally separate from the legacy local cache in internal/cache.
// Router holds only the last-known-good Control Plane snapshot: it computes a
// fixed partition for a key and resolves the local-region leader (GET) or the
// master-region leader (PUT). It never stores cache data.
package gateway

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/partition"
)

// Routing errors. Callers use errors.Is, never error text.
var (
	ErrNoConfig              = errors.New("oracache/gateway: no routing config")
	ErrPartitionOutOfRange   = errors.New("oracache/gateway: partition out of range")
	ErrUnknownRegion         = errors.New("oracache/gateway: unknown local region")
	ErrNoLeader              = errors.New("oracache/gateway: no leader for partition")
	ErrStaleRevision         = cpconfig.ErrStaleRevision
	ErrPartitionCountChanged = cpconfig.ErrPartitionCountChanged
)

// Leader is a resolved local Raft leader. Region is included for diagnostics
// and must equal the region whose assignment produced the route.
type Leader struct {
	NodeID  string
	Address string
	Region  string
}

type snapshot struct {
	revision       uint64
	partitionCount uint32
	masterRegion   string
	local          map[uint32]Leader
	write          map[uint32]Leader
	localReplicas  map[uint32][]Leader
	writeReplicas  map[uint32][]Leader
}

// Router is a lock-free reader over an immutable routing snapshot.
//
// Router must not be copied after first use.
type Router struct {
	region string
	mu     sync.Mutex
	cur    atomic.Pointer[snapshot]
}

// NewRouter returns a router for one local region. An empty region is invalid.
func NewRouter(region string) (*Router, error) {
	if region == "" {
		return nil, fmt.Errorf("%w: empty region", ErrUnknownRegion)
	}
	return &Router{region: region}, nil
}

// Region returns the local region served by this router.
func (r *Router) Region() string { return r.region }

// Apply validates cfg and atomically replaces the routing snapshot. Stale
// revisions and partition count changes are rejected; on error the previous
// snapshot is preserved.
func (r *Router) Apply(cfg *cpconfig.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if _, ok := cfg.RegionByID(r.region); !ok {
		return fmt.Errorf("%w: %q", ErrUnknownRegion, r.region)
	}
	next, err := buildSnapshot(r.region, cfg)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	if old != nil {
		if cfg.Revision <= old.revision {
			return fmt.Errorf("%w: have %d, got %d", ErrStaleRevision, old.revision, cfg.Revision)
		}
		if cfg.PartitionCount != old.partitionCount {
			return fmt.Errorf("%w: have %d, got %d", ErrPartitionCountChanged, old.partitionCount, cfg.PartitionCount)
		}
	}
	r.cur.Store(next)
	return nil
}

func buildSnapshot(localRegion string, cfg *cpconfig.Config) (*snapshot, error) {
	masterRegion := cfg.MasterRegion()
	if masterRegion == "" {
		return nil, fmt.Errorf("%w: config has no master region", ErrUnknownRegion)
	}
	s := &snapshot{
		revision:       cfg.Revision,
		partitionCount: cfg.PartitionCount,
		masterRegion:   masterRegion,
		local:          make(map[uint32]Leader, cfg.PartitionCount),
		write:          make(map[uint32]Leader, cfg.PartitionCount),
		localReplicas:  make(map[uint32][]Leader, cfg.PartitionCount),
		writeReplicas:  make(map[uint32][]Leader, cfg.PartitionCount),
	}
	for p := uint32(0); p < cfg.PartitionCount; p++ {
		localAssignment, ok := cfg.Assignment(localRegion, p)
		if !ok {
			return nil, fmt.Errorf("%w: local region %q has no assignment for partition %d", ErrNoLeader, localRegion, p)
		}
		localLeader, err := resolveLeader(cfg, localAssignment)
		if err != nil {
			return nil, err
		}
		s.local[p] = localLeader
		if s.localReplicas[p], err = resolveReplicas(cfg, localAssignment); err != nil {
			return nil, err
		}

		writeAssignment, ok := cfg.Assignment(masterRegion, p)
		if !ok {
			return nil, fmt.Errorf("%w: master region %q has no assignment for partition %d", ErrNoLeader, masterRegion, p)
		}
		writeLeader, err := resolveLeader(cfg, writeAssignment)
		if err != nil {
			return nil, err
		}
		s.write[p] = writeLeader
		if s.writeReplicas[p], err = resolveReplicas(cfg, writeAssignment); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func resolveLeader(cfg *cpconfig.Config, assignment cpconfig.PartitionAssignment) (Leader, error) {
	node, ok := cfg.NodeByID(assignment.LeaderNodeID)
	if !ok {
		return Leader{}, fmt.Errorf("%w: partition %d references unknown leader %q", ErrNoLeader, assignment.PartitionID, assignment.LeaderNodeID)
	}
	if node.Region != assignment.Region {
		return Leader{}, fmt.Errorf("%w: leader %q is outside region %q", ErrNoLeader, node.ID, assignment.Region)
	}
	return Leader{NodeID: node.ID, Address: node.Address, Region: node.Region}, nil
}

func resolveReplicas(cfg *cpconfig.Config, assignment cpconfig.PartitionAssignment) ([]Leader, error) {
	replicas := make([]Leader, 0, len(assignment.ReplicaNodeIDs))
	for _, id := range assignment.ReplicaNodeIDs {
		node, ok := cfg.NodeByID(id)
		if !ok || node.Region != assignment.Region {
			return nil, fmt.Errorf("%w: partition %d references unknown replica %q", ErrNoLeader, assignment.PartitionID, id)
		}
		replicas = append(replicas, Leader{NodeID: node.ID, Address: node.Address, Region: node.Region})
	}
	return replicas, nil
}

// Replicas returns every replica of a partition's group in configuration order:
// the local region for reads, the master region for writes. Gateways use it
// only to fail over when the assigned leader is stale or unreachable.
func (r *Router) Replicas(partitionID uint32, write bool) ([]Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return nil, ErrNoConfig
	}
	if partitionID >= cur.partitionCount {
		return nil, fmt.Errorf("%w: %d >= %d", ErrPartitionOutOfRange, partitionID, cur.partitionCount)
	}
	src := cur.localReplicas
	if write {
		src = cur.writeReplicas
	}
	return append([]Leader(nil), src[partitionID]...), nil
}

// Revision returns the current config revision, or 0 if no snapshot was applied.
func (r *Router) Revision() uint64 {
	if cur := r.cur.Load(); cur != nil {
		return cur.revision
	}
	return 0
}

// PartitionCount returns the current partition count, or 0 without a snapshot.
func (r *Router) PartitionCount() uint32 {
	if cur := r.cur.Load(); cur != nil {
		return cur.partitionCount
	}
	return 0
}

// MasterRegion returns the region that accepts client writes, or "" without a
// snapshot.
func (r *Router) MasterRegion() string {
	if cur := r.cur.Load(); cur != nil {
		return cur.masterRegion
	}
	return ""
}

// Partition returns the fixed partition for a key.
func (r *Router) Partition(key string) (uint32, error) {
	cur := r.cur.Load()
	if cur == nil {
		return 0, ErrNoConfig
	}
	return partition.Of(key, cur.partitionCount), nil
}

// GetLeader resolves the local-region leader for a partition. This is the GET
// path: strict reads always go to the leader of the caller's region.
func (r *Router) GetLeader(partitionID uint32) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.local, partitionID, cur.partitionCount)
}

// GetLeaderForKey hashes the key and resolves the local-region GET leader.
func (r *Router) GetLeaderForKey(key string) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.local, partition.Of(key, cur.partitionCount), cur.partitionCount)
}

// WriteLeader resolves the master-region leader for a partition. In a slave
// region this is the proxy target for client PUTs.
func (r *Router) WriteLeader(partitionID uint32) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.write, partitionID, cur.partitionCount)
}

// WriteLeaderForKey hashes the key and resolves the master-region PUT leader.
func (r *Router) WriteLeaderForKey(key string) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.write, partition.Of(key, cur.partitionCount), cur.partitionCount)
}

func routeFor(routes map[uint32]Leader, partitionID, partitionCount uint32) (Leader, error) {
	if partitionID >= partitionCount {
		return Leader{}, fmt.Errorf("%w: %d >= %d", ErrPartitionOutOfRange, partitionID, partitionCount)
	}
	leader, ok := routes[partitionID]
	if !ok {
		return Leader{}, fmt.Errorf("%w: partition %d", ErrNoLeader, partitionID)
	}
	return leader, nil
}
