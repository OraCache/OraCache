package statestore

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// MemoryStore — in-memory Store для CP Phase 1 и тестов.
//
// Рестарт CP API теряет heartbeat и последний config snapshot.
// OpenMemory can checkpoint the applied Raft membership independently.
// В production должен использоваться etcd-backed StateStore, описанный в
// docs/architecture/ARCHITECTURE.md §7.1.
//
// TODO(phase-3): заменить на etcd-backed Store, сохранив стабильную границу API.
type MemoryStore struct {
	mu                 sync.RWMutex
	heartbeats         map[string]Heartbeat
	memberships        map[groupKey]cpconfig.GroupMembership
	membershipFile     string
	bootstrapDiscovery bool
	receipts           map[heartbeatSource]heartbeatReceiptTime // keys are the configured node-region pairs
	snapshot           Snapshot
}

type groupKey struct {
	region    string
	partition uint32
}

type heartbeatSource struct {
	nodeID string
	region string
}

type heartbeatReceiptTime struct {
	receivedAt time.Time
	received   bool
}

// NewMemory возвращает пустой in-memory store.
func NewMemory() *MemoryStore {
	return &MemoryStore{heartbeats: make(map[string]Heartbeat), memberships: make(map[groupKey]cpconfig.GroupMembership)}
}

// UpsertHeartbeat реализует Store.
func (s *MemoryStore) UpsertHeartbeat(ctx context.Context, heartbeat Heartbeat) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := ValidateHeartbeat(heartbeat); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Validate every reported group before publishing any part of the heartbeat.
	updates := make(map[groupKey]cpconfig.GroupMembership)
	for _, p := range heartbeat.Partitions {
		m := p.Membership
		if m == nil {
			continue
		}
		if p.Role != RaftRoleLeader || m.GroupID == "" || m.Index == 0 || m.Region != heartbeat.Region || m.PartitionID != p.PartitionID || m.ReporterID == 0 || len(m.Members) == 0 {
			return 0, fmt.Errorf("%w: invalid leader membership report", ErrInvalidHeartbeat)
		}
		if s.snapshot.Config == nil {
			return 0, fmt.Errorf("%w: missing topology", ErrInvalidHeartbeat)
		}
		assignment, ok := s.snapshot.Config.Assignment(m.Region, m.PartitionID)
		if !ok {
			return 0, fmt.Errorf("%w: unknown group assignment", ErrInvalidHeartbeat)
		}
		if assignment.GroupID != "" && assignment.GroupID != m.GroupID {
			return 0, fmt.Errorf("%w: group incarnation does not match topology", ErrMembershipConflict)
		}
		peerIDs, nodeIDs := map[uint64]bool{}, map[string]bool{}
		outgoingIDs := map[uint64]bool{}
		reporter := false
		for _, peer := range m.Members {
			if peer.PeerID == 0 || peer.NodeID == "" || peer.Address == "" || peer.RaftAddress == "" || peerIDs[peer.PeerID] || nodeIDs[peer.NodeID] || (peer.Voter && peer.Learner) || (!peer.Voter && !peer.Learner && !peer.OutgoingVoter) {
				return 0, fmt.Errorf("%w: invalid or duplicate member", ErrInvalidHeartbeat)
			}
			peerIDs[peer.PeerID], nodeIDs[peer.NodeID] = true, true
			outgoingIDs[peer.PeerID] = peer.OutgoingVoter
			if peer.PeerID == m.ReporterID && peer.NodeID == heartbeat.NodeID && peer.Voter {
				reporter = true
			}
		}
		if !reporter {
			return 0, fmt.Errorf("%w: reporter is not a voter", ErrInvalidHeartbeat)
		}
		removedIDs := map[uint64]bool{}
		for _, id := range m.RemovedIDs {
			if id == 0 || (peerIDs[id] && (!m.Joint || !outgoingIDs[id])) || removedIDs[id] {
				return 0, fmt.Errorf("%w: invalid or duplicate removed peer", ErrInvalidHeartbeat)
			}
			removedIDs[id] = true
		}
		key := groupKey{m.Region, m.PartitionID}
		if old, ok := s.memberships[key]; ok {
			if old.GroupID != m.GroupID {
				return 0, fmt.Errorf("%w: group incarnation changed", ErrMembershipConflict)
			}
			if m.Index < old.Index {
				continue
			}
			if m.Index == old.Index && !sameMembership(old, *m) {
				return 0, fmt.Errorf("%w: same index has different content", ErrMembershipConflict)
			}
		}
		if old, ok := s.memberships[key]; ok && m.Index == old.Index && p.Term < old.Term {
			continue
		}
		copyOf := *m
		copyOf.Term = p.Term
		copyOf.Members = append([]cpconfig.GroupMember(nil), m.Members...)
		copyOf.RemovedIDs = append([]uint64(nil), m.RemovedIDs...)
		updates[key] = copyOf
	}
	for key, m := range updates {
		if s.membershipFile != "" && m.Index > s.memberships[key].Index {
			next := maps.Clone(s.memberships)
			maps.Copy(next, updates)
			if err := writeMemberships(s.membershipFile, next); err != nil {
				return 0, err
			}
			break
		}
	}
	for key, m := range updates {
		s.memberships[key] = m
	}
	s.heartbeats[heartbeat.NodeID] = CloneHeartbeat(heartbeat)
	// Track receipt times by region independently of the routing heartbeat.
	source := heartbeatSource{nodeID: heartbeat.NodeID, region: heartbeat.Region}
	if previous, configured := s.receipts[source]; configured &&
		(!previous.received || heartbeat.ReceivedAt.After(previous.receivedAt)) {
		s.receipts[source] = heartbeatReceiptTime{receivedAt: heartbeat.ReceivedAt, received: true}
	}
	return s.snapshot.Revision, nil
}

// Heartbeat реализует Store.
func (s *MemoryStore) Heartbeat(ctx context.Context, nodeID string) (Heartbeat, bool, error) {
	if err := ctx.Err(); err != nil {
		return Heartbeat{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	heartbeat, ok := s.heartbeats[nodeID]
	if !ok {
		return Heartbeat{}, false, nil
	}
	return CloneHeartbeat(heartbeat), true, nil
}

// HeartbeatReceipts реализует Store.
func (s *MemoryStore) HeartbeatReceipts(ctx context.Context) ([]HeartbeatReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshot.Config == nil {
		return nil, nil
	}
	receipts := make([]HeartbeatReceipt, 0, len(s.snapshot.Config.Nodes))
	for _, node := range s.snapshot.Config.Nodes {
		if receipt := s.receipts[heartbeatSource{nodeID: node.ID, region: node.Region}]; receipt.received {
			receipts = append(receipts, HeartbeatReceipt{NodeID: node.ID, Region: node.Region, ReceivedAt: receipt.receivedAt})
		}
	}
	return receipts, nil
}

// Config реализует Store.
func (s *MemoryStore) Config(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var config *cpconfig.Config
	if s.snapshot.Config != nil {
		config = s.snapshot.Config.Clone()
	}
	return Snapshot{Revision: s.snapshot.Revision, Config: config}, nil
}

// ApplyConfig реализует Store.
func (s *MemoryStore) ApplyConfig(ctx context.Context, config *cpconfig.Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := config.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Config != nil {
		if config.Revision <= s.snapshot.Revision {
			return fmt.Errorf("%w: have %d, got %d", cpconfig.ErrStaleRevision, s.snapshot.Revision, config.Revision)
		}
		if config.PartitionCount != s.snapshot.Config.PartitionCount {
			return fmt.Errorf("%w: have %d, got %d", cpconfig.ErrPartitionCountChanged, s.snapshot.Config.PartitionCount, config.PartitionCount)
		}
	}
	var obsolete []groupKey
	for key, m := range s.memberships {
		if assignment, ok := config.Assignment(key.region, key.partition); !ok || (assignment.GroupID != "" && assignment.GroupID != m.GroupID) {
			obsolete = append(obsolete, key)
		}
	}
	if s.membershipFile != "" && len(obsolete) > 0 {
		next := maps.Clone(s.memberships)
		for _, key := range obsolete {
			delete(next, key)
		}
		if err := writeMemberships(s.membershipFile, next); err != nil {
			return err
		}
	}
	for _, key := range obsolete {
		delete(s.memberships, key)
	}
	// Keep receipt times only for pairs present in both configurations.
	receipts := make(map[heartbeatSource]heartbeatReceiptTime, len(config.Nodes))
	for _, node := range config.Nodes {
		source := heartbeatSource{nodeID: node.ID, region: node.Region}
		receipts[source] = s.receipts[source]
	}
	s.receipts = receipts
	s.snapshot = Snapshot{Revision: config.Revision, Config: config.Clone()}
	return nil
}

// LiveConfig реализует Store. Он строит представление конфигурации, в котором:
//   - нода без heartbeat свежее ttl помечается dead;
//   - нода с heartbeat, но без готовых partition-групп, помечается starting;
//   - лидер партиции берётся из последних heartbeat, если он жив и входит в реплики.
//
// Revision не меняется: liveness — это представление поверх неизменяемой
// топологии, а не новая revision конфигурации.
func (s *MemoryStore) LiveConfig(ctx context.Context, now time.Time, ttl time.Duration) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if ttl <= 0 {
		return Snapshot{}, fmt.Errorf("statestore: node TTL must be positive")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	view := projectLiveConfig(s.snapshot, s.memberships, func(node cpconfig.Node) replicaHeartbeat {
		heartbeat, ok := s.heartbeats[node.ID]
		received := ok && heartbeat.Region == node.Region
		return replicaHeartbeat{
			heartbeat: heartbeat,
			received:  received,
			live:      received && now.Sub(heartbeat.ReceivedAt) <= ttl,
		}
	})
	view.BootstrapDiscovery = s.bootstrapDiscovery
	return view, nil
}

func sameMembership(a, b cpconfig.GroupMembership) bool {
	if a.GroupID != b.GroupID || a.Region != b.Region || a.PartitionID != b.PartitionID || a.Index != b.Index || a.Joint != b.Joint {
		return false
	}
	ac, bc := append([]cpconfig.GroupMember(nil), a.Members...), append([]cpconfig.GroupMember(nil), b.Members...)
	slices.SortFunc(ac, func(x, y cpconfig.GroupMember) int {
		if x.PeerID < y.PeerID {
			return -1
		}
		if x.PeerID > y.PeerID {
			return 1
		}
		return 0
	})
	slices.SortFunc(bc, func(x, y cpconfig.GroupMember) int {
		if x.PeerID < y.PeerID {
			return -1
		}
		if x.PeerID > y.PeerID {
			return 1
		}
		return 0
	})
	ar, br := append([]uint64(nil), a.RemovedIDs...), append([]uint64(nil), b.RemovedIDs...)
	slices.Sort(ar)
	slices.Sort(br)
	return reflect.DeepEqual(ac, bc) && reflect.DeepEqual(ar, br)
}
