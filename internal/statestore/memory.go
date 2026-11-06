package statestore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// MemoryStore — in-memory Store для CP Phase 1 и тестов.
//
// Он не durable: рестарт CP API теряет heartbeat и последний config snapshot.
// В production должен использоваться etcd-backed StateStore, описанный в
// docs/architecture/ARCHITECTURE.md §7.1.
//
// TODO(phase-3): заменить на etcd-backed Store, сохранив стабильную границу API.
type MemoryStore struct {
	mu         sync.RWMutex
	heartbeats map[string]Heartbeat
	snapshot   Snapshot
}

// NewMemory возвращает пустой in-memory store.
func NewMemory() *MemoryStore {
	return &MemoryStore{heartbeats: make(map[string]Heartbeat)}
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
	s.heartbeats[heartbeat.NodeID] = CloneHeartbeat(heartbeat)
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
	if s.snapshot.Config == nil {
		return Snapshot{Revision: s.snapshot.Revision}, nil
	}

	config := s.snapshot.Config.Clone()
	assigned := make(map[string]bool, len(config.Nodes))
	for i := range config.Partitions {
		for _, nodeID := range config.Partitions[i].ReplicaNodeIDs {
			assigned[nodeID] = true
		}
	}

	alive := make(map[string]bool, len(config.Nodes))
	for i := range config.Nodes {
		node := &config.Nodes[i]
		heartbeat, ok := s.heartbeats[node.ID]
		if !ok || heartbeat.Region != node.Region || now.Sub(heartbeat.ReceivedAt) > ttl {
			node.State = cpconfig.NodeStateDead
			continue
		}
		alive[node.ID] = true
		switch node.State {
		case cpconfig.NodeStateDraining:
			continue
		}
		ready := false
		for _, partition := range heartbeat.Partitions {
			if partition.Ready {
				ready = true
				break
			}
		}
		switch {
		case ready, !assigned[node.ID]:
			node.State = cpconfig.NodeStateReady
		default:
			node.State = cpconfig.NodeStateStarting
		}
	}

	type partitionKey struct {
		region    string
		partition uint32
	}
	type leaderReport struct {
		nodeID string
		term   uint64
	}
	reported := make(map[partitionKey]leaderReport)
	hinted := make(map[partitionKey]string)
	for nodeID, heartbeat := range s.heartbeats {
		if !alive[nodeID] {
			continue
		}
		for _, partition := range heartbeat.Partitions {
			if partition.PartitionID >= config.PartitionCount {
				continue
			}
			key := partitionKey{region: heartbeat.Region, partition: partition.PartitionID}
			if partition.Role == RaftRoleLeader && partition.Ready {
				current, ok := reported[key]
				if !ok || partition.Term > current.term || (partition.Term == current.term && nodeID < current.nodeID) {
					reported[key] = leaderReport{nodeID: nodeID, term: partition.Term}
				}
			}
			if partition.LeaderID != "" {
				if current, ok := hinted[key]; !ok || partition.LeaderID < current {
					hinted[key] = partition.LeaderID
				}
			}
		}
	}

	for i := range config.Partitions {
		assignment := &config.Partitions[i]
		key := partitionKey{region: assignment.Region, partition: assignment.PartitionID}
		candidate := assignment.LeaderNodeID
		if report, ok := reported[key]; ok {
			candidate = report.nodeID
		} else if nodeID, ok := hinted[key]; ok {
			candidate = nodeID
		}
		if candidate == assignment.LeaderNodeID {
			continue
		}
		if !alive[candidate] {
			continue
		}
		node, ok := config.NodeByID(candidate)
		if !ok || node.Region != assignment.Region || node.State != cpconfig.NodeStateReady {
			continue
		}
		replicas := false
		for _, nodeID := range assignment.ReplicaNodeIDs {
			if nodeID == candidate {
				replicas = true
				break
			}
		}
		if !replicas {
			continue
		}
		assignment.LeaderNodeID = candidate
	}

	// Заполняем per-partition readiness по heartbeat выбранного лидера.
	for i := range config.Partitions {
		assignment := &config.Partitions[i]
		assignment.Ready = false
		assignment.Readable = false
		assignment.Writable = false

		heartbeat, ok := s.heartbeats[assignment.LeaderNodeID]
		if !ok || heartbeat.Region != assignment.Region {
			continue
		}
		node, ok := config.NodeByID(assignment.LeaderNodeID)
		if !ok || node.State != cpconfig.NodeStateReady {
			continue
		}
		region, ok := config.RegionByID(assignment.Region)
		if !ok {
			continue
		}
		for _, partition := range heartbeat.Partitions {
			if partition.PartitionID != assignment.PartitionID {
				continue
			}
			if partition.Role != RaftRoleLeader || !partition.Ready {
				break
			}
			assignment.Ready = true
			assignment.Readable = true
			assignment.Writable = region.Role == cpconfig.RegionRoleMaster
			break
		}
	}

	return Snapshot{Revision: s.snapshot.Revision, Config: config}, nil
}
