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
	receipts   map[heartbeatSource]heartbeatReceiptTime // keys are the configured node-region pairs
	snapshot   Snapshot
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
	return projectLiveConfig(s.snapshot, func(node cpconfig.Node) replicaHeartbeat {
		heartbeat, ok := s.heartbeats[node.ID]
		received := ok && heartbeat.Region == node.Region
		return replicaHeartbeat{
			heartbeat: heartbeat,
			received:  received,
			live:      received && now.Sub(heartbeat.ReceivedAt) <= ttl,
		}
	}), nil
}
