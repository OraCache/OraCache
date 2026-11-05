package statestore

import (
	"context"
	"fmt"
	"sync"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// MemoryStore is an in-memory Store used by CP Phase 1 and by tests.
//
// It is not durable: a CP API restart loses heartbeats and the last config
// snapshot. Production must use the etcd-backed StateStore described in
// docs/architecture/ARCHITECTURE.md §7.1.
//
// TODO(phase-3): replace with an etcd-backed Store; keep the API boundary stable.
type MemoryStore struct {
	mu         sync.RWMutex
	heartbeats map[string]Heartbeat
	snapshot   Snapshot
}

// NewMemory returns an empty in-memory store.
func NewMemory() *MemoryStore {
	return &MemoryStore{heartbeats: make(map[string]Heartbeat)}
}

// UpsertHeartbeat implements Store.
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

// Heartbeat implements Store.
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

// Config implements Store.
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

// ApplyConfig implements Store.
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
