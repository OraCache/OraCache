package shardraft

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type endpoint struct {
	group string
	peer  uint64
}
type link struct {
	group    string
	from, to uint64
}

// MemoryNetwork delivers serialized messages between independent Instances.
// It shares neither caches nor Raft state. Cut supports bidirectional partitions.
type MemoryNetwork struct {
	mu      sync.RWMutex
	nodes   map[endpoint]*Instance
	blocked map[link]bool
}

func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{nodes: make(map[endpoint]*Instance), blocked: make(map[link]bool)}
}

// Register is called before Run. Reusing a registered identity is rejected,
// including after Close: empty-state peer restarts are outside this PoC.
func (m *MemoryNetwork) Register(n *Instance) error {
	if n == nil {
		return fmt.Errorf("nil instance")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := endpoint{n.cfg.GroupID, n.cfg.PeerID}
	if _, exists := m.nodes[key]; exists {
		return fmt.Errorf("peer already registered")
	}
	m.nodes[key] = n
	return nil
}

func (m *MemoryNetwork) Cut(group string, a, b uint64, blocked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pair := range [][2]uint64{{a, b}, {b, a}} {
		key := link{group, pair[0], pair[1]}
		if blocked {
			m.blocked[key] = true
		} else {
			delete(m.blocked, key)
		}
	}
}

func (m *MemoryNetwork) Send(ctx context.Context, group string, to uint64, data []byte) error {
	var message raftpb.Message
	if err := message.Unmarshal(data); err != nil {
		return err
	}
	m.mu.RLock()
	n := m.nodes[endpoint{group, to}]
	blocked := m.blocked[link{group, message.From, to}]
	m.mu.RUnlock()
	if n == nil || blocked {
		return fmt.Errorf("peer unavailable")
	}
	return n.Step(ctx, group, data)
}
