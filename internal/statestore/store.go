// Package statestore defines the Control Plane state boundary.
//
// The MemoryStore implementation is a temporary, process-local store for CP
// Phase 1. It loses all state on restart and must be replaced by an etcd-backed
// implementation before production; see
// docs/architecture/ADR-001-cp-phase1-in-memory-store.md.
package statestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// ErrInvalidHeartbeat is returned for malformed heartbeat records. The CP API
// maps it to gRPC InvalidArgument.
var ErrInvalidHeartbeat = errors.New("oracache/statestore: invalid heartbeat")

// RaftRole is the local role of a partition group. It is never a global role
// across regions.
type RaftRole uint8

const (
	RaftRoleUnspecified RaftRole = iota
	RaftRoleFollower
	RaftRoleCandidate
	RaftRoleLeader
	RaftRoleLearner
)

func (r RaftRole) String() string {
	switch r {
	case RaftRoleFollower:
		return "follower"
	case RaftRoleCandidate:
		return "candidate"
	case RaftRoleLeader:
		return "leader"
	case RaftRoleLearner:
		return "learner"
	default:
		return "unspecified"
	}
}

// PartitionStatus is the last reported state of one local Raft group.
type PartitionStatus struct {
	PartitionID  uint32
	Role         RaftRole
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	LeaderID     string
	Ready        bool
	LastError    string
}

// Resources is the last reported node capacity snapshot.
type Resources struct {
	CPUMillis     uint64
	MemoryBytes   uint64
	DiskFreeBytes uint64
}

// Heartbeat is one full local-region snapshot reported by a CP Agent.
//
// ReceivedAt is assigned by the CP API clock and is the only liveness input.
// SentAt is diagnostics only: node clocks may be skewed.
type Heartbeat struct {
	NodeID         string
	Region         string
	Instance       string
	SentAt         time.Time
	ReceivedAt     time.Time
	ConfigRevision uint64
	Partitions     []PartitionStatus
	Resources      Resources
}

// Snapshot is an immutable configuration revision and its body. Config is nil
// only when the store has no configuration yet (revision 0).
type Snapshot struct {
	Revision uint64
	Config   *cpconfig.Config
}

// Store is the Control Plane state boundary used by CP API. The production
// implementation is etcd-backed; MemoryStore exists only for Phase 1 and tests.
type Store interface {
	// UpsertHeartbeat records a full heartbeat snapshot and returns the current
	// config revision. A heartbeat never changes the config revision.
	UpsertHeartbeat(ctx context.Context, heartbeat Heartbeat) (uint64, error)

	// Heartbeat returns the last heartbeat for a node.
	Heartbeat(ctx context.Context, nodeID string) (Heartbeat, bool, error)

	// Config returns the current immutable snapshot. The returned Config is an
	// independent copy; callers may not use it to mutate store state.
	Config(ctx context.Context) (Snapshot, error)

	// ApplyConfig publishes a newer config revision. Revisions must increase and
	// the partition count must not change.
	ApplyConfig(ctx context.Context, config *cpconfig.Config) error
}

// CloneHeartbeat returns a deep copy suitable for crossing goroutine boundaries.
func CloneHeartbeat(h Heartbeat) Heartbeat {
	h.Partitions = append([]PartitionStatus(nil), h.Partitions...)
	return h
}

// ValidateHeartbeat checks the fields the store relies on. The CP API performs
// transport-level validation too; this is defense in depth for other callers.
func ValidateHeartbeat(h Heartbeat) error {
	if h.NodeID == "" {
		return fmt.Errorf("%w: node_id is required", ErrInvalidHeartbeat)
	}
	if h.Region == "" {
		return fmt.Errorf("%w: region is required", ErrInvalidHeartbeat)
	}
	seen := make(map[uint32]struct{}, len(h.Partitions))
	for _, p := range h.Partitions {
		if p.PartitionID >= cpconfig.DefaultPartitionCount {
			return fmt.Errorf("%w: partition %d is outside [0,%d)", ErrInvalidHeartbeat, p.PartitionID, cpconfig.DefaultPartitionCount)
		}
		if _, ok := seen[p.PartitionID]; ok {
			return fmt.Errorf("%w: duplicate partition %d", ErrInvalidHeartbeat, p.PartitionID)
		}
		seen[p.PartitionID] = struct{}{}
	}
	return nil
}
