// Package statestore определяет границу состояния Control Plane.
//
// MemoryStore — временное process-local хранилище для CP Phase 1. Оно теряет
// всё состояние при рестарте и должно быть заменено etcd-backed реализацией до
// production; см.
// docs/architecture/ADR-001-cp-phase1-in-memory-store.md.
package statestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
)

// ErrInvalidHeartbeat возвращается для некорректных heartbeat. CP API
// отображает его в gRPC InvalidArgument.
var ErrInvalidHeartbeat = errors.New("oracache/statestore: invalid heartbeat")
var ErrMembershipConflict = errors.New("oracache/statestore: conflicting raft membership")

// RaftRole — локальная роль partition-группы. Она никогда не является глобальной
// ролью для всех регионов.
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

// PartitionStatus — последнее полученное состояние одной локальной Raft-группы.
type PartitionStatus struct {
	PartitionID  uint32
	Role         RaftRole
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	LeaderID     string
	Ready        bool
	LastError    string
	Membership   *cpconfig.GroupMembership
}

// Resources — последний полученный snapshot ёмкости ноды.
type Resources struct {
	CPUMillis     uint64
	MemoryBytes   uint64
	DiskFreeBytes uint64
}

// Heartbeat — полный snapshot локального региона, присланный CP Agent.
//
// ReceivedAt выставляется часами CP API и является единственным входом для
// liveness. SentAt — только диагностика: часы нод могут расходиться.
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

// HeartbeatReceipt records when CP received the last heartbeat for a node in a region.
type HeartbeatReceipt struct {
	NodeID     string
	Region     string
	ReceivedAt time.Time
}

// Snapshot — неизменяемая revision конфигурации и её тело. Config равен nil,
// только когда в store ещё нет конфигурации (revision 0).
type Snapshot struct {
	Revision uint64
	Config   *cpconfig.Config
	// MissingReplicaHeartbeats indicates that some configured, non-draining replicas
	// have never reported to this store. It is only populated by LiveConfig.
	MissingReplicaHeartbeats bool
}

// Store — граница состояния Control Plane, используемая CP API. Production-
// реализация работает через etcd; MemoryStore существует только для Phase 1
// и тестов.
type Store interface {
	// UpsertHeartbeat сохраняет полный heartbeat snapshot и возвращает текущую
	// revision конфигурации. Heartbeat никогда не меняет revision конфигурации.
	UpsertHeartbeat(ctx context.Context, heartbeat Heartbeat) (uint64, error)

	// Heartbeat возвращает последний heartbeat ноды.
	Heartbeat(ctx context.Context, nodeID string) (Heartbeat, bool, error)

	// HeartbeatReceipts returns receipts recorded while each node-region pair was
	// continuously configured, including receipts older than the liveness TTL.
	HeartbeatReceipts(ctx context.Context) ([]HeartbeatReceipt, error)

	// Config возвращает текущий неизменяемый snapshot. Возвращённый Config —
	// независимая копия; вызывающий код не должен менять через него состояние store.
	Config(ctx context.Context) (Snapshot, error)

	// LiveConfig возвращает snapshot с актуальным live-состоянием нод и лидеров.
	// Revision остаётся revision топологии: liveness не создаёт новую конфигурацию.
	LiveConfig(ctx context.Context, now time.Time, ttl time.Duration) (Snapshot, error)

	// ApplyConfig публикует новую revision конфигурации. Revision должна расти,
	// а число партиций — не меняться.
	ApplyConfig(ctx context.Context, config *cpconfig.Config) error
}

// CloneHeartbeat возвращает глубокую копию для передачи между goroutine.
func CloneHeartbeat(h Heartbeat) Heartbeat {
	h.Partitions = append([]PartitionStatus(nil), h.Partitions...)
	for i := range h.Partitions {
		if m := h.Partitions[i].Membership; m != nil {
			copyOf := *m
			copyOf.Members = append([]cpconfig.GroupMember(nil), m.Members...)
			copyOf.RemovedIDs = append([]uint64(nil), m.RemovedIDs...)
			h.Partitions[i].Membership = &copyOf
		}
	}
	return h
}

// ValidateHeartbeat проверяет поля, на которые опирается store. CP API также
// валидирует транспортный уровень; это defense in depth для других вызывающих.
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
