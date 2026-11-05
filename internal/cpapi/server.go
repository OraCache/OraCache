// Package cpapi implements the stateless Control Plane gRPC API.
//
// The API accepts heartbeats and serves immutable config snapshots from a
// statestore.Store. It contains no placement logic: assignments are produced by
// the Scheduler/Controller and published through the StateStore. Heartbeats are
// soft state and never change the config revision by themselves.
package cpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MaxIDBytes bounds node, region and instance identifiers.
	MaxIDBytes = 256
	// MaxLastErrorBytes bounds diagnostic text carried by a heartbeat.
	MaxLastErrorBytes = 1024
	// DefaultHeartbeatInterval is the interval advertised to CP Agents.
	DefaultHeartbeatInterval = 5 * time.Second
)

// Clock supplies CP receipt time. Tests use a fake clock; liveness must never
// be derived from a client-supplied timestamp.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Option customizes a Server.
type Option func(*Server)

// WithClock replaces the wall clock.
func WithClock(clock Clock) Option {
	return func(s *Server) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithLogger replaces the logger. A nil logger keeps the default.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		if logger != nil {
			s.log = logger
		}
	}
}

// WithHeartbeatInterval overrides the interval advertised to CP Agents. The
// production default is DefaultHeartbeatInterval.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(s *Server) {
		if interval > 0 {
			s.heartbeatInterval = interval
		}
	}
}

// Server implements controlplanev1.ControlPlaneAPIServer.
type Server struct {
	controlplanev1.UnimplementedControlPlaneAPIServer

	store             statestore.Store
	clock             Clock
	log               *slog.Logger
	heartbeatInterval time.Duration
}

// New returns a CP API server. The store must not be nil.
func New(store statestore.Store, opts ...Option) *Server {
	if store == nil {
		panic("cpapi: nil StateStore")
	}
	s := &Server{
		store:             store,
		clock:             systemClock{},
		log:               slog.Default(),
		heartbeatInterval: DefaultHeartbeatInterval,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ReportHeartbeat implements controlplanev1.ControlPlaneAPI. It records a full
// snapshot of local partition groups and returns the current config revision.
func (s *Server) ReportHeartbeat(ctx context.Context, req *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "heartbeat request is required")
	}
	if err := validateID("node_id", req.GetNodeId()); err != nil {
		return nil, err
	}
	if err := validateID("region", req.GetRegion()); err != nil {
		return nil, err
	}
	if len(req.GetInstance()) > MaxIDBytes {
		return nil, status.Error(codes.InvalidArgument, "instance is too long")
	}
	if len(req.GetPartitions()) > int(cpconfig.DefaultPartitionCount) {
		return nil, status.Errorf(codes.InvalidArgument, "too many partitions: %d", len(req.GetPartitions()))
	}

	heartbeat := statestore.Heartbeat{
		NodeID:         req.GetNodeId(),
		Region:         req.GetRegion(),
		Instance:       req.GetInstance(),
		ConfigRevision: req.GetConfigRevision(),
		ReceivedAt:     s.clock.Now().UTC(),
	}
	if req.GetSentAt() != nil {
		heartbeat.SentAt = req.GetSentAt().AsTime().UTC()
	}
	if resources := req.GetResources(); resources != nil {
		heartbeat.Resources = statestore.Resources{
			CPUMillis:     resources.GetCpuMillis(),
			MemoryBytes:   resources.GetMemoryBytes(),
			DiskFreeBytes: resources.GetDiskFreeBytes(),
		}
	}
	seen := make(map[uint32]struct{}, len(req.GetPartitions()))
	for i, p := range req.GetPartitions() {
		if p == nil {
			return nil, status.Errorf(codes.InvalidArgument, "partition %d is nil", i)
		}
		if p.GetPartitionId() >= cpconfig.DefaultPartitionCount {
			return nil, status.Errorf(codes.InvalidArgument, "partition %d is outside [0,%d)", p.GetPartitionId(), cpconfig.DefaultPartitionCount)
		}
		if _, ok := seen[p.GetPartitionId()]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate partition %d", p.GetPartitionId())
		}
		seen[p.GetPartitionId()] = struct{}{}
		if len(p.GetLastError()) > MaxLastErrorBytes {
			return nil, status.Errorf(codes.InvalidArgument, "partition %d last_error is too long", p.GetPartitionId())
		}
		role, err := fromProtoRole(p.GetRole())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		heartbeat.Partitions = append(heartbeat.Partitions, statestore.PartitionStatus{
			PartitionID:  p.GetPartitionId(),
			Role:         role,
			Term:         p.GetTerm(),
			CommitIndex:  p.GetCommitIndex(),
			AppliedIndex: p.GetAppliedIndex(),
			LeaderID:     p.GetLeaderId(),
			Ready:        p.GetReady(),
			LastError:    p.GetLastError(),
		})
	}

	revision, err := s.store.UpsertHeartbeat(ctx, heartbeat)
	if err != nil {
		if errors.Is(err, statestore.ErrInvalidHeartbeat) {
			return nil, status.Error(codes.InvalidArgument, "invalid heartbeat")
		}
		s.log.ErrorContext(ctx, "store heartbeat failed", "node_id", heartbeat.NodeID, "region", heartbeat.Region, "error", err)
		return nil, status.Error(codes.Unavailable, "heartbeat store unavailable")
	}
	s.log.DebugContext(ctx, "heartbeat accepted",
		"node_id", heartbeat.NodeID,
		"region", heartbeat.Region,
		"config_revision", revision,
		"partitions", len(heartbeat.Partitions),
	)
	return &controlplanev1.HeartbeatResponse{
		Accepted:             true,
		ConfigRevision:       revision,
		NextHeartbeatAfterMs: uint32(s.heartbeatInterval / time.Millisecond),
	}, nil
}

// GetConfig implements controlplanev1.ControlPlaneAPI. A client that already
// knows the current revision receives revision only and keeps last-known-good.
func (s *Server) GetConfig(ctx context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "config request is required")
	}
	if err := validateID("node_id", req.GetNodeId()); err != nil {
		return nil, err
	}
	switch req.GetKind() {
	case controlplanev1.AgentKind_AGENT_KIND_GATEWAY, controlplanev1.AgentKind_AGENT_KIND_STORAGE_NODE:
	default:
		return nil, status.Error(codes.InvalidArgument, "kind must be GATEWAY or STORAGE_NODE")
	}

	snapshot, err := s.store.Config(ctx)
	if err != nil {
		s.log.ErrorContext(ctx, "store config failed", "node_id", req.GetNodeId(), "error", err)
		return nil, status.Error(codes.Unavailable, "config store unavailable")
	}
	if req.GetKnownRevision() > snapshot.Revision {
		return nil, status.Errorf(codes.FailedPrecondition, "known revision %d is ahead of CP revision %d", req.GetKnownRevision(), snapshot.Revision)
	}
	if req.GetKnownRevision() == snapshot.Revision {
		return &controlplanev1.GetConfigResponse{Revision: snapshot.Revision}, nil
	}
	if snapshot.Config == nil {
		s.log.ErrorContext(ctx, "config snapshot is missing", "node_id", req.GetNodeId(), "revision", snapshot.Revision)
		return nil, status.Error(codes.Internal, "config snapshot is inconsistent")
	}
	if snapshot.Config.Revision != snapshot.Revision {
		s.log.ErrorContext(ctx, "config revision mismatch",
			"node_id", req.GetNodeId(),
			"snapshot_revision", snapshot.Revision,
			"config_revision", snapshot.Config.Revision,
		)
		return nil, status.Error(codes.Internal, "config snapshot is inconsistent")
	}
	response := &controlplanev1.GetConfigResponse{
		Revision: snapshot.Revision,
		Config:   snapshot.Config.ToProto(),
	}
	s.log.DebugContext(ctx, "config served",
		"node_id", req.GetNodeId(),
		"kind", req.GetKind().String(),
		"known_revision", req.GetKnownRevision(),
		"revision", response.Revision,
	)
	return response, nil
}

func validateID(field, value string) error {
	if value == "" {
		return status.Errorf(codes.InvalidArgument, "%s is required", field)
	}
	if len(value) > MaxIDBytes {
		return status.Errorf(codes.InvalidArgument, "%s is too long", field)
	}
	return nil
}

func fromProtoRole(role controlplanev1.RaftRole) (statestore.RaftRole, error) {
	switch role {
	case controlplanev1.RaftRole_RAFT_ROLE_UNSPECIFIED:
		return statestore.RaftRoleUnspecified, nil
	case controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER:
		return statestore.RaftRoleFollower, nil
	case controlplanev1.RaftRole_RAFT_ROLE_CANDIDATE:
		return statestore.RaftRoleCandidate, nil
	case controlplanev1.RaftRole_RAFT_ROLE_LEADER:
		return statestore.RaftRoleLeader, nil
	case controlplanev1.RaftRole_RAFT_ROLE_LEARNER:
		return statestore.RaftRoleLearner, nil
	default:
		return statestore.RaftRoleUnspecified, fmt.Errorf("unknown raft role %d", role)
	}
}
