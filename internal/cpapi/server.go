// Package cpapi реализует stateless gRPC API Control Plane.
//
// API принимает heartbeat, отдаёт неизменяемый snapshot конфигурации и
// live-view состава кластера для Gateway. Desired placement формирует
// Scheduler/Controller и публикует через StateStore; heartbeat остаётся soft
// state и не меняет revision конфигурации.
package cpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/statestore"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MaxIDBytes ограничивает длину node_id, region и instance.
	MaxIDBytes = 256
	// MaxLastErrorBytes ограничивает диагностический текст в heartbeat.
	MaxLastErrorBytes = 1024
	// DefaultHeartbeatInterval — интервал, который CP API сообщает CP Agent.
	DefaultHeartbeatInterval = 5 * time.Second
	// DefaultNodeTTL — сколько CP API считает ноду живой без heartbeat.
	DefaultNodeTTL = 15 * time.Second
)

// Clock даёт время получения heartbeat на CP. Тесты используют fake clock;
// liveness нельзя вычислять по timestamp клиента.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Option настраивает Server.
type Option func(*Server)

// WithClock подменяет системные часы.
func WithClock(clock Clock) Option {
	return func(s *Server) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithLogger подменяет логгер. nil сохраняет логгер по умолчанию.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Server) {
		if logger != nil {
			s.log = logger
		}
	}
}

// WithMetrics подменяет реестр метрик. По умолчанию создаётся новый, доступный
// через Server.Metrics.
func WithMetrics(metrics *cpmetrics.Metrics) Option {
	return func(s *Server) {
		if metrics != nil {
			s.metrics = metrics
		}
	}
}

// WithHeartbeatInterval переопределяет интервал, который сообщается CP Agent.
// По умолчанию используется DefaultHeartbeatInterval.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(s *Server) {
		if interval > 0 {
			s.heartbeatInterval = interval
		}
	}
}

// WithNodeTTL переопределяет TTL liveness ноды. По умолчанию используется
// DefaultNodeTTL.
func WithNodeTTL(ttl time.Duration) Option {
	return func(s *Server) {
		if ttl > 0 {
			s.nodeTTL = ttl
		}
	}
}

// Server реализует controlplanev1.ControlPlaneAPIServer.
type Server struct {
	controlplanev1.UnimplementedControlPlaneAPIServer

	store             statestore.Store
	clock             Clock
	log               *slog.Logger
	heartbeatInterval time.Duration
	nodeTTL           time.Duration
	startedAt         time.Time
	metrics           *cpmetrics.Metrics
	heartbeatDuration prometheus.Observer
	getConfigDuration prometheus.Observer
}

// New создаёт сервер CP API. store не должен быть nil.
func New(store statestore.Store, opts ...Option) *Server {
	if store == nil {
		panic("cpapi: nil StateStore")
	}
	s := &Server{
		store:             store,
		clock:             systemClock{},
		log:               slog.Default(),
		heartbeatInterval: DefaultHeartbeatInterval,
		nodeTTL:           DefaultNodeTTL,
		metrics:           cpmetrics.New(),
	}
	for _, opt := range opts {
		opt(s)
	}
	duration := cpmetrics.NewDuration(s.metrics.Prometheus(), "oracache_cp_rpc_duration_seconds", "Control Plane RPC handler duration in seconds.", "operation")
	s.heartbeatDuration = duration.WithLabelValues("report_heartbeat")
	s.getConfigDuration = duration.WithLabelValues("get_config")
	s.startedAt = s.clock.Now()
	s.metrics.RegisterGaugeFunc(cpmetrics.HeartbeatAge, "Seconds since the CP API received the last heartbeat from a configured node.", s.heartbeatAges)
	s.metrics.RegisterGaugeFunc("oracache_cp_raft_member", "Replica in the last applied Raft group membership received by Control Plane.", s.raftMembers)
	s.metrics.RegisterGaugeFunc("oracache_cp_raft_membership_fresh", "Raft membership confirmation: 1 current, 0 last known, -1 unknown.", s.raftMembershipFreshness)
	return s
}

// Metrics возвращает реестр метрик сервера для экспозиции /metrics.
func (s *Server) Metrics() *cpmetrics.Metrics { return s.metrics }

func (s *Server) heartbeatAges(ctx context.Context) ([]cpmetrics.GaugeSample, error) {
	receipts, err := s.store.HeartbeatReceipts(ctx)
	if err != nil {
		return nil, err
	}
	now := s.clock.Now()
	samples := make([]cpmetrics.GaugeSample, 0, len(receipts))
	for _, receipt := range receipts {
		samples = append(samples, cpmetrics.GaugeSample{
			Labels: map[string]string{"region": receipt.Region, "node_id": receipt.NodeID},
			Value:  max(0, now.Sub(receipt.ReceivedAt).Seconds()),
		})
	}
	return samples, nil
}

// ReportHeartbeat реализует controlplanev1.ControlPlaneAPI. Он сохраняет полный
// snapshot локальных partition-групп и возвращает текущую revision конфигурации.
func (s *Server) ReportHeartbeat(ctx context.Context, req *controlplanev1.HeartbeatRequest) (*controlplanev1.HeartbeatResponse, error) {
	start := time.Now()
	defer func() { s.heartbeatDuration.Observe(time.Since(start).Seconds()) }()
	if req == nil {
		s.metrics.Heartbeat(cpmetrics.HeartbeatInvalid)
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
		s.metrics.Heartbeat(cpmetrics.HeartbeatInvalid)
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
		ps := statestore.PartitionStatus{
			PartitionID:  p.GetPartitionId(),
			Role:         role,
			Term:         p.GetTerm(),
			CommitIndex:  p.GetCommitIndex(),
			AppliedIndex: p.GetAppliedIndex(),
			LeaderID:     p.GetLeaderId(),
			Ready:        p.GetReady(),
			LastError:    p.GetLastError(),
		}
		if wire := p.GetMembership(); wire != nil {
			m := &cpconfig.GroupMembership{GroupID: wire.GetGroupId(), Region: wire.GetRegion(), PartitionID: wire.GetPartitionId(), ReporterID: wire.GetReporterPeerId(), Index: wire.GetConfigIndex(), Joint: wire.GetJoint(), RemovedIDs: append([]uint64(nil), wire.GetRemovedIds()...)}
			for _, peer := range wire.GetMembers() {
				if peer == nil {
					return nil, status.Error(codes.InvalidArgument, "nil membership peer")
				}
				m.Members = append(m.Members, cpconfig.GroupMember{PeerID: peer.GetPeerId(), NodeID: peer.GetNodeId(), Address: peer.GetAddress(), RaftAddress: peer.GetRaftAddress(), Voter: peer.GetVoter(), Learner: peer.GetLearner(), OutgoingVoter: peer.GetOutgoingVoter()})
			}
			ps.Membership = m
		}
		heartbeat.Partitions = append(heartbeat.Partitions, ps)
	}

	revision, err := s.store.UpsertHeartbeat(ctx, heartbeat)
	if err != nil {
		if errors.Is(err, statestore.ErrMembershipConflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if errors.Is(err, statestore.ErrInvalidHeartbeat) {
			s.metrics.Heartbeat(cpmetrics.HeartbeatInvalid)
			return nil, status.Error(codes.InvalidArgument, "invalid heartbeat")
		}
		s.metrics.Heartbeat(cpmetrics.HeartbeatError)
		s.log.ErrorContext(ctx, "store heartbeat failed", "node_id", heartbeat.NodeID, "region", heartbeat.Region, "error", err)
		return nil, status.Error(codes.Unavailable, "heartbeat store unavailable")
	}
	s.metrics.Heartbeat(cpmetrics.HeartbeatAccepted)
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

// GetConfig выдаёт Gateway live-view при той же revision. Во время старта CP
// может вернуть только revision, пока назначенные реплики собирают наблюдения.
func (s *Server) GetConfig(ctx context.Context, req *controlplanev1.GetConfigRequest) (*controlplanev1.GetConfigResponse, error) {
	start := time.Now()
	defer func() { s.getConfigDuration.Observe(time.Since(start).Seconds()) }()
	if req == nil {
		s.metrics.ConfigPull(cpmetrics.PullError)
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

	gateway := req.GetKind() == controlplanev1.AgentKind_AGENT_KIND_GATEWAY
	var (
		snapshot  statestore.Snapshot
		err       error
		warmingUp bool
	)
	if gateway {
		now := s.clock.Now()
		snapshot, err = s.store.LiveConfig(ctx, now.UTC(), s.nodeTTL)
		warmingUp = snapshot.MissingReplicaHeartbeats && now.Before(s.startedAt.Add(s.nodeTTL))
	} else {
		snapshot, err = s.store.Config(ctx)
	}
	if err != nil {
		s.metrics.ConfigPull(cpmetrics.PullError)
		s.log.ErrorContext(ctx, "store config failed", "node_id", req.GetNodeId(), "error", err)
		return nil, status.Error(codes.Unavailable, "config store unavailable")
	}
	if req.GetKnownRevision() > snapshot.Revision {
		s.metrics.ConfigPull(cpmetrics.PullAhead)
		return nil, status.Errorf(codes.FailedPrecondition, "known revision %d is ahead of CP revision %d", req.GetKnownRevision(), snapshot.Revision)
	}
	if snapshot.Config == nil {
		if req.GetKnownRevision() == snapshot.Revision {
			return &controlplanev1.GetConfigResponse{Revision: snapshot.Revision}, nil
		}
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
	if req.GetKnownRevision() == snapshot.Revision && (!gateway || warmingUp) {
		s.metrics.ConfigPull(cpmetrics.PullUnchanged)
		return &controlplanev1.GetConfigResponse{Revision: snapshot.Revision}, nil
	}
	response := &controlplanev1.GetConfigResponse{
		Revision: snapshot.Revision,
		Config:   snapshot.Config.ToProto(),
	}
	s.metrics.ConfigPull(cpmetrics.PullServed)
	s.metrics.SetConfigRevision(snapshot.Revision)
	s.metrics.SetLiveNodes(countNodeStates(snapshot.Config))
	s.log.DebugContext(ctx, "config served",
		"node_id", req.GetNodeId(),
		"kind", req.GetKind().String(),
		"known_revision", req.GetKnownRevision(),
		"revision", response.Revision,
	)
	return response, nil
}

// countNodeStates считает ноды по состояниям live-view для метрики.
func countNodeStates(config *cpconfig.Config) map[string]uint64 {
	counts := map[string]uint64{}
	if config == nil {
		return counts
	}
	for _, node := range config.Nodes {
		switch node.State {
		case cpconfig.NodeStateReady:
			counts[cpmetrics.NodeStateReady]++
		case cpconfig.NodeStateStarting:
			counts[cpmetrics.NodeStateStarting]++
		case cpconfig.NodeStateDraining:
			counts[cpmetrics.NodeStateDraining]++
		case cpconfig.NodeStateDead:
			counts[cpmetrics.NodeStateDead]++
		default:
			counts[cpmetrics.NodeStateOther]++
		}
	}
	return counts
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
