// Package node is the application layer inside one storage-node process. It
// wraps the versioned store and one member of the shard Raft group:
// config and constructors, the client gRPC API delegating to shardraft, the
// HTTP Raft transport, the CP-agent heartbeat and lifecycle.
//
// There is deliberately no network hop between Raft and the store: Raft applies
// committed commands to the store in-process.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	membershipv1 "github.com/OraCache/oracache/gen/membership/v1"
	replicationv1 "github.com/OraCache/oracache/gen/replication/v1"
	"github.com/OraCache/oracache/internal/cpagent"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/replication"
	"github.com/OraCache/oracache/internal/store"
	"github.com/OraCache/oracache/shardraft"
	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Peer describes an initial member or a seed for a joining process. Bootstrap
// processes use identical lists; joining processes learn the committed registry.
type Peer struct {
	ID         uint64 // Raft ID, nonzero
	NodeID     string // Control Plane node ID, e.g. "node-1"
	ClientAddr string // client gRPC address as reachable by gateways (NOT_LEADER hint)
	RaftURL    string // Raft HTTP endpoint as reachable by the other members
}

type Config struct {
	NodeID   string
	Region   string
	ShardID  uint32
	GroupID  string // identifies this incarnation of the group
	PeerID   uint64
	Peers    []Peer
	Join     bool               // Peers are seeds; start empty and wait for AddLearner.
	ReadMode shardraft.ReadMode // zero value: Linearizable

	// Listen addresses; ignored when the matching Listener is supplied.
	GRPCAddr     string
	RaftAddr     string
	GRPCListener net.Listener
	RaftListener net.Listener

	// CPAddr enables the CP agent. Empty disables heartbeats.
	CPAddr             string
	HeartbeatInterval  time.Duration
	ConfigPullInterval time.Duration

	Store       store.Config
	Replication replication.Config

	// Zero values select shardraft defaults.
	TickInterval     time.Duration
	ElectionTicks    int
	RequestTimeout   time.Duration
	SnapshotEntries  uint64
	MaxRaftWireBytes int64

	Logger *slog.Logger
}

// Node is one storage-node module. Create with New, run with Run.
type Node struct {
	cfg     Config
	log     *slog.Logger
	store   *store.Store
	inst    *shardraft.Instance
	grpcLis net.Listener
	raftLis net.Listener
	httpSrv *http.Server
	grpcSrv *grpc.Server
	agent   *cpagent.Agent
	cpConn  *grpc.ClientConn
	httpCli *http.Client
	once    sync.Once
	sender  *replication.Sender
}

// New validates the configuration, binds the listeners and builds the store and
// the Raft instance. It starts no goroutines; call Run.
func New(cfg Config) (*Node, error) {
	if cfg.NodeID == "" || cfg.Region == "" || cfg.GroupID == "" {
		return nil, errors.New("node: node ID, region and group ID are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.CPAddr != "" && cfg.ReadMode != shardraft.Linearizable {
		return nil, errors.New("node: Control Plane integration requires linearizable reads")
	}
	if cfg.Replication.TargetRegion != "" && cfg.CPAddr == "" {
		return nil, errors.New("node: outgoing replication requires Control Plane discovery")
	}
	replState, err := replication.NewState(replication.StateOptions{
		Region: cfg.Region, ShardID: cfg.ShardID, GroupID: cfg.GroupID,
		Config: cfg.Replication,
	})
	if err != nil {
		return nil, err
	}
	cfg.Replication = replState.Config()
	rc := &cfg.Replication

	n := &Node{cfg: cfg, log: cfg.Logger.With("node", cfg.NodeID)}

	raftPeers := make([]shardraft.Peer, 0, len(cfg.Peers))
	addrs := make(map[uint64]string, len(cfg.Peers))
	local := false
	for _, p := range cfg.Peers {
		if p.NodeID == "" || p.ClientAddr == "" || p.RaftURL == "" {
			return nil, fmt.Errorf("node: peer %d needs node ID, client address and raft URL", p.ID)
		}
		raftPeers = append(raftPeers, shardraft.Peer{ID: p.ID, Address: p.ClientAddr, NodeID: p.NodeID, RaftAddress: p.RaftURL})
		addrs[p.ID] = p.RaftURL
		if p.ID == cfg.PeerID && p.NodeID == cfg.NodeID {
			local = true
		}
	}
	if !local {
		return nil, fmt.Errorf("node: peer %d is not %q in the peer list", cfg.PeerID, cfg.NodeID)
	}

	if n.store, err = store.New(cfg.Store); err != nil {
		return nil, err
	}
	n.httpCli = &http.Client{Timeout: 2 * time.Second}
	transport, err := shardraft.NewHTTPTransport(addrs, n.httpCli)
	if err != nil {
		return nil, err
	}
	metrics := cpmetrics.NewRegistry()
	metrics.RegisterProcessCollectors()
	raftMetrics := newRaftMetrics(metrics.Prometheus(), n)
	sm := NewStateMachine(n.store, replState)
	if rc.TargetRegion != "" {
		registerOutboxMetrics(metrics.Prometheus(), cfg, replState.Config().OutboxLimit, sm)
	}
	n.inst, err = shardraft.NewInstance(shardraft.Config{
		RegionID: cfg.Region, ShardID: cfg.ShardID, GroupID: cfg.GroupID,
		PeerID: cfg.PeerID, Peers: raftPeers, Join: cfg.Join, ReadMode: cfg.ReadMode,
		TickInterval: cfg.TickInterval, ElectionTicks: cfg.ElectionTicks,
		RequestTimeout: cfg.RequestTimeout, SnapshotEntries: cfg.SnapshotEntries, MaxWireBytes: cfg.MaxRaftWireBytes,
		Observer: raftMetrics,
		Logger:   &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}, sm, transport)
	if err != nil {
		return nil, err
	}

	if n.grpcLis, err = listener(cfg.GRPCListener, cfg.GRPCAddr); err != nil {
		return nil, err
	}
	if n.raftLis, err = listener(cfg.RaftListener, cfg.RaftAddr); err != nil {
		n.grpcLis.Close()
		return nil, err
	}

	if cfg.CPAddr != "" {
		n.cpConn, err = grpc.NewClient(cfg.CPAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDisableServiceConfig())
		if err != nil {
			n.grpcLis.Close()
			n.raftLis.Close()
			return nil, fmt.Errorf("node: control plane client: %w", err)
		}
		n.agent, err = cpagent.New(cpagent.Options{
			NodeID: cfg.NodeID, Region: cfg.Region, Instance: cfg.NodeID,
			Client: controlplanev1.NewControlPlaneAPIClient(n.cpConn), Statuses: n,
			HeartbeatInterval: cfg.HeartbeatInterval, ConfigPullInterval: cfg.ConfigPullInterval,
			Logger: n.log,
		})
		if err != nil {
			n.grpcLis.Close()
			n.raftLis.Close()
			n.cpConn.Close()
			return nil, err
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/raft", n.inst.RaftHandler())
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/membership", n.membershipHTTP)
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(n.inst.Status())
	})
	n.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	interceptors := []grpc.UnaryServerInterceptor{cpmetrics.CacheRPCInterceptor(metrics.Prometheus())}
	if n.agent != nil {
		interceptors = append(interceptors, n.requireMasterForWrites)
	}
	n.grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.ChainUnaryInterceptor(interceptors...))
	pb.RegisterCacheGatewayServer(n.grpcSrv, shardraft.GatewayServer(n.inst))
	membershipv1.RegisterMembershipAdminServer(n.grpcSrv, &membershipServer{n: n})
	if rc.SourceRegion != "" {
		replicationv1.RegisterReplicationServiceServer(n.grpcSrv, &replication.Receiver{Destination: n.inst})
	}
	if rc.TargetRegion != "" {
		n.sender, err = replication.NewSender(
			n.inst,
			cfg.Region,
			rc.TargetRegion,
			cfg.GroupID,
			cfg.ShardID,
			n.replicationAddresses,
			n.log,
		)
		if err != nil {
			n.shutdown()
			return nil, err
		}
	}
	return n, nil
}

func listener(l net.Listener, addr string) (net.Listener, error) {
	if l != nil {
		return l, nil
	}
	return net.Listen("tcp", addr)
}

// GRPCAddr and RaftAddr return the bound listen addresses.
func (n *Node) GRPCAddr() string { return n.grpcLis.Addr().String() }
func (n *Node) RaftAddr() string { return n.raftLis.Addr().String() }

// Status returns the real Raft status of this member.
func (n *Node) Status() shardraft.Status { return n.inst.Status() }

// Store exposes the local store for diagnostics and tests; callers must not
// mutate it, because only Raft apply may change replicated state.
func (n *Node) Store() *store.Store { return n.store }

// Read the last accepted topology from memory. Raft still decides whether
// this node may propose and commit a write.
func (n *Node) requireMasterForWrites(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	switch info.FullMethod {
	case pb.CacheGateway_Put_FullMethodName, pb.CacheGateway_BatchPut_FullMethodName, pb.CacheGateway_Delete_FullMethodName:
		config := n.agent.Config()
		if config == nil {
			return nil, status.Error(codes.Unavailable, "storage node has not received its Control Plane configuration")
		}
		if config.MasterRegion() != n.cfg.Region {
			return nil, status.Error(codes.FailedPrecondition, "client writes require the master region")
		}
		node, assigned := config.NodeByID(n.cfg.NodeID)
		assignment, placed := config.Assignment(n.cfg.Region, n.cfg.ShardID)
		if assigned && (node.Region != n.cfg.Region || node.State == cpconfig.NodeStateDraining) {
			return nil, status.Error(codes.FailedPrecondition, "storage node has no active placement for this partition")
		}
		member := false
		if n.inst != nil {
			for _, peer := range n.inst.Membership().Members {
				if peer.ID == n.cfg.PeerID && peer.NodeID == n.cfg.NodeID && peer.Voter {
					member = true
					break
				}
			}
		} else {
			member = assigned && placed && slices.Contains(assignment.ReplicaNodeIDs, n.cfg.NodeID)
		}
		if !member {
			return nil, status.Error(codes.FailedPrecondition, "storage node has no active placement for this partition")
		}
	}
	return handler(ctx, req)
}

// PartitionStatuses implements cpagent.StatusProvider with the actual status.
func (n *Node) PartitionStatuses(context.Context) ([]*controlplanev1.PartitionStatus, error) {
	s, m := n.inst.HeartbeatState()
	ps := &controlplanev1.PartitionStatus{
		PartitionId:  s.ShardID,
		Role:         protoRole(s.Role),
		Term:         s.Term,
		CommitIndex:  s.CommitIndex,
		AppliedIndex: s.AppliedIndex,
		Ready:        s.Ready && s.Running && (n.agent == nil || n.agent.Config() != nil),
	}
	if s.LeaderID != 0 {
		for _, member := range m.Members {
			if member.ID == s.LeaderID {
				ps.LeaderId = member.NodeID
				break
			}
		}
	}
	if s.IsLearner {
		ps.Role = controlplanev1.RaftRole_RAFT_ROLE_LEARNER
	}
	if s.Role == "StateLeader" && m.Index > 0 && len(m.Members) > 0 {
		wire := &controlplanev1.GroupMembership{GroupId: m.GroupID, Region: n.cfg.Region, PartitionId: s.ShardID, ReporterPeerId: n.cfg.PeerID, ConfigIndex: m.Index, Joint: m.Joint, RemovedIds: m.RemovedIDs}
		for _, peer := range m.Members {
			wire.Members = append(wire.Members, &controlplanev1.GroupMember{PeerId: peer.ID, NodeId: peer.NodeID, Address: peer.Address, RaftAddress: peer.RaftAddress, Voter: peer.Voter, Learner: peer.Learner, OutgoingVoter: peer.OutgoingVoter})
		}
		ps.Membership = wire
	}
	return []*controlplanev1.PartitionStatus{ps}, nil
}

func protoRole(role string) controlplanev1.RaftRole {
	switch role {
	case "StateLeader":
		return controlplanev1.RaftRole_RAFT_ROLE_LEADER
	case "StateFollower":
		return controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER
	case "StateCandidate", "StatePreCandidate":
		return controlplanev1.RaftRole_RAFT_ROLE_CANDIDATE
	default:
		return controlplanev1.RaftRole_RAFT_ROLE_UNSPECIFIED
	}
}

// Run serves until ctx is cancelled or the Raft instance fails fatally, then
// shuts everything down. It returns the fatal error, if any. Run may be called
// once; a stopped node is never restarted into the group (no persistence).
func (n *Node) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	raftDone := make(chan error, 1)
	wg.Add(1)
	go func() { defer wg.Done(); raftDone <- n.inst.Run(ctx) }()
	serveErr := make(chan error, 2)
	go func() { serveErr <- n.grpcSrv.Serve(n.grpcLis) }()
	go func() { serveErr <- n.httpSrv.Serve(n.raftLis) }()
	if n.agent != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-n.inst.MembershipEvents():
					n.agent.TriggerHeartbeat()
				}
			}
		}()
		wg.Add(1)
		go func() { defer wg.Done(); _ = n.agent.Run(ctx) }()
	}
	if n.sender != nil {
		wg.Go(func() {
			n.sender.Run(ctx)
		})
	}
	n.log.Info("storage node started", "grpc", n.GRPCAddr(), "raft", n.RaftAddr(), "peer_id", n.cfg.PeerID)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-raftDone:
		raftDone <- runErr
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			runErr = err
		}
	}
	n.shutdown()
	cancel()
	wg.Wait()
	if err := <-raftDone; err != nil && runErr == nil {
		runErr = err
	}
	return runErr
}

// Close stops the node; it is safe to call concurrently with Run and repeatedly.
func (n *Node) Close() { n.shutdown() }

func (n *Node) shutdown() {
	n.once.Do(func() {
		stopped := make(chan struct{})
		go func() { n.grpcSrv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			n.grpcSrv.Stop()
		}
		_ = n.inst.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = n.httpSrv.Shutdown(ctx)
		_ = n.grpcLis.Close() // matters only if Run never started
		_ = n.raftLis.Close()
		n.httpCli.CloseIdleConnections()
		if n.cpConn != nil {
			_ = n.cpConn.Close()
		}
		if n.sender != nil {
			n.sender.Close()
		}
	})
}

// replicationAddresses uses the existing CP agent's last-known-good GetConfig
// snapshot. STORAGE_NODE views contain placement, not fresh leader observations:
// try the configured leader first, then the remaining replicas of this shard.
// This tolerates elections without changing the config revision or CP API.
func (n *Node) replicationAddresses() []string {
	c := n.agent.Config()
	if c == nil || c.MasterRegion() != n.cfg.Region {
		return nil
	}
	target := n.cfg.Replication.TargetRegion
	r, ok := c.RegionByID(target)
	if !ok || r.Role != cpconfig.RegionRoleSlave {
		return nil
	}
	a, ok := c.Assignment(target, n.cfg.ShardID)
	if !ok {
		return nil
	}
	var addresses []string
	seen := make(map[string]bool)
	for _, id := range append([]string{a.LeaderNodeID}, a.ReplicaNodeIDs...) {
		node, ok := c.NodeByID(id)
		if !ok || node.Region != target || node.Address == "" || node.State == cpconfig.NodeStateDead || node.State == cpconfig.NodeStateDraining || seen[node.Address] {
			continue
		}
		seen[node.Address] = true
		addresses = append(addresses, node.Address)
	}
	return addresses
}
