// Package node is the application layer inside one storage-node process. It
// wraps the versioned store and one member of the static shard Raft group:
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
	"sync"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpagent"
	"github.com/OraCache/oracache/internal/store"
	"github.com/OraCache/oracache/shardraft"
	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Peer describes one member of the static group. The list is identical on all
// processes; only Config.PeerID differs.
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

	Store store.Config

	// Raft timing; zero values select shardraft defaults.
	TickInterval    time.Duration
	ElectionTicks   int
	RequestTimeout  time.Duration
	SnapshotEntries uint64

	Logger *slog.Logger
}

// Node is one storage-node module. Create with New, run with Run.
type Node struct {
	cfg      Config
	log      *slog.Logger
	store    *store.Store
	inst     *shardraft.Instance
	grpcLis  net.Listener
	raftLis  net.Listener
	httpSrv  *http.Server
	grpcSrv  *grpc.Server
	agent    *cpagent.Agent
	cpConn   *grpc.ClientConn
	httpCli  *http.Client
	peerNode map[uint64]string
	once     sync.Once
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
	n := &Node{cfg: cfg, log: cfg.Logger.With("node", cfg.NodeID), peerNode: map[uint64]string{}}

	raftPeers := make([]shardraft.Peer, 0, len(cfg.Peers))
	addrs := make(map[uint64]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		if p.NodeID == "" || p.ClientAddr == "" || p.RaftURL == "" {
			return nil, fmt.Errorf("node: peer %d needs node ID, client address and raft URL", p.ID)
		}
		raftPeers = append(raftPeers, shardraft.Peer{ID: p.ID, Address: p.ClientAddr})
		addrs[p.ID] = p.RaftURL
		n.peerNode[p.ID] = p.NodeID
	}
	if n.peerNode[cfg.PeerID] != cfg.NodeID {
		return nil, fmt.Errorf("node: peer %d is not %q in the peer list", cfg.PeerID, cfg.NodeID)
	}

	var err error
	if n.store, err = store.New(cfg.Store); err != nil {
		return nil, err
	}
	n.httpCli = &http.Client{Timeout: 2 * time.Second}
	transport, err := shardraft.NewHTTPTransport(addrs, n.httpCli)
	if err != nil {
		return nil, err
	}
	n.inst, err = shardraft.NewInstance(shardraft.Config{
		RegionID: cfg.Region, ShardID: cfg.ShardID, GroupID: cfg.GroupID,
		PeerID: cfg.PeerID, Peers: raftPeers, ReadMode: cfg.ReadMode,
		TickInterval: cfg.TickInterval, ElectionTicks: cfg.ElectionTicks,
		RequestTimeout: cfg.RequestTimeout, SnapshotEntries: cfg.SnapshotEntries,
		Logger: &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}, NewStateMachine(n.store), transport)
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
		n.cpConn, err = grpc.NewClient(cfg.CPAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(n.inst.Status())
	})
	n.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	n.grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	pb.RegisterCacheGatewayServer(n.grpcSrv, shardraft.GatewayServer(n.inst))
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

// PartitionStatuses implements cpagent.StatusProvider with the actual status.
func (n *Node) PartitionStatuses(context.Context) ([]*controlplanev1.PartitionStatus, error) {
	s := n.inst.Status()
	ps := &controlplanev1.PartitionStatus{
		PartitionId:  s.ShardID,
		Role:         protoRole(s.Role),
		Term:         s.Term,
		CommitIndex:  s.CommitIndex,
		AppliedIndex: s.AppliedIndex,
		Ready:        s.Ready && s.Running,
		LastError:    s.LastError,
	}
	if s.LeaderID != 0 {
		ps.LeaderId = n.peerNode[s.LeaderID]
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
		go func() { defer wg.Done(); _ = n.agent.Run(ctx) }()
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
	})
}
