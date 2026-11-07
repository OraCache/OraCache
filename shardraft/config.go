// Package shardraft runs one in-memory replica of a static Raft group.
// An Instance becomes leader or follower through elections, not configuration.
package shardraft

import (
	"context"
	"fmt"
	"sort"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"go.etcd.io/raft/v3"
	"google.golang.org/grpc"
)

type ReadMode uint8

const (
	// Linearizable reads require the leader, a quorum ReadIndex and local apply.
	Linearizable ReadMode = iota
	// Local reads use this replica's state and may be arbitrarily stale.
	Local
)

type Peer struct {
	ID uint64
	// Address is an optional client RPC address used in NotLeader hints.
	// Raft transport addresses belong to the transport configuration.
	Address string
}

type Config struct {
	RegionID string
	ShardID  uint32
	// GroupID must uniquely identify this incarnation of the group.
	GroupID         string
	PeerID          uint64
	Peers           []Peer
	ReadMode        ReadMode
	TickInterval    time.Duration
	ElectionTicks   int
	HeartbeatTicks  int
	RequestTimeout  time.Duration
	SendTimeout     time.Duration
	MaxPending      int
	MaxCommandBytes int
	// SnapshotEntries triggers an in-memory checkpoint and log compaction.
	// Zero selects the default (1000 applied entries).
	SnapshotEntries uint64
	Logger          raft.Logger
}

func (c Config) normalized() (Config, error) {
	if c.RegionID == "" || c.GroupID == "" || c.PeerID == 0 || len(c.Peers) == 0 {
		return c, fmt.Errorf("region, group, nonzero peer ID and peers are required")
	}
	if c.ReadMode != Linearizable && c.ReadMode != Local {
		return c, fmt.Errorf("unknown read mode %d", c.ReadMode)
	}
	seen := make(map[uint64]bool)
	c.Peers = append([]Peer(nil), c.Peers...)
	sort.Slice(c.Peers, func(i, j int) bool { return c.Peers[i].ID < c.Peers[j].ID })
	for _, p := range c.Peers {
		if p.ID == 0 || raft.IsLocalMsgTarget(p.ID) || seen[p.ID] {
			return c, fmt.Errorf("peer IDs must be nonzero, unique and not reserved by Raft")
		}
		seen[p.ID] = true
	}
	if !seen[c.PeerID] {
		return c, fmt.Errorf("local peer must belong to the initial configuration")
	}
	if c.TickInterval == 0 {
		c.TickInterval = 100 * time.Millisecond
	}
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = 1
	}
	if c.ElectionTicks == 0 {
		c.ElectionTicks = 10
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.SendTimeout == 0 {
		c.SendTimeout = time.Second
	}
	if c.MaxPending == 0 {
		c.MaxPending = 256
	}
	if c.MaxCommandBytes == 0 {
		c.MaxCommandBytes = 16 << 20
	}
	if c.SnapshotEntries == 0 {
		c.SnapshotEntries = 1000
	}
	if c.TickInterval <= 0 || c.HeartbeatTicks <= 0 || c.ElectionTicks <= c.HeartbeatTicks ||
		c.RequestTimeout <= 0 || c.SendTimeout <= 0 || c.MaxPending < 1 || c.MaxCommandBytes < 1024 || c.MaxCommandBytes > maxWireBytes-(1<<20) {
		return c, fmt.Errorf("invalid timeout, election, queue or command-size configuration")
	}
	return c, nil
}

// StateMachine must produce identical results for identical committed commands.
// Instance serializes every call, including reads, checkpoint and restore.
// No other component may mutate the supplied cache. Get must be read-only.
// Context cancellation of a client never cancels a committed mutation.
type StateMachine interface {
	Get(context.Context, *pb.GetRequest, ...grpc.CallOption) (*pb.GetResponse, error)
	Put(context.Context, *pb.PutRequest, ...grpc.CallOption) (*pb.PutResponse, error)
	Delete(context.Context, *pb.DeleteRequest, ...grpc.CallOption) (*pb.DeleteResponse, error)
	Checkpoint() ([]byte, error)
	Restore([]byte) error
}

// LocalCache is the equivalent in-process interface without gRPC call options.
type LocalCache interface {
	Get(context.Context, *pb.GetRequest) (*pb.GetResponse, error)
	Put(context.Context, *pb.PutRequest) (*pb.PutResponse, error)
	Delete(context.Context, *pb.DeleteRequest) (*pb.DeleteResponse, error)
	Checkpoint() ([]byte, error)
	Restore([]byte) error
}

// AdaptCache adapts a local cache, such as internal/cache.Server.
func AdaptCache(c LocalCache) StateMachine { return localCacheAdapter{c} }

type localCacheAdapter struct{ LocalCache }

func (a localCacheAdapter) Get(ctx context.Context, r *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	return a.LocalCache.Get(ctx, r)
}
func (a localCacheAdapter) Put(ctx context.Context, r *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	return a.LocalCache.Put(ctx, r)
}
func (a localCacheAdapter) Delete(ctx context.Context, r *pb.DeleteRequest, _ ...grpc.CallOption) (*pb.DeleteResponse, error) {
	return a.LocalCache.Delete(ctx, r)
}

// Transport delivers serialized raftpb.Message bytes to the target's Step.
// Send must honor ctx; nil means the receiver accepted the message, not commit.
// Instance owns bounded per-peer send queues; Transport may perform blocking IO.
// The caller owns transport resources and closes them after Instance stops.
type Transport interface {
	Send(ctx context.Context, group string, to uint64, data []byte) error
}

type Status struct {
	RegionID      string
	ShardID       uint32
	GroupID       string
	PeerID        uint64
	Role          string
	Term          uint64
	LeaderID      uint64
	CommitIndex   uint64
	AppliedIndex  uint64
	SnapshotIndex uint64
	// Ready means initialized and applied through a known committed position.
	// It is not a promise of freshness or current quorum availability.
	Ready     bool
	Running   bool
	LastError string
}
