// storage-node runs one member of the static single-shard Raft group with the
// versioned in-memory LRU store. State is memory only: a stopped node must not
// be restarted into a running group.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/shardraft"
)

type peerFlags []node.Peer

func (p *peerFlags) String() string { return fmt.Sprint(*p) }

// Set parses "id,node-id,client-grpc-address,raft-http-url".
func (p *peerFlags) Set(v string) error {
	parts := strings.SplitN(v, ",", 4)
	if len(parts) != 4 {
		return errors.New("want id,node-id,client-address,raft-url")
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || id == 0 {
		return fmt.Errorf("invalid peer id %q", parts[0])
	}
	*p = append(*p, node.Peer{ID: id, NodeID: parts[1], ClientAddr: parts[2], RaftURL: parts[3]})
	return nil
}

func main() {
	var peers peerFlags
	cfg := node.Config{ShardID: 0}
	var readMode string
	flag.StringVar(&cfg.NodeID, "node-id", "", "Control Plane node ID, e.g. node-1")
	flag.StringVar(&cfg.Region, "region", "region-a", "region ID")
	flag.StringVar(&cfg.GroupID, "group-id", "region-a/shard-0/poc-1", "Raft group incarnation ID; identical on all members")
	flag.Uint64Var(&cfg.PeerID, "peer-id", 0, "this member's Raft ID")
	flag.Var(&peers, "peer", "group member id,node-id,client-address,raft-url (repeat for every member, including this one)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-listen", "127.0.0.1:28091", "client gRPC listen address")
	flag.StringVar(&cfg.RaftAddr, "raft-listen", "127.0.0.1:28191", "Raft HTTP transport listen address")
	flag.StringVar(&cfg.CPAddr, "cp-addr", "", "Control Plane gRPC address; empty disables heartbeats")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", 2*time.Second, "CP heartbeat interval")
	flag.StringVar(&readMode, "read-mode", "linearizable", "linearizable (leader ReadIndex) or local (possibly stale)")
	flag.IntVar(&cfg.Store.MaxVersions, "max-versions", 0, "versions kept per key (0: store default)")
	flag.Int64Var(&cfg.Store.MaxBytes, "max-bytes", 0, "store memory budget in bytes (0: store default)")
	flag.DurationVar(&cfg.TickInterval, "tick", 0, "Raft tick (0: default 100ms)")
	flag.IntVar(&cfg.ElectionTicks, "election-ticks", 0, "Raft election ticks (0: default 10)")
	flag.Uint64Var(&cfg.SnapshotEntries, "snapshot-entries", 0, "applied entries between snapshots (0: default)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	cfg.Peers = peers
	switch readMode {
	case "linearizable":
		cfg.ReadMode = shardraft.Linearizable
	case "local":
		cfg.ReadMode = shardraft.Local
	default:
		fmt.Fprintln(os.Stderr, "unknown -read-mode")
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg.Logger = logger

	n, err := node.New(cfg)
	if err != nil {
		logger.Error("configure node", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := n.Run(ctx); err != nil {
		logger.Error("node stopped", "error", err)
		os.Exit(1)
	}
}
