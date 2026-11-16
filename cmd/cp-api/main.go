// cp-api serves the stateless Control Plane API over plaintext gRPC.
//
// Phase 1 keeps state in memory. Production replaces it with etcd behind the
// same statestore.Store boundary; see
// docs/architecture/ADR-001-cp-phase1-in-memory-store.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc"
)

func newServer(store statestore.Store, logger *slog.Logger, opts ...cpapi.Option) *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	opts = append(opts, cpapi.WithLogger(logger))
	controlplanev1.RegisterControlPlaneAPIServer(server, cpapi.New(store, opts...))
	return server
}

func main() {
	listen := flag.String("listen", "127.0.0.1:50052", "plaintext gRPC listen address (use port 0 for an ephemeral port)")
	configPath := flag.String("config", "", "optional JSON config snapshot loaded into the in-memory store")
	discoveryRegion := flag.String("discovery-region", "", "opt-in discovery region; requires --discovery-group")
	discoveryGroup := flag.String("discovery-group", "", "static Raft group identity, not membership")
	discoveryTTL := flag.Duration("discovery-ttl", 20*time.Second, "receiver-side discovery lease")
	heartbeat := flag.Duration("heartbeat-interval", 5*time.Second, "heartbeat interval advertised in discovery mode")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	opts, _, err := discoveryOptions(*discoveryRegion, *discoveryGroup, *configPath, *discoveryTTL, *heartbeat)
	if err != nil {
		logger.Error("discovery settings", "error", err)
		os.Exit(2)
	}
	store := statestore.NewMemory()
	revision := uint64(0)
	if *configPath != "" {
		cfg, err := cpconfig.LoadFile(*configPath)
		if err != nil {
			logger.Error("load bootstrap config", "path", *configPath, "error", err)
			os.Exit(1)
		}
		if err := store.ApplyConfig(context.Background(), cfg); err != nil {
			logger.Error("apply bootstrap config", "path", *configPath, "error", err)
			os.Exit(1)
		}
		revision = cfg.Revision
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen", "address", *listen, "error", err)
		os.Exit(1)
	}
	server := newServer(store, logger, opts...)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		<-signals
		server.GracefulStop()
	}()

	logger.Info("cp-api listening", "address", listener.Addr().String(), "config_revision", revision)
	if err := server.Serve(listener); err != nil {
		logger.Error("serve", "error", err)
		os.Exit(1)
	}
}
