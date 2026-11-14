// cp-api serves the stateless Control Plane API over plaintext gRPC.
//
// Phase 1 keeps state in memory. Production replaces it with etcd behind the
// same statestore.Store boundary; see
// docs/architecture/ADR-001-cp-phase1-in-memory-store.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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

func newServer(store statestore.Store, logger *slog.Logger) *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	controlplanev1.RegisterControlPlaneAPIServer(server, cpapi.New(store, cpapi.WithLogger(logger)))
	return server
}

// statusHandler serves a read-only JSON view of the last heartbeat of every node
// in the loaded config. Heartbeats are soft state; this is for diagnostics only.
func statusHandler(store statestore.Store) http.Handler {
	type partition struct {
		Partition uint32 `json:"partition"`
		Role      string `json:"role"`
		Term      uint64 `json:"term"`
		Commit    uint64 `json:"commit_index"`
		Applied   uint64 `json:"applied_index"`
		Leader    string `json:"leader_id"`
		Ready     bool   `json:"ready"`
		LastError string `json:"last_error,omitempty"`
	}
	type node struct {
		NodeID     string      `json:"node_id"`
		ReceivedAt time.Time   `json:"received_at"`
		Partitions []partition `json:"partitions"`
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot, err := store.Config(r.Context())
		if err != nil || snapshot.Config == nil {
			http.Error(w, "no config", http.StatusServiceUnavailable)
			return
		}
		out := struct {
			ConfigRevision uint64 `json:"config_revision"`
			Nodes          []node `json:"nodes"`
		}{ConfigRevision: snapshot.Revision, Nodes: []node{}}
		for _, n := range snapshot.Config.Nodes {
			hb, ok, err := store.Heartbeat(r.Context(), n.ID)
			if err != nil || !ok {
				continue
			}
			view := node{NodeID: n.ID, ReceivedAt: hb.ReceivedAt}
			for _, p := range hb.Partitions {
				view.Partitions = append(view.Partitions, partition{p.PartitionID, p.Role.String(), p.Term, p.CommitIndex, p.AppliedIndex, p.LeaderID, p.Ready, p.LastError})
			}
			out.Nodes = append(out.Nodes, view)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
}

func main() {
	listen := flag.String("listen", "127.0.0.1:50052", "plaintext gRPC listen address (use port 0 for an ephemeral port)")
	configPath := flag.String("config", "", "optional JSON config snapshot loaded into the in-memory store")
	statusListen := flag.String("status-listen", "", "optional HTTP address serving read-only heartbeat diagnostics at /heartbeats")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
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
	server := newServer(store, logger)

	if *statusListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/heartbeats", statusHandler(store))
		go func() {
			srv := &http.Server{Addr: *statusListen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
			if err := srv.ListenAndServe(); err != nil {
				logger.Error("status server", "error", err)
			}
		}()
	}

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
