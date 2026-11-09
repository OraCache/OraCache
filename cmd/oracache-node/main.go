// oracache-node — PoC Storage Node. В Phase 1 это heartbeat-источник для
// Control Plane: нода стартует со статической топологией внутри шарда, сообщает
// в CP API свой регион, локальные partition-группы и готовность к чтению/записи.
//
// Диапазоны партиций задаются флагами, поэтому воспроизводимый сценарий не
// требует писать 128 JSON-записей вручную.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpagent"
	"github.com/OraCache/oracache/internal/partition"
)

func main() {
	var (
		nodeID      = flag.String("node-id", "", "node identifier known to Control Plane")
		region      = flag.String("region", "", "region identifier known to Control Plane")
		instance    = flag.String("instance", "", "optional instance label for diagnostics")
		cpAddr      = flag.String("cp-addr", "127.0.0.1:50052", "Control Plane API address")
		partitions  = flag.String("partitions", "all", "partition ids or spec (all/even/odd/1-9/0,2,5-9) this node holds")
		leaders     = flag.String("leader-partitions", "all", "subset of -partitions this node leads")
		term        = flag.Uint64("term", 1, "Raft term reported in heartbeat")
		hbInterval  = flag.Duration("heartbeat-interval", cpagent.DefaultHeartbeatInterval, "heartbeat interval")
		statusAddr  = flag.String("status-listen", "127.0.0.1:9465", `HTTP address for /partitions and /healthz ("none" disables)`)
		clusterSpec = flag.Uint("cluster-partitions", uint(partition.DefaultCount), "cluster partition count")
	)
	flag.Parse()
	if *nodeID == "" || *region == "" {
		fmt.Fprintln(os.Stderr, "-node-id and -region are required")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	held, err := partition.ParseSpec(*partitions, uint32(*clusterSpec))
	if err != nil {
		logger.Error("parse partitions", "spec", *partitions, "error", err)
		os.Exit(1)
	}
	led, err := partition.ParseSpec(*leaders, uint32(*clusterSpec))
	if err != nil {
		logger.Error("parse leader-partitions", "spec", *leaders, "error", err)
		os.Exit(1)
	}
	ledSet := make(map[uint32]struct{}, len(led))
	for _, id := range led {
		ledSet[id] = struct{}{}
	}

	conn, err := grpc.NewClient(*cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("dial cp", "address", *cpAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	provider := statusProvider{nodeID: *nodeID, region: *region, held: held, led: ledSet, term: *term}
	agent, err := cpagent.New(cpagent.Options{
		NodeID:            *nodeID,
		Region:            *region,
		Instance:          *instance,
		Client:            controlplanev1.NewControlPlaneAPIClient(conn),
		Statuses:          provider,
		HeartbeatInterval: *hbInterval,
		Logger:            logger,
	})
	if err != nil {
		logger.Error("create cp agent", "error", err)
		os.Exit(1)
	}

	stopStatus := serveStatus(*statusAddr, provider, *nodeID, *region, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	intervalNote := "cp-hint"
	if *hbInterval > 0 {
		intervalNote = "fixed"
	}
	logger.Info("node started",
		"node_id", *nodeID,
		"region", *region,
		"cp_addr", *cpAddr,
		"partitions", len(held),
		"leaders", len(ledSet),
		"heartbeat_interval", *hbInterval,
		"heartbeat_mode", intervalNote,
	)
	if err := agent.Run(ctx); err != nil {
		logger.Error("cp agent stopped", "error", err)
		stopStatus()
		os.Exit(1)
	}
	stopStatus()
}

// statusProvider builds heartbeat payloads from the static local topology.
type statusProvider struct {
	nodeID string
	region string
	held   []uint32
	led    map[uint32]struct{}
	term   uint64
}

func (p statusProvider) PartitionStatuses(context.Context) ([]*controlplanev1.PartitionStatus, error) {
	out := make([]*controlplanev1.PartitionStatus, 0, len(p.held))
	for _, id := range p.held {
		status := &controlplanev1.PartitionStatus{
			PartitionId: id,
			Role:        controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER,
			Term:        p.term,
			Ready:       true,
		}
		if _, ok := p.led[id]; ok {
			status.Role = controlplanev1.RaftRole_RAFT_ROLE_LEADER
			status.LeaderId = p.nodeID
		}
		out = append(out, status)
	}
	return out, nil
}

// serveStatus exposes the local topology for inspection. Ошибка listener'а не
// фатальна: heartbeat в CP важнее локальной диагностики.
func serveStatus(listen string, provider statusProvider, nodeID, region string, logger *slog.Logger) func() {
	if listen == "" || listen == "none" {
		return func() {}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/partitions", func(w http.ResponseWriter, _ *http.Request) {
		statuses, err := provider.PartitionStatuses(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"node_id":    nodeID,
			"region":     region,
			"partitions": statuses,
		})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		logger.Error("status listen", "address", listen, "error", err)
		return func() {}
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("status server stopped", "address", listen, "error", err)
		}
	}()
	logger.Info("node status listening", "address", listener.Addr().String())
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

func writeJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(payload)
}
