// cp-api обслуживает stateless API Control Plane по plaintext gRPC.
//
// В Phase 1 состояние хранится в памяти. В production его заменит etcd за той же
// границей statestore.Store; см.
// docs/architecture/ADR-001-cp-phase1-in-memory-store.md.
//
// Метрики Control Plane отдаются отдельным HTTP-портом в формате Prometheus
// text exposition. Пустая строка в -metrics-listen отключает endpoint.
package main

import (
	"context"
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

const defaultMetricsListen = "127.0.0.1:9464"

func newServer(store statestore.Store, logger *slog.Logger, opts ...cpapi.Option) (*grpc.Server, *cpapi.Server) {
	api := cpapi.New(store, append([]cpapi.Option{cpapi.WithLogger(logger)}, opts...)...)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	controlplanev1.RegisterControlPlaneAPIServer(server, api)
	return server, api
}

// serveMetrics поднимает /metrics. Ошибка listener'а возвращается, чтобы main
// завершился с понятным диагнозом, а не потерял endpoint молча.
func serveMetrics(listen string, api *cpapi.Server, logger *slog.Logger) error {
	if listen == "" {
		return nil
	}
	api.Metrics().RegisterProcessCollectors()
	mux := http.NewServeMux()
	mux.Handle("/metrics", api.Metrics().Handler())
	mux.HandleFunc("GET /discovery/storage-nodes", api.StorageDiscovery)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	metricsListener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := server.Serve(metricsListener); err != nil {
			logger.Error("metrics server stopped", "address", listen, "error", err)
		}
	}()
	logger.Info("cp-api metrics listening", "address", metricsListener.Addr().String())
	return nil
}

func main() {
	listen := flag.String("listen", "127.0.0.1:50052", "plaintext gRPC listen address (use port 0 for an ephemeral port)")
	configPath := flag.String("config", "", "optional JSON config snapshot loaded into the in-memory store")
	nodeTTL := flag.Duration("node-ttl", cpapi.DefaultNodeTTL, "node liveness TTL without heartbeat")
	devCluster := flag.Bool("dev-cluster", false, "load a synthetic 2-region, 2-node-per-region, 128-partition cluster instead of -config")
	metricsListen := flag.String("metrics-listen", defaultMetricsListen, `HTTP listen address for metrics, health and storage discovery ("none" disables)`)
	membershipFile := flag.String("membership-state", "", "optional Raft membership checkpoint file for storage discovery")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if *metricsListen == "none" {
		*metricsListen = ""
	}

	if *devCluster && *configPath != "" {
		fmt.Fprintln(os.Stderr, "-dev-cluster and -config are mutually exclusive")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	store, err := statestore.OpenMemory(*membershipFile)
	if err != nil {
		logger.Error("open Raft membership state", "error", err)
		os.Exit(1)
	}
	revision := uint64(0)
	if *devCluster {
		cfg := cpconfig.SyntheticDevCluster()
		if err := store.ApplyConfig(context.Background(), cfg); err != nil {
			logger.Error("apply dev cluster", "error", err)
			os.Exit(1)
		}
		revision = cfg.Revision
		logger.Info("loaded synthetic dev cluster", "partitions", cfg.PartitionCount, "regions", len(cfg.Regions))
	}
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
	server, api := newServer(store, logger, cpapi.WithNodeTTL(*nodeTTL))
	if err := serveMetrics(*metricsListen, api, logger); err != nil {
		logger.Error("metrics listen", "address", *metricsListen, "error", err)
		_ = listener.Close()
		os.Exit(1)
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
