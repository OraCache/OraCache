// cp-api обслуживает stateless API Control Plane по plaintext gRPC.
//
// По умолчанию используется in-memory PoC. Для общего состояния CP API
// поддерживает etcd за той же границей statestore.Store.
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
	mux := http.NewServeMux()
	mux.Handle("/metrics", api.Metrics().Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	metricsListener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	go func() {
		if err := http.Serve(metricsListener, mux); err != nil {
			logger.Error("metrics server stopped", "address", listen, "error", err)
		}
	}()
	logger.Info("cp-api metrics listening", "address", metricsListener.Addr().String())
	return nil
}

func main() {
	var options storeOptions
	flag.StringVar(&options.backend, "store", "memory", "StateStore backend: memory or etcd")
	flag.StringVar(&options.endpoints, "etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints (-store etcd)")
	flag.StringVar(&options.prefix, "etcd-prefix", "/oracache", "etcd namespace (-store etcd)")
	flag.StringVar(&options.configPath, "config", "", "JSON snapshot for an empty store")
	flag.DurationVar(&options.nodeTTL, "node-ttl", cpapi.DefaultNodeTTL, "node liveness TTL without heartbeat")
	flag.BoolVar(&options.devCluster, "dev-cluster", false, "bootstrap a synthetic 2-region, 2-node-per-region, 128-partition cluster")
	listen := flag.String("listen", "127.0.0.1:50052", "plaintext gRPC listen address (use port 0 for an ephemeral port)")
	metricsListen := flag.String("metrics-listen", defaultMetricsListen, `HTTP listen address for /metrics and /healthz ("none" disables)`)
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if *metricsListen == "none" {
		*metricsListen = ""
	}

	if options.devCluster && options.configPath != "" {
		fmt.Fprintln(os.Stderr, "-dev-cluster and -config are mutually exclusive")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(*listen, *metricsListen, options, logger); err != nil {
		logger.Error("cp-api stopped", "error", err)
		os.Exit(1)
	}
}

func run(listen, metricsListen string, options storeOptions, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	store, revision, closeStore, err := openStore(ctx, options)
	cancel()
	if err != nil {
		return err
	}
	defer closeStore()

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("gRPC listen: %w", err)
	}
	defer listener.Close()
	server, api := newServer(store, logger, cpapi.WithNodeTTL(options.nodeTTL))
	if err := serveMetrics(metricsListen, api, logger); err != nil {
		return fmt.Errorf("metrics listen: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	logger.Info("cp-api listening", "address", listener.Addr().String(), "store", options.backend, "config_revision", revision)
	return server.Serve(listener)
}
