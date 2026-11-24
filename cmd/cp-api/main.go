// cp-api обслуживает stateless gRPC API Control Plane.
//
// По умолчанию используется in-memory PoC. Для общего состояния CP API
// поддерживает etcd за той же границей statestore.Store.
//
// Метрики Control Plane отдаются отдельным HTTP-портом в формате Prometheus
// text exposition. Пустая строка в -metrics-listen отключает endpoint.
package main

import (
	"context"
	"crypto/tls"
	"errors"
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
	"github.com/OraCache/oracache/internal/cplink"
	"github.com/OraCache/oracache/internal/statestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const defaultMetricsListen = "127.0.0.1:9464"

const storeMetricsInterval = time.Second

func newServer(store statestore.Store, logger *slog.Logger, opts ...cpapi.Option) (*grpc.Server, *cpapi.Server) {
	api := cpapi.New(store, append([]cpapi.Option{cpapi.WithLogger(logger)}, opts...)...)
	return registerServer(api), api
}

func registerServer(api *cpapi.Server, options ...grpc.ServerOption) *grpc.Server {
	options = append(options, grpc.MaxRecvMsgSize(16<<20))
	server := grpc.NewServer(options...)
	controlplanev1.RegisterControlPlaneAPIServer(server, api)
	return server
}

// serveMetrics поднимает /metrics. Ошибка listener'а возвращается, чтобы main
// завершился с понятным диагнозом, а не потерял endpoint молча.
func serveMetrics(ctx context.Context, listen string, api *cpapi.Server, logger *slog.Logger) (net.Addr, error) {
	if listen == "" {
		return nil, nil
	}
	api.Metrics().RegisterProcessCollectors()
	mux := http.NewServeMux()
	mux.Handle("/metrics", api.Metrics().Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !api.Ready(r.Context()) {
			http.Error(w, "config unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	metricsListener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	stopClose := context.AfterFunc(ctx, func() { _ = server.Close() })
	pollCtx, stopPolling := context.WithCancel(ctx)
	go func() {
		defer stopClose()
		defer stopPolling()
		if err := server.Serve(metricsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped", "address", listen, "error", err)
		}
	}()
	go func() {
		ticker := time.NewTicker(storeMetricsInterval)
		defer ticker.Stop()
		for pollCtx.Err() == nil {
			api.RefreshStoreMetrics(pollCtx)
			select {
			case <-pollCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	logger.Info("cp-api metrics listening", "address", metricsListener.Addr().String())
	return metricsListener.Addr(), nil
}

func main() {
	var options storeOptions
	flag.StringVar(&options.backend, "store", "memory", "StateStore backend: memory or etcd")
	flag.StringVar(&options.endpoints, "etcd-endpoints", "http://127.0.0.1:2379", "comma-separated etcd endpoints (-store etcd)")
	flag.StringVar(&options.prefix, "etcd-prefix", "/oracache", "etcd namespace (-store etcd)")
	flag.StringVar(&options.configPath, "config", "", "JSON snapshot for an empty store")
	flag.DurationVar(&options.nodeTTL, "node-ttl", cpapi.DefaultNodeTTL, "node liveness TTL without heartbeat")
	flag.BoolVar(&options.devCluster, "dev-cluster", false, "bootstrap a synthetic 2-region, 2-node-per-region, 128-partition cluster")
	flag.Uint64Var(&options.minimumRevision, "minimum-config-revision", 0, "reject configs older than this application revision")
	bootstrapOnly := flag.Bool("bootstrap-only", false, "initialize the configured StateStore and exit")
	recoveryRevision := flag.Uint64("recover-config-revision", 0, "offline etcd recovery: republish the restored config at this application revision and exit")
	listen := flag.String("listen", "127.0.0.1:50052", "gRPC listen address (use port 0 for an ephemeral port)")
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
	if *bootstrapOnly {
		if options.configPath == "" && !options.devCluster {
			logger.Error("bootstrap requires a config source")
			os.Exit(2)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, revision, closeStore, err := openStore(ctx, options)
		cancel()
		if err != nil {
			logger.Error("bootstrap config", "error", err)
			os.Exit(1)
		}
		closeStore()
		logger.Info("bootstrapped config", "config_revision", revision)
		return
	}
	if *recoveryRevision != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := recoverConfig(ctx, options, *recoveryRevision); err != nil {
			logger.Error("recover config", "error", err)
			os.Exit(1)
		}
		logger.Info("recovered config", "config_revision", *recoveryRevision)
		return
	}
	if err := run(*listen, *metricsListen, options, logger); err != nil {
		logger.Error("cp-api stopped", "error", err)
		os.Exit(1)
	}
}

func run(listen, metricsListen string, options storeOptions, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	store, revision, closeStore, err := startStore(ctx, options)
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
	api := cpapi.New(store, cpapi.WithLogger(logger), cpapi.WithNodeTTL(options.nodeTTL), cpapi.WithMinimumRevision(options.minimumRevision), cpapi.WithMasterReadsOnly(options.backend == "etcd"))
	tlsConfig, err := cplink.TLSFromEnv("ORACACHE_CP", true)
	if err != nil {
		return err
	}
	serverOptions, err := transportOptions(tlsConfig, cplink.Cluster())
	if err != nil {
		return err
	}
	server := registerServer(api, serverOptions...)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if _, err := serveMetrics(ctx, metricsListen, api, logger); err != nil {
		return fmt.Errorf("metrics listen: %w", err)
	}
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	logger.Info("cp-api listening", "address", listener.Addr().String(), "store", options.backend, "config_revision", revision)
	return server.Serve(listener)
}

func transportOptions(config *tls.Config, cluster string) ([]grpc.ServerOption, error) {
	options := []grpc.ServerOption{grpc.UnaryInterceptor(cplink.Authorize(cluster, config != nil))}
	if config != nil {
		if cluster == "" {
			return nil, fmt.Errorf("CP mTLS requires ORACACHE_CP_CLUSTER_ID")
		}
		options = append(options, grpc.Creds(credentials.NewTLS(config)))
	}
	return options, nil
}
