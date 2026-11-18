// api-gateway — PoC target Gateway. Он периодически тянет состав кластера из
// Control Plane и держит routing map: локальный лидер региона для GET и лидер
// master-региона для PUT. Данные кэша он не хранит и в CP не обращается на
// каждом запросе: CP недоступен — Gateway работает по последнему удачному
// снимку.
//
// С флагом -listen Gateway пересылает клиентские RPC в storage-node.
// Диагностика доступна через /routes, /healthz и /metrics.
package main

import (
	"context"
	"encoding/json"
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	gatewayv1 "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpmetrics"
	"github.com/OraCache/oracache/internal/gateway"
)

const defaultStatusListen = "127.0.0.1:9466"

func main() {
	var (
		region       = flag.String("region", "", "local region of this gateway")
		nodeID       = flag.String("node-id", "", "gateway identifier reported to Control Plane")
		cpAddr       = flag.String("cp-addr", "127.0.0.1:50052", "Control Plane API address")
		pollInterval = flag.Duration("poll-interval", gateway.DefaultPollInterval, "config pull interval")
		rpcTimeout   = flag.Duration("rpc-timeout", gateway.DefaultRPCTimeout, "single CP RPC timeout")
		listen       = flag.String("listen", "", "client gRPC address (empty serves diagnostics only)")
		statusListen = flag.String("status-listen", defaultStatusListen, `HTTP address for /routes, /healthz and /metrics ("none" disables)`)
	)
	flag.Parse()
	if *region == "" || *nodeID == "" {
		fmt.Fprintln(os.Stderr, "-region and -node-id are required")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	router, err := gateway.NewRouter(*region)
	if err != nil {
		logger.Error("create router", "region", *region, "error", err)
		os.Exit(1)
	}
	conn, err := grpc.NewClient(*cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("dial cp", "address", *cpAddr, "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	poller := gateway.NewPoller(controlplanev1.NewControlPlaneAPIClient(conn), router, *nodeID)
	poller.Interval = *pollInterval
	poller.RPCTimeout = *rpcTimeout
	poller.Logger = logger

	var stopStatus func()
	if *statusListen != "" && *statusListen != "none" {
		statusListener, err := net.Listen("tcp", *statusListen)
		if err != nil {
			logger.Error("status listen", "address", *statusListen, "error", err)
			os.Exit(1)
		}
		stopStatus = serveStatus(statusListener, router, *region, logger)
	} else {
		stopStatus = func() {}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer stopStatus()

	var serveErr <-chan error
	if *listen != "" {
		listener, err := net.Listen("tcp", *listen)
		if err != nil {
			logger.Error("client gRPC listen", "address", *listen, "error", err)
			os.Exit(1)
		}
		done, stopRPC := serveGateway(listener, poller, logger)
		defer stopRPC()
		serveErr = done
		logger.Info("api-gateway client endpoint listening", "address", listener.Addr().String())
	}

	logger.Info("api-gateway started",
		"region", *region,
		"node_id", *nodeID,
		"cp_addr", *cpAddr,
		"poll_interval", poller.Interval.String(),
	)
	pollerDone := make(chan error, 1)
	go func() { pollerDone <- poller.Run(ctx) }()
	var runErr error
	select {
	case runErr = <-pollerDone:
	case runErr = <-serveErr:
		stop()
		<-pollerDone
	}
	if runErr != nil && !errors.Is(runErr, grpc.ErrServerStopped) {
		logger.Error("api-gateway stopped", "error", runErr)
		os.Exit(1)
	}
}

func serveGateway(listener net.Listener, poller *gateway.Poller, logger *slog.Logger) (<-chan error, func()) {
	forwarder := &gateway.Forwarder{Router: poller.Router, Logger: logger, OnNotLeader: poller.RequestRefresh}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	gatewayv1.RegisterCacheGatewayServer(server, forwarder)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	return done, func() {
		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-stopped:
		case <-timer.C:
			server.Stop()
			<-stopped
		}
		forwarder.Close()
	}
}

// newStatusHandler собирает диагностические эндпоинты routing map.
func newStatusHandler(router *gateway.Router) http.Handler {
	mux := http.NewServeMux()
	metrics := cpmetrics.NewRegistry()
	metrics.RegisterGaugeFunc("oracache_gateway_partitions_without_route", "Partitions without a route in the applied Gateway map.", func(context.Context) ([]cpmetrics.GaugeSample, error) {
		snapshot := router.Routes()
		if !snapshot.Ready() {
			return nil, nil
		}
		return []cpmetrics.GaugeSample{
			{Labels: map[string]string{"operation": "get"}, Value: float64(snapshot.MissingReads())},
			{Labels: map[string]string{"operation": "put"}, Value: float64(snapshot.MissingWrites())},
		}, nil
	})
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/routes", func(w http.ResponseWriter, _ *http.Request) {
		snapshot := router.Routes()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(snapshot)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if !router.Routes().Ready() {
			http.Error(w, "routing map not loaded\n", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// serveStatus обслуживает диагностические эндпоинты routing map на переданном
// listener'е. Возвращает функцию остановки; вызов её безопасен повторно.
func serveStatus(listener net.Listener, router *gateway.Router, region string, logger *slog.Logger) func() {
	mux := newStatusHandler(router)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("status server stopped", "address", listener.Addr().String(), "error", err)
		}
	}()
	logger.Info("api-gateway status listening",
		"address", listener.Addr().String(),
		"region", region,
		"status", healthStatus(router),
	)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

func healthStatus(router *gateway.Router) string {
	if router.Routes().Ready() {
		return "ready"
	}
	return "no-config"
}
