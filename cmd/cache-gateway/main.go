// cache-gateway is the stable client endpoint of the PoC. It polls routing from
// the Control Plane and forwards CacheGateway calls to the storage nodes,
// following Raft leader hints with bounded retries.
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
	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/gateway"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:28080", "plaintext gRPC listen address (use port 0 for an ephemeral port)")
	cpAddr := flag.String("cp-addr", "127.0.0.1:28081", "Control Plane gRPC address")
	region := flag.String("region", "region-a", "local region")
	id := flag.String("gateway-id", "gateway-1", "gateway ID reported to the CP")
	poll := flag.Duration("poll-interval", 2*time.Second, "routing poll interval")
	timeout := flag.Duration("request-timeout", 10*time.Second, "overall forwarding deadline per request")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	router, err := gateway.NewRouter(*region)
	if err != nil {
		logger.Error("router", "error", err)
		os.Exit(2)
	}
	conn, err := grpc.NewClient(*cpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("control plane client", "error", err)
		os.Exit(2)
	}
	defer conn.Close()
	poller := gateway.NewPoller(controlplanev1.NewControlPlaneAPIClient(conn), router, *id)
	poller.Interval, poller.Logger = *poll, logger

	fwd := &gateway.Forwarder{Router: router, Timeout: *timeout, Logger: logger}
	defer fwd.Close()
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	pb.RegisterCacheGatewayServer(server, fwd)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { _ = poller.Run(ctx) }()
	go func() { <-ctx.Done(); server.GracefulStop() }()
	logger.Info("cache-gateway listening", "address", lis.Addr().String(), "cp", *cpAddr, "region", *region)
	if err := server.Serve(lis); err != nil {
		logger.Error("serve", "error", err)
		os.Exit(1)
	}
}
