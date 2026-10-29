// oracache-server serves a local, plaintext, in-memory cache gateway.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"google.golang.org/grpc"
)

func newServer() *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	pb.RegisterCacheGatewayServer(server, cache.New())
	return server
}

func main() {
	listen := flag.String("listen", "127.0.0.1:50051", "plaintext gRPC listen address (use port 0 for an ephemeral port)")
	flag.Parse()
	if flag.NArg() != 0 {
		log.Fatal("unexpected positional arguments")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	server := newServer()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	// The cache itself has no background work. This goroutine only handles
	// process shutdown, allowing in-flight RPCs to complete before exit.
	go func() { <-signals; server.GracefulStop() }()
	fmt.Printf("listening %s\n", listener.Addr())
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
