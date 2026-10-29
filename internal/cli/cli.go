// Package cli implements the command-line cache client.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Run writes one protojson response to stdout. Domain statuses are successful
// RPC results (exit 0); usage, I/O and transport errors exit nonzero.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if err := run(args, stdin, stdout, stderr); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: oracache {get|put|delete} [flags] KEY (flags precede KEY)")
	}
	command := args[0]
	if command != "get" && command != "put" && command != "delete" {
		return fmt.Errorf("unknown command %q", command)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout (positive duration)")
	var rev uint64
	var file string
	var expected uint64
	if command == "get" || command == "delete" {
		fs.Uint64Var(&rev, "rev", 0, "revision (0 means latest for get, all for delete)")
	}
	if command == "put" {
		fs.StringVar(&file, "file", "-", "payload file, or - for stdin")
		fs.Uint64Var(&expected, "expected-rev", 0, "conditional write (omitted: unconditional; 0: create only)")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 1 || fs.Arg(0) == "" {
		return errors.New("exactly one nonempty KEY is required; flags must precede KEY")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	endpoint := os.Getenv("ORACACHE_ENDPOINT")
	if endpoint == "" {
		return errors.New("ORACACHE_ENDPOINT is required (host:port, plaintext gRPC)")
	}
	var data []byte
	if command == "put" {
		reader := stdin
		if file != "-" {
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			defer f.Close()
			reader = f
		}
		var err error
		data, err = io.ReadAll(io.LimitReader(reader, cache.MaxPayload+1))
		if err != nil {
			return err
		}
		if len(data) > cache.MaxPayload {
			return fmt.Errorf("payload exceeds %d bytes", cache.MaxPayload)
		}
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewCacheGatewayClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var response proto.Message
	switch command {
	case "get":
		response, err = client.Get(ctx, &pb.GetRequest{Key: fs.Arg(0), Rev: rev})
	case "delete":
		response, err = client.Delete(ctx, &pb.DeleteRequest{Key: fs.Arg(0), Rev: rev})
	case "put":
		r := &pb.PutRequest{Key: fs.Arg(0), Data: data}
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "expected-rev" {
				r.ExpectedRev = &expected
			}
		})
		response, err = client.Put(ctx, r)
	}
	if err != nil {
		return err
	}
	encoded, err := protojson.Marshal(response)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, string(encoded))
	return err
}
