// Package cli implements the command-line cache client.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxPayload matches the 10 KiB wire contract in gateway.proto; the client does
// not depend on the gateway implementation to enforce its input limit.
const maxPayload = 10 * 1024

var errHelp = errors.New("help displayed")

type revisionCommand struct {
	Key string `arg:"" help:"Nonempty cache key."`
	Rev uint64 `help:"Revision (0: latest for get, all for delete)."`
}

type arguments struct {
	Timeout time.Duration   `default:"5s" help:"Positive RPC timeout."`
	Get     revisionCommand `cmd:"" help:"Read the latest or a specific revision."`
	Put     struct {
		Key         string  `arg:"" help:"Nonempty cache key."`
		Value       string  `arg:"" help:"Literal bytes, @path for a file, @- for stdin, @@text for literal @text."`
		ExpectedRev *uint64 `help:"Conditional write: omitted is unconditional; 0 is create only."`
	} `cmd:"" help:"Write a value and return its assigned revision."`
	Delete revisionCommand `cmd:"" help:"Delete one revision or all revisions."`
}

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
	var opts arguments
	parser, err := kong.New(&opts, kong.Name("oracache"),
		kong.Description("Plaintext gRPC cache client. ORACACHE_ENDPOINT is required for RPCs."),
		kong.Writers(stdout, stderr),
		// Return a sentinel from the help hook rather than terminating the host
		// process or continuing to validate required command arguments.
		kong.Help(func(options kong.HelpOptions, ctx *kong.Context) error {
			if err := kong.DefaultHelpPrinter(options, ctx); err != nil {
				return err
			}
			return errHelp
		}))
	if err != nil {
		return err
	}
	parsed, err := parser.Parse(args)
	if errors.Is(err, errHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if opts.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	command := strings.Fields(parsed.Command())[0]
	var key string
	switch command {
	case "get":
		key = opts.Get.Key
	case "put":
		key = opts.Put.Key
	case "delete":
		key = opts.Delete.Key
	}
	if key == "" {
		return errors.New("KEY must be nonempty")
	}
	endpoint := os.Getenv("ORACACHE_ENDPOINT")
	if endpoint == "" {
		return errors.New("ORACACHE_ENDPOINT is required (host:port, plaintext gRPC)")
	}
	var data []byte
	if command == "put" {
		data, err = payload(opts.Put.Value, stdin)
		if err != nil {
			return err
		}
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewCacheGatewayClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	var response proto.Message
	switch command {
	case "get":
		response, err = client.Get(ctx, &pb.GetRequest{Key: key, Rev: opts.Get.Rev})
	case "delete":
		response, err = client.Delete(ctx, &pb.DeleteRequest{Key: key, Rev: opts.Delete.Rev})
	case "put":
		response, err = client.Put(ctx, &pb.PutRequest{Key: key, Data: data, ExpectedRev: opts.Put.ExpectedRev})
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

func payload(value string, stdin io.Reader) ([]byte, error) {
	var reader io.Reader
	switch {
	case strings.HasPrefix(value, "@@"):
		reader = strings.NewReader(value[1:])
	case value == "@-":
		reader = stdin
	case strings.HasPrefix(value, "@"):
		file, err := os.Open(value[1:])
		if err != nil {
			return nil, err
		}
		defer file.Close()
		reader = file
	default:
		reader = strings.NewReader(value)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxPayload+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxPayload {
		return nil, fmt.Errorf("payload exceeds %d bytes", maxPayload)
	}
	return data, nil
}
