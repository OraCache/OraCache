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
	membershipv1 "github.com/OraCache/oracache/gen/membership/v1"
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

type membershipTarget struct {
	GroupID     string `help:"Raft group incarnation ID."`
	Region      string `help:"Region of the Raft group."`
	PartitionID uint32 `help:"Partition ID."`
}

type membershipChange struct {
	membershipTarget
	OperationID string `help:"Stable ID for safe retries."`
}

type membershipCommands struct {
	AddLearner struct {
		membershipChange
		PeerID      uint64 `help:"New Raft peer ID."`
		NodeID      string `help:"New node ID."`
		Address     string `help:"Client gRPC address."`
		RaftAddress string `help:"Raft HTTP URL."`
	} `cmd:"" help:"Add a non-voting learner."`
	Promote struct {
		membershipChange
		PeerID uint64 `help:"Learner ID."`
	} `cmd:"" help:"Promote a caught-up learner."`
	Remove struct {
		membershipChange
		PeerID uint64 `help:"Member ID."`
	} `cmd:"" help:"Remove a member."`
	Replace struct {
		membershipChange
		OldPeerID uint64 `help:"Voter to remove."`
		LearnerID uint64 `help:"Learner to promote."`
	} `cmd:"" help:"Replace a voter with a learner."`
	Get membershipTarget `cmd:"" help:"Show the applied group membership."`
}

type arguments struct {
	Timeout time.Duration   `default:"5s" help:"Positive RPC timeout."`
	Get     revisionCommand `cmd:"" help:"Read the latest or a specific revision."`
	Put     struct {
		Key         string  `arg:"" help:"Nonempty cache key."`
		Value       string  `arg:"" help:"Literal bytes, @path for a file, @- for stdin, @@text for literal @text."`
		ExpectedRev *uint64 `help:"Conditional write: omitted is unconditional; 0 is create only."`
	} `cmd:"" help:"Write a value and return its assigned revision."`
	Delete     revisionCommand    `cmd:"" help:"Delete one revision or all revisions."`
	Membership membershipCommands `cmd:"" help:"Manage a Raft group."`
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
	if command != "membership" && key == "" {
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
	if command == "membership" {
		return runMembership(ctx, membershipv1.NewMembershipAdminClient(conn), &opts.Membership, parsed.Command(), stdout)
	}
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

func runMembership(ctx context.Context, client membershipv1.MembershipAdminClient, commands *membershipCommands, action string, stdout io.Writer) error {
	var target membershipTarget
	var change membershipChange
	request := &membershipv1.ChangeMembershipRequest{}
	switch action {
	case "membership add-learner":
		c := commands.AddLearner
		change = c.membershipChange
		request.Action = "add-learner"
		request.Peer = &membershipv1.Peer{Id: c.PeerID, NodeId: c.NodeID, Address: c.Address, RaftAddress: c.RaftAddress}
	case "membership promote":
		change = commands.Promote.membershipChange
		request.Action, request.PeerId = "promote", commands.Promote.PeerID
	case "membership remove":
		change = commands.Remove.membershipChange
		request.Action, request.PeerId = "remove", commands.Remove.PeerID
	case "membership replace":
		change = commands.Replace.membershipChange
		request.Action, request.PeerId, request.RemoveId = "replace", commands.Replace.LearnerID, commands.Replace.OldPeerID
	case "membership get":
		target = commands.Get
	default:
		return fmt.Errorf("unknown membership action %q", action)
	}
	if action != "membership get" {
		target = change.membershipTarget
		request.OperationId = change.OperationID
	}
	if target.GroupID == "" || target.Region == "" {
		return errors.New("membership requires --group-id and --region")
	}
	var response proto.Message
	var err error
	if action == "membership get" {
		response, err = client.GetMembership(ctx, &membershipv1.GetMembershipRequest{GroupId: target.GroupID, Region: target.Region, PartitionId: target.PartitionID})
	} else {
		if request.OperationId == "" {
			return errors.New("membership change requires --operation-id")
		}
		request.GroupId, request.Region, request.PartitionId = target.GroupID, target.Region, target.PartitionID
		response, err = client.ChangeMembership(ctx, request)
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
