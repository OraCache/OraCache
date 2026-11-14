package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Forwarder is the stateless forwarding gateway: it implements the public
// CacheGateway API and relays every request to a storage node of the key's
// partition. The Router only resolves addresses; the Forwarder adds bounded
// failover:
//
//   - The first target is the last node that answered (learned from traffic),
//     else the CP-assigned leader.
//   - NOT_LEADER (a guaranteed pre-proposal refusal) is retried at the Raft
//     leader address from the hint, if any, otherwise at the next replica.
//   - Unavailable moves on to the next replica. For writes this can be
//     ambiguous (the node may have proposed before it died): an unconditional
//     Put retried this way can create an extra version; callers needing
//     exactly-once use expected_rev. DeadlineExceeded is never retried for writes.
//   - Total work is bounded by MaxAttempts and one overall Timeout. CP is never
//     contacted on the request path and assignments are never rewritten.
type Forwarder struct {
	pb.UnimplementedCacheGatewayServer

	Router *Router
	// Dial returns a client for a node address. The default dials plaintext gRPC
	// and caches the connection.
	Dial           func(addr string) (pb.CacheGatewayClient, error)
	MaxAttempts    int           // default 12
	Timeout        time.Duration // hard cap on one request; the caller's deadline applies too, earlier wins; default 10s
	AttemptTimeout time.Duration // per forwarded call; default 3s
	Backoff        time.Duration // pause after a full unsuccessful round; default 150ms
	Logger         *slog.Logger

	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn
	preferred map[uint32]string
}

// Close releases cached connections.
func (f *Forwarder) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

func (f *Forwarder) client(addr string) (pb.CacheGatewayClient, error) {
	if f.Dial != nil {
		return f.Dial(addr)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.conns[addr]; c != nil {
		return pb.NewCacheGatewayClient(c), nil
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		return nil, err
	}
	if f.conns == nil {
		f.conns = map[string]*grpc.ClientConn{}
	}
	f.conns[addr] = c
	return pb.NewCacheGatewayClient(c), nil
}

func (f *Forwarder) logger() *slog.Logger {
	if f.Logger != nil {
		return f.Logger
	}
	return slog.Default()
}

func orDefault[T int | time.Duration](v, d T) T {
	if v <= 0 {
		return d
	}
	return v
}

// notLeaderHint returns the leader client address advertised by a NOT_LEADER
// refusal. ok is false for any other error.
func notLeaderHint(err error) (addr string, ok bool) {
	st, isStatus := status.FromError(err)
	if !isStatus || st.Code() != codes.FailedPrecondition {
		return "", false
	}
	for _, d := range st.Details() {
		if info, isInfo := d.(*errdetails.ErrorInfo); isInfo && info.Reason == "NOT_LEADER" {
			return info.Metadata["address"], true
		}
	}
	return "", false
}

// forward runs call against the partition's group until it succeeds, fails with
// a non-retryable error or the bounds are exhausted. call receives the node
// address that served it.
func (f *Forwarder) forward(ctx context.Context, partition uint32, write bool, call func(context.Context, pb.CacheGatewayClient, string) error) error {
	replicas, err := f.Router.Replicas(partition, write)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	var leader Leader
	if write {
		leader, err = f.Router.WriteLeader(partition)
	} else {
		leader, err = f.Router.GetLeader(partition)
	}
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}

	var order []string
	seen := map[string]bool{}
	add := func(a string) {
		if a != "" && !seen[a] {
			seen[a] = true
			order = append(order, a)
		}
	}
	f.mu.Lock()
	pref := f.preferred[partition]
	f.mu.Unlock()
	add(pref)
	add(leader.Address)
	for _, r := range replicas {
		add(r.Address)
	}
	if len(order) == 0 {
		return status.Error(codes.Unavailable, "partition has no replicas")
	}

	// Timeout is a hard cap: the earlier of the caller's deadline and Timeout wins.
	ctx, cancelAll := context.WithTimeout(ctx, orDefault(f.Timeout, 10*time.Second))
	defer cancelAll()
	attempts, attemptTimeout, backoff := orDefault(f.MaxAttempts, 12), orDefault(f.AttemptTimeout, 3*time.Second), orDefault(f.Backoff, 150*time.Millisecond)

	var (
		hint    string
		cursor  int
		lastErr error
	)
	for i := 0; i < attempts && ctx.Err() == nil; i++ {
		addr := hint
		if addr == "" {
			addr = order[cursor%len(order)]
			cursor++
		}
		hint = ""
		cl, derr := f.client(addr)
		if derr != nil {
			return status.Error(codes.Internal, derr.Error())
		}
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := call(actx, cl, addr)
		cancel()
		if err == nil {
			f.mu.Lock()
			if f.preferred == nil {
				f.preferred = map[uint32]string{}
			}
			f.preferred[partition] = addr
			f.mu.Unlock()
			return nil
		}
		lastErr = err

		f.mu.Lock()
		if f.preferred[partition] == addr {
			delete(f.preferred, partition)
		}
		f.mu.Unlock()

		wait := false
		if h, notLeader := notLeaderHint(err); notLeader {
			if h != "" && h != addr {
				hint = h // follow the actual Raft leader at once
			} else {
				wait = cursor%len(order) == 0 // election in progress: pace the retries
			}
		} else {
			switch status.Code(err) {
			case codes.Unavailable:
				wait = cursor%len(order) == 0
			case codes.DeadlineExceeded:
				if write {
					return err // outcome unknown: never retry automatically
				}
				wait = cursor%len(order) == 0
			default:
				return err
			}
		}
		f.logger().Debug("forward retry", "addr", addr, "partition", partition, "error", err)
		if wait {
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
		}
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return status.Error(codes.Unavailable, fmt.Sprintf("no serving leader for partition %d: %v", partition, lastErr))
}

func (f *Forwarder) partitionOf(key string) (uint32, error) {
	p, err := f.Router.Partition(key)
	if err != nil {
		return 0, status.Error(codes.Unavailable, err.Error())
	}
	return p, nil
}

func (f *Forwarder) served(addr string, write bool, p uint32) *pb.ServedBy {
	by := &pb.ServedBy{}
	if replicas, err := f.Router.Replicas(p, write); err == nil {
		for _, r := range replicas {
			if r.Address == addr {
				by.Region, by.Instance = r.Region, r.NodeID
			}
		}
	}
	return by
}

func (f *Forwarder) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	p, err := f.partitionOf(r.GetKey())
	if err != nil {
		return nil, err
	}
	var out *pb.GetResponse
	err = f.forward(ctx, p, false, func(ctx context.Context, c pb.CacheGatewayClient, addr string) (err error) {
		if out, err = c.Get(ctx, r); err == nil && out.ServedBy == nil {
			out.ServedBy = f.served(addr, false, p)
		}
		return err
	})
	return out, err
}

func (f *Forwarder) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	p, err := f.partitionOf(r.GetKey())
	if err != nil {
		return nil, err
	}
	var out *pb.PutResponse
	err = f.forward(ctx, p, true, func(ctx context.Context, c pb.CacheGatewayClient, addr string) (err error) {
		if out, err = c.Put(ctx, r); err == nil && out.ServedBy == nil {
			out.ServedBy = f.served(addr, true, p)
		}
		return err
	})
	return out, err
}

func (f *Forwarder) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	p, err := f.partitionOf(r.GetKey())
	if err != nil {
		return nil, err
	}
	var out *pb.DeleteResponse
	err = f.forward(ctx, p, true, func(ctx context.Context, c pb.CacheGatewayClient, addr string) (err error) {
		if out, err = c.Delete(ctx, r); err == nil && out.ServedBy == nil {
			out.ServedBy = f.served(addr, true, p)
		}
		return err
	})
	return out, err
}

var errCrossPartition = errors.New("batch items must belong to one partition")

func (f *Forwarder) batchPartition(keys []string) (uint32, error) {
	var first uint32
	for i, k := range keys {
		p, err := f.partitionOf(k)
		if err != nil {
			return 0, err
		}
		if i == 0 {
			first = p
		} else if p != first {
			return 0, status.Error(codes.InvalidArgument, errCrossPartition.Error())
		}
	}
	return first, nil
}

func (f *Forwarder) BatchGet(ctx context.Context, r *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	keys := make([]string, len(r.GetItems()))
	for i, it := range r.GetItems() {
		keys[i] = it.GetKey()
	}
	p, err := f.batchPartition(keys)
	if err != nil {
		return nil, err
	}
	var out *pb.BatchGetResponse
	err = f.forward(ctx, p, false, func(ctx context.Context, c pb.CacheGatewayClient, _ string) (err error) {
		out, err = c.BatchGet(ctx, r)
		return err
	})
	return out, err
}

func (f *Forwarder) BatchPut(ctx context.Context, r *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	keys := make([]string, len(r.GetItems()))
	for i, it := range r.GetItems() {
		keys[i] = it.GetKey()
	}
	p, err := f.batchPartition(keys)
	if err != nil {
		return nil, err
	}
	var out *pb.BatchPutResponse
	err = f.forward(ctx, p, true, func(ctx context.Context, c pb.CacheGatewayClient, _ string) (err error) {
		out, err = c.BatchPut(ctx, r)
		return err
	})
	return out, err
}
