package shardraft_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"github.com/OraCache/oracache/shardraft"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type observedCache struct {
	*cache.Server
	restores atomic.Int64
}

func (c *observedCache) Restore(data []byte) error {
	err := c.Server.Restore(data)
	if err == nil {
		c.restores.Add(1)
	}
	return err
}

type cluster struct {
	network *shardraft.MemoryNetwork
	nodes   []*shardraft.Instance
	caches  []*observedCache
}

func testConfig(id uint64, count int) shardraft.Config {
	peers := make([]shardraft.Peer, count)
	for i := range peers {
		peers[i] = shardraft.Peer{ID: uint64(i + 1), Address: fmt.Sprintf("node-%d:50051", i+1)}
	}
	return shardraft.Config{
		RegionID: "test", ShardID: 1, GroupID: "test/s1/run1", PeerID: id, Peers: peers,
		TickInterval: 10 * time.Millisecond, ElectionTicks: 15, HeartbeatTicks: 1,
		RequestTimeout: 3 * time.Second, SendTimeout: 100 * time.Millisecond,
		SnapshotEntries: 8,
		Logger:          &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}
}

func runNode(t *testing.T, n *shardraft.Instance) {
	t.Helper()
	finished := make(chan error, 1)
	go func() { finished <- n.Run(context.Background()) }()
	t.Cleanup(func() {
		if err := n.Close(); err != nil {
			t.Error(err)
		}
		if err := <-finished; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func newCluster(t *testing.T, mode shardraft.ReadMode) *cluster {
	t.Helper()
	c := &cluster{network: shardraft.NewMemoryNetwork()}
	for i := 0; i < 3; i++ {
		cfg := testConfig(uint64(i+1), 3)
		cfg.ReadMode = mode
		backend := &observedCache{Server: cache.New()}
		n, err := shardraft.NewInstance(cfg, shardraft.AdaptCache(backend), c.network)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.network.Register(n); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		c.caches = append(c.caches, backend)
	}
	for _, n := range c.nodes {
		runNode(t, n)
	}
	return c
}

func eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(6 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timeout: %s", description)
		case <-tick.C:
		}
	}
}

func (c *cluster) leader(t *testing.T, excluded uint64) *shardraft.Instance {
	t.Helper()
	var leader *shardraft.Instance
	eventually(t, "leader election", func() bool {
		for _, n := range c.nodes {
			s := n.Status()
			if s.PeerID != excluded && s.Running && s.Ready && s.Role == "StateLeader" {
				leader = n
				return true
			}
		}
		return false
	})
	return leader
}

func put(t *testing.T, n *shardraft.Instance, key, value string) *pb.PutResponse {
	t.Helper()
	r, err := n.Put(context.Background(), &pb.PutRequest{Key: key, Data: []byte(value)})
	if err != nil || r.Status != pb.PutStatus_PUT_STATUS_OK {
		t.Fatalf("Put: %v, %v", r, err)
	}
	return r
}

// TestClusterPutGetAndFailover is the executable PoC: three instances, three
// independent caches, elections, replication, reads, failover and lost quorum.
func TestClusterPutGetAndFailover(t *testing.T) {
	c := newCluster(t, shardraft.Linearizable)
	leader := c.leader(t, 0)
	var client pb.CacheGatewayClient = leader
	p := put(t, leader, "profile:42", "first")
	g, err := client.Get(context.Background(), &pb.GetRequest{Key: "profile:42"})
	if err != nil || g.Rev != p.Rev || string(g.Data) != "first" {
		t.Fatalf("Get: %v %v", g, err)
	}
	eventually(t, "all independent caches applied", func() bool {
		for _, backend := range c.caches {
			g, _ := backend.Get(context.Background(), &pb.GetRequest{Key: "profile:42"})
			if g.Rev != p.Rev || string(g.Data) != "first" {
				return false
			}
		}
		return true
	})
	for _, n := range c.nodes {
		if n == leader {
			continue
		}
		_, err := n.Put(context.Background(), &pb.PutRequest{Key: "wrong-node"})
		var hint *shardraft.NotLeaderError
		if !errors.As(err, &hint) {
			t.Fatalf("expected NotLeader, got %v", err)
		}
		if _, err := n.Get(context.Background(), &pb.GetRequest{Key: "profile:42"}); !errors.As(err, &hint) {
			t.Fatalf("strict follower read: %v", err)
		}
	}
	oldID := leader.Status().PeerID
	leader.Close()
	leader = c.leader(t, oldID)
	g, err = leader.Get(context.Background(), &pb.GetRequest{Key: "profile:42"})
	if err != nil || string(g.Data) != "first" {
		t.Fatalf("confirmed data lost: %v %v", g, err)
	}
	if p := put(t, leader, "profile:42", "second"); p.Rev != 2 {
		t.Fatalf("revision after election: %v", p)
	}
	d, err := leader.Delete(context.Background(), &pb.DeleteRequest{Key: "profile:42"})
	if err != nil || d.Status != pb.DeleteStatus_DELETE_STATUS_DELETED {
		t.Fatalf("Delete: %v %v", d, err)
	}
	g, err = leader.Get(context.Background(), &pb.GetRequest{Key: "profile:42"})
	if err != nil || g.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatalf("expected MISS: %v %v", g, err)
	}
	for _, n := range c.nodes {
		if n != leader {
			n.Close()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	if r, err := leader.Put(ctx, &pb.PutRequest{Key: "no-quorum"}); err == nil {
		t.Fatalf("acknowledged without quorum: %v", r)
	}
	g, _ = c.caches[leader.Status().PeerID-1].Get(context.Background(), &pb.GetRequest{Key: "no-quorum"})
	if g.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatal("uncommitted proposal changed cache")
	}
}

func TestPartitionAndSnapshotCatchup(t *testing.T) {
	c := newCluster(t, shardraft.Linearizable)
	old := c.leader(t, 0)
	oldID := old.Status().PeerID
	put(t, old, "key", "initial")
	for _, n := range c.nodes {
		if n != old {
			c.network.Cut(old.Status().GroupID, oldID, n.Status().PeerID, true)
		}
	}
	// The isolated old leader cannot serve a fresh strict read even before it
	// notices quorum loss. A role check alone would incorrectly return a HIT.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := old.Get(ctx, &pb.GetRequest{Key: "key"})
	cancel()
	if err == nil {
		t.Fatal("isolated leader served a linearizable read")
	}
	leader := c.leader(t, oldID)
	var last *pb.PutResponse
	for i := 0; i < 24; i++ {
		last = put(t, leader, "key", fmt.Sprintf("value-%d", i))
	}
	eventually(t, "leader compacted log", func() bool { return leader.Status().SnapshotIndex > old.Status().AppliedIndex })
	for _, n := range c.nodes {
		if n != old {
			c.network.Cut(old.Status().GroupID, oldID, n.Status().PeerID, false)
		}
	}
	eventually(t, "old leader restored snapshot and caught up", func() bool {
		backend := c.caches[oldID-1]
		g, _ := backend.Get(context.Background(), &pb.GetRequest{Key: "key"})
		return backend.restores.Load() > 0 && g.Rev == last.Rev && string(g.Data) == "value-23"
	})
}

func TestBatchValidationAndLocalReads(t *testing.T) {
	c := newCluster(t, shardraft.Local)
	leader := c.leader(t, 0)
	zero := uint64(0)
	r, err := leader.BatchPut(context.Background(), &pb.BatchPutRequest{Items: []*pb.PutRequest{
		{Key: "k", Data: []byte("one")},
		{Key: "k", Data: []byte("conflict"), ExpectedRev: &zero},
		{Key: "k", Data: []byte("two")},
	}})
	if err != nil || len(r.Items) != 3 || r.Items[0].Rev != 1 || r.Items[1].Status != pb.PutStatus_PUT_STATUS_CONFLICT || r.Items[2].Rev != 2 {
		t.Fatalf("BatchPut: %v %v", r, err)
	}
	_, err = leader.BatchPut(context.Background(), &pb.BatchPutRequest{Items: []*pb.PutRequest{{Key: "not-written"}, {}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid batch: %v", err)
	}
	for _, n := range c.nodes {
		eventually(t, "local follower batch reads converge", func() bool {
			g, err := n.BatchGet(context.Background(), &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "k", Rev: 1}, {Key: "k"}, {Key: "not-written"}}})
			return err == nil && len(g.Items) == 3 && string(g.Items[0].Data) == "one" && string(g.Items[1].Data) == "two" && g.Items[2].Status == pb.GetStatus_GET_STATUS_MISS
		})
	}
	if _, err := leader.Put(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request: %v", err)
	}
	if _, err := leader.Get(context.Background(), &pb.GetRequest{Key: "k"}, grpc.WaitForReady(true)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported call options: %v", err)
	}
}

func TestConcurrentWritesAreOrdered(t *testing.T) {
	c := newCluster(t, shardraft.Linearizable)
	leader := c.leader(t, 0)
	const count = 32
	results := make(chan *pb.PutResponse, count)
	errors := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := leader.Put(context.Background(), &pb.PutRequest{Key: "shared", Data: []byte(fmt.Sprint(i))})
			if err != nil {
				errors <- err
			} else {
				results <- r
			}
		}(i)
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool)
	for r := range results {
		if r.Status != pb.PutStatus_PUT_STATUS_OK || r.Rev < 1 || r.Rev > count || seen[r.Rev] {
			t.Fatalf("invalid revision: %v", r)
		}
		seen[r.Rev] = true
	}
	if len(seen) != count {
		t.Fatal("missing responses")
	}
	gold, err := leader.BatchGet(context.Background(), &pb.BatchGetRequest{Items: []*pb.GetRequest{{Key: "shared", Rev: 1}, {Key: "shared", Rev: count}}})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "replicas agree on ordered revisions", func() bool {
		for _, backend := range c.caches {
			for _, want := range gold.Items {
				got, _ := backend.Get(context.Background(), &pb.GetRequest{Key: "shared", Rev: want.Rev})
				if got.LatestRev != count || !bytes.Equal(got.Data, want.Data) {
					return false
				}
			}
		}
		return true
	})
}

type blockingCache struct {
	*cache.Server
	entered     chan struct{}
	release     chan struct{}
	applyCtxErr chan error
}

func (c *blockingCache) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	close(c.entered)
	<-c.release
	c.applyCtxErr <- ctx.Err()
	return c.Server.Put(ctx, r)
}

func TestClientCancellationDoesNotCancelCommittedApply(t *testing.T) {
	network := shardraft.NewMemoryNetwork()
	backend := &blockingCache{Server: cache.New(), entered: make(chan struct{}), release: make(chan struct{}), applyCtxErr: make(chan error, 1)}
	n, err := shardraft.NewInstance(testConfig(1, 1), shardraft.AdaptCache(backend), network)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.Register(n); err != nil {
		t.Fatal(err)
	}
	runNode(t, n)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(backend.release) }) })
	eventually(t, "single node leader", func() bool { return n.Status().Ready && n.Status().Role == "StateLeader" })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { _, err := n.Put(ctx, &pb.PutRequest{Key: "committed", Data: []byte("yes")}); returned <- err }()
	select {
	case <-backend.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("apply did not start")
	}
	cancel()
	if err := <-returned; status.Code(err) != codes.Canceled {
		t.Fatalf("cancel: %v", err)
	}
	release.Do(func() { close(backend.release) })
	if err := <-backend.applyCtxErr; err != nil {
		t.Fatalf("client cancellation reached apply: %v", err)
	}
	g, err := n.Get(context.Background(), &pb.GetRequest{Key: "committed"})
	if err != nil || string(g.Data) != "yes" {
		t.Fatalf("committed write missing: %v %v", g, err)
	}
}

func TestLifecycleAndTransportBoundaries(t *testing.T) {
	network := shardraft.NewMemoryNetwork()
	cfg := testConfig(1, 1)
	bad := cfg
	bad.Peers = []shardraft.Peer{{ID: 1}, {ID: 1}}
	if _, err := shardraft.NewInstance(bad, shardraft.AdaptCache(cache.New()), network); err == nil {
		t.Fatal("duplicate peers accepted")
	}
	n, err := shardraft.NewInstance(cfg, shardraft.AdaptCache(cache.New()), network)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Get(context.Background(), &pb.GetRequest{Key: "k"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("before Run: %v", err)
	}
	if err := network.Register(n); err != nil {
		t.Fatal(err)
	}
	if err := network.Register(n); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	runNode(t, n)
	eventually(t, "node running", func() bool { return n.Status().Running })
	if err := n.Run(context.Background()); err == nil {
		t.Fatal("concurrent Run accepted")
	}
	msg := raftpb.Message{Type: raftpb.MsgHup, From: 1, To: 1}
	data, _ := msg.Marshal()
	if err := n.Step(context.Background(), cfg.GroupID, data); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("local control message accepted: %v", err)
	}
	if err := n.Step(context.Background(), "wrong", data); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-group message accepted: %v", err)
	}
	n.Close()
	n.Close()
	if err := n.Run(context.Background()); err == nil {
		t.Fatal("empty-state restart accepted")
	}
	if _, err := n.Put(context.Background(), &pb.PutRequest{Key: "k"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("after Close: %v", err)
	}
}

type failingCache struct {
	*cache.Server
	nilResponse bool
}

func (c *failingCache) Put(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
	if c.nilResponse {
		return nil, nil
	}
	return nil, errors.New("backend failed")
}

func TestFatalApplyStopsInstance(t *testing.T) {
	for _, nilResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("nil_response=%t", nilResponse), func(t *testing.T) {
			network := shardraft.NewMemoryNetwork()
			backend := &failingCache{Server: cache.New(), nilResponse: nilResponse}
			n, err := shardraft.NewInstance(testConfig(1, 1), shardraft.AdaptCache(backend), network)
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			go func() { finished <- n.Run(context.Background()) }()
			t.Cleanup(func() { n.Close() })
			eventually(t, "leader", func() bool { return n.Status().Ready && n.Status().Role == "StateLeader" })
			if _, err := n.Put(context.Background(), &pb.PutRequest{Key: "k"}); err == nil {
				t.Fatal("failed apply acknowledged")
			}
			select {
			case err := <-finished:
				if err == nil || !strings.Contains(err.Error(), "apply entry") {
					t.Fatalf("Run did not report apply failure: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("failed instance kept running")
			}
			if s := n.Status(); s.Ready || s.Running || s.LastError == "" {
				t.Fatalf("bad failed status: %+v", s)
			}
			if _, err := n.Get(context.Background(), &pb.GetRequest{Key: "k"}); status.Code(err) != codes.Unavailable {
				t.Fatalf("failed replica served read: %v", err)
			}
		})
	}
}
