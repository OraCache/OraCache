package node_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/internal/store"
	"github.com/OraCache/oracache/shardraft"
	"go.etcd.io/raft/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", what)
}

// smallStoreConfig keeps exactly two 100-byte keys, so eviction is easy to trigger.
func smallStoreConfig() store.Config {
	return store.Config{MaxVersions: 2, MaxValueBytes: 100, MaxBytes: 2 * (64 + 3 + 2*(32+100))}
}

func newSM(t *testing.T) (*node.StateMachine, *store.Store) {
	t.Helper()
	s, err := store.New(smallStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	return node.NewStateMachine(s), s
}

func TestStateMachineMapsStoreResults(t *testing.T) {
	ctx := context.Background()
	sm, _ := newSM(t)
	zero, one := uint64(0), uint64(1)

	if g, _ := sm.Get(ctx, &pb.GetRequest{Key: "k"}); g.Status != pb.GetStatus_GET_STATUS_MISS {
		t.Fatalf("miss: %v", g)
	}
	// expected_rev 0 means "key must not exist".
	p, err := sm.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("a"), ExpectedRev: &zero})
	if err != nil || p.Status != pb.PutStatus_PUT_STATUS_OK || p.Rev != 1 {
		t.Fatalf("put: %v %v", p, err)
	}
	if p, _ = sm.Put(ctx, &pb.PutRequest{Key: "k", ExpectedRev: &zero}); p.Status != pb.PutStatus_PUT_STATUS_CONFLICT || p.Rev != 1 {
		t.Fatalf("conflict must report the latest rev: %v", p)
	}
	if p, _ = sm.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("b"), ExpectedRev: &one}); p.Rev != 2 {
		t.Fatalf("put 2: %v", p)
	}
	if p, _ = sm.Put(ctx, &pb.PutRequest{Key: "k", Data: bytes.Repeat([]byte("x"), 101)}); p.Status != pb.PutStatus_PUT_STATUS_TOO_LARGE {
		t.Fatalf("too large: %v", p)
	}
	g, _ := sm.Get(ctx, &pb.GetRequest{Key: "k", Rev: 1})
	if g.Status != pb.GetStatus_GET_STATUS_HIT || string(g.Data) != "a" || g.LatestRev != 2 {
		t.Fatalf("old version: %v", g)
	}
	if g, _ = sm.Get(ctx, &pb.GetRequest{Key: "k", Rev: 7}); g.Status != pb.GetStatus_GET_STATUS_VERSION_NOT_FOUND || g.LatestRev != 2 {
		t.Fatalf("version not found: %v", g)
	}
	if d, _ := sm.Delete(ctx, &pb.DeleteRequest{Key: "k", Rev: 2}); d.Status != pb.DeleteStatus_DELETE_STATUS_DELETED {
		t.Fatalf("delete one version: %v", d)
	}
	if d, _ := sm.Delete(ctx, &pb.DeleteRequest{Key: "k", Rev: 2}); d.Status != pb.DeleteStatus_DELETE_STATUS_NOT_FOUND {
		t.Fatalf("repeat delete: %v", d)
	}
	for _, call := range []func() error{
		func() error { _, err := sm.Get(ctx, &pb.GetRequest{}); return err },
		func() error { _, err := sm.Put(ctx, &pb.PutRequest{}); return err },
		func() error { _, err := sm.Delete(ctx, &pb.DeleteRequest{}); return err },
	} {
		if status.Code(call()) != codes.InvalidArgument {
			t.Fatal("empty key must be InvalidArgument")
		}
	}
}

// TestReadsDoNotDiverge shows why the adapter must not promote on Get: one
// replica serving reads would otherwise evict a different key than the others.
func TestReadsDoNotChangeEvictionOrder(t *testing.T) {
	ctx := context.Background()
	val := bytes.Repeat([]byte("x"), 100)
	apply := func(sm *node.StateMachine, reads bool) {
		for _, k := range []string{"a", "b", "c", "d"} {
			if _, err := sm.Put(ctx, &pb.PutRequest{Key: k, Data: val}); err != nil {
				t.Fatal(err)
			}
			if reads { // the "leader" is read from constantly; followers never
				for _, rk := range []string{"a", "b"} {
					_, _ = sm.Get(ctx, &pb.GetRequest{Key: rk})
				}
			}
		}
	}
	reader, readerStore := newSM(t)
	quiet, quietStore := newSM(t)
	apply(reader, true)
	apply(quiet, false)
	a, _ := readerStore.Checkpoint()
	b, _ := quietStore.Checkpoint()
	if !bytes.Equal(a, b) {
		t.Fatalf("reads changed replicated state:\n%s\n%s", a, b)
	}

	// Control: the store's promoting Get does diverge, which is the bug avoided.
	promoted, promotedStore := newSM(t)
	for _, k := range []string{"a", "b", "c"} {
		_, _ = promoted.Put(ctx, &pb.PutRequest{Key: k, Data: val})
		promotedStore.Get("a", 0)
	}
	plain, plainStore := newSM(t)
	for _, k := range []string{"a", "b", "c"} {
		_, _ = plain.Put(ctx, &pb.PutRequest{Key: k, Data: val})
	}
	x, _ := promotedStore.Checkpoint()
	y, _ := plainStore.Checkpoint()
	if bytes.Equal(x, y) {
		t.Fatal("control: expected the promoting Get to change eviction")
	}
}

type memCluster struct {
	network *shardraft.MemoryNetwork
	nodes   []*shardraft.Instance
	stores  []*store.Store
}

func newMemCluster(t *testing.T) *memCluster {
	t.Helper()
	c := &memCluster{network: shardraft.NewMemoryNetwork()}
	peers := []shardraft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	for i := range peers {
		sm, s := newSM(t)
		inst, err := shardraft.NewInstance(shardraft.Config{
			RegionID: "r", ShardID: 1, GroupID: "r/s1/test", PeerID: peers[i].ID, Peers: peers,
			TickInterval: 10 * time.Millisecond, ElectionTicks: 15, HeartbeatTicks: 1,
			RequestTimeout: 3 * time.Second, SendTimeout: 100 * time.Millisecond, SnapshotEntries: 8,
			Logger: &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
		}, sm, c.network)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.network.Register(inst); err != nil {
			t.Fatal(err)
		}
		c.nodes, c.stores = append(c.nodes, inst), append(c.stores, s)
	}
	for _, inst := range c.nodes {
		done := make(chan error, 1)
		go func() { done <- inst.Run(context.Background()) }()
		t.Cleanup(func() { inst.Close(); <-done })
	}
	return c
}

func (c *memCluster) leader(t *testing.T) *shardraft.Instance {
	t.Helper()
	var l *shardraft.Instance
	eventually(t, "leader", func() bool {
		for _, n := range c.nodes {
			if s := n.Status(); s.Running && s.Ready && s.Role == "StateLeader" {
				l = n
				return true
			}
		}
		return false
	})
	return l
}

func (c *memCluster) converged(want []byte) bool {
	for _, s := range c.stores {
		got, _ := s.Checkpoint()
		if !bytes.Equal(got, want) {
			return false
		}
	}
	return true
}

// TestReplicatedStoresStayIdentical drives evicting writes plus strict leader
// reads through real Raft instances and requires byte-identical store
// checkpoints on all replicas (versions, sequence, LRU order).
func TestReplicatedStoresStayIdentical(t *testing.T) {
	ctx := context.Background()
	c := newMemCluster(t)
	leader := c.leader(t)
	val := bytes.Repeat([]byte("x"), 100)
	for i := 0; i < 15; i++ {
		if _, err := leader.Put(ctx, &pb.PutRequest{Key: fmt.Sprintf("k%d", i%5), Data: val}); err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"k0", "k1", "k4"} {
			if _, err := leader.Get(ctx, &pb.GetRequest{Key: k}); err != nil {
				t.Fatal(err)
			}
		}
	}
	want, _ := c.stores[leader.Status().PeerID-1].Checkpoint()
	eventually(t, "all stores identical", func() bool { return c.converged(want) })
	if !strings.Contains(string(want), `"seq":15`) {
		t.Fatalf("unexpected checkpoint: %s", want)
	}
}

func TestLaggingFollowerRestoresStoreSnapshot(t *testing.T) {
	ctx := context.Background()
	c := newMemCluster(t)
	leader := c.leader(t)
	lagID := uint64(1)
	if leader.Status().PeerID == lagID {
		lagID = 2
	}
	group := leader.Status().GroupID
	for _, n := range c.nodes {
		if id := n.Status().PeerID; id != lagID {
			c.network.Cut(group, lagID, id, true)
		}
	}
	val := bytes.Repeat([]byte("y"), 100)
	for i := 0; i < 30; i++ {
		if _, err := leader.Put(ctx, &pb.PutRequest{Key: fmt.Sprintf("k%d", i%4), Data: val}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "leader compacted its log beyond the lagging follower", func() bool {
		return leader.Status().SnapshotIndex > c.nodes[lagID-1].Status().AppliedIndex
	})
	for _, n := range c.nodes {
		if id := n.Status().PeerID; id != lagID {
			c.network.Cut(group, lagID, id, false)
		}
	}
	want, _ := c.stores[leader.Status().PeerID-1].Checkpoint()
	eventually(t, "lagging store equals leader store after snapshot", func() bool {
		got, _ := c.stores[lagID-1].Checkpoint()
		return bytes.Equal(got, want)
	})
	// The restored replica continues deterministically: new writes keep it identical.
	if _, err := leader.Put(ctx, &pb.PutRequest{Key: "after", Data: val}); err != nil {
		t.Fatal(err)
	}
	want, _ = c.stores[leader.Status().PeerID-1].Checkpoint()
	eventually(t, "identical after post-snapshot write", func() bool { return c.converged(want) })
}
