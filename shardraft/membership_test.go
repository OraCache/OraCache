package shardraft_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cache"
	"github.com/OraCache/oracache/shardraft"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func joinNode(t *testing.T, c *cluster, id uint64, snapshots uint64) (*shardraft.Instance, *observedCache) {
	t.Helper()
	cfg := testConfig(id, 3)
	cfg.Join, cfg.ReadMode, cfg.SnapshotEntries = true, shardraft.Local, snapshots
	cfg.Peers = append(cfg.Peers, shardraft.Peer{ID: id, Address: fmt.Sprintf("node-%d:50051", id)})
	backend := &observedCache{Server: cache.New()}
	n, err := shardraft.NewInstance(cfg, shardraft.AdaptCache(backend), c.network)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.network.Register(n); err != nil {
		t.Fatal(err)
	}
	c.nodes, c.caches = append(c.nodes, n), append(c.caches, backend)
	runNode(t, n)
	eventually(t, "joining node running", func() bool { return n.Status().Running })
	return n, backend
}

func change(t *testing.T, leader *shardraft.Instance, op shardraft.MembershipChange) *shardraft.MembershipResult {
	t.Helper()
	r, err := leader.ChangeMembership(context.Background(), op)
	if err != nil {
		t.Fatalf("%s: %v", op.Action, err)
	}
	return r
}

func promoteWhenCaughtUp(t *testing.T, leader *shardraft.Instance, op shardraft.MembershipChange) *shardraft.MembershipResult {
	t.Helper()
	var result *shardraft.MembershipResult
	eventually(t, "learner ready for promotion", func() bool {
		var err error
		if op.Action == "promote" {
			result, err = leader.PromoteLearner(context.Background(), op.OperationID, op.PeerID)
		} else if op.Action == "replace" {
			result, err = leader.Replace(context.Background(), op.OperationID, op.RemoveID, op.PeerID)
		} else {
			result, err = leader.ChangeMembership(context.Background(), op)
		}
		if err != nil && status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("promotion: %v", err)
		}
		return err == nil
	})
	return result
}

func TestAddLearnerLogAndSnapshot(t *testing.T) {
	for _, snapshots := range []uint64{1000, 8} {
		t.Run(fmt.Sprintf("snapshot_entries_%d", snapshots), func(t *testing.T) {
			c := &cluster{network: shardraft.NewMemoryNetwork()}
			for id := uint64(1); id <= 3; id++ {
				cfg := testConfig(id, 3)
				cfg.SnapshotEntries = snapshots
				backend := &observedCache{Server: cache.New()}
				n, err := shardraft.NewInstance(cfg, shardraft.AdaptCache(backend), c.network)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.network.Register(n); err != nil {
					t.Fatal(err)
				}
				c.nodes, c.caches = append(c.nodes, n), append(c.caches, backend)
				runNode(t, n)
			}
			leader := c.leader(t, 0)
			for i := 0; i < 24; i++ {
				put(t, leader, "before-join", fmt.Sprint(i))
			}
			joining, backend := joinNode(t, c, 4, snapshots)
			group := joining.Status().GroupID
			for id := uint64(1); id <= 3; id++ {
				c.network.Cut(group, id, 4, true)
			}
			if _, err := joining.Get(context.Background(), &pb.GetRequest{Key: "before-join"}); status.Code(err) != codes.Unavailable {
				t.Fatalf("unjoined local read: %v", err)
			}
			add := shardraft.MembershipChange{OperationID: "add-4", Action: "add-learner", Peer: shardraft.Peer{ID: 4, Address: "node-4:50051"}}
			duplicate := add.Peer
			duplicate.Address = leader.Membership().Members[0].Address
			if _, err := leader.AddLearner(t.Context(), "add-duplicate-address", duplicate); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("added learner with an active member's gRPC address: %v", err)
			}
			first, err := leader.AddLearner(context.Background(), add.OperationID, add.Peer)
			if err != nil {
				t.Fatal(err)
			}
			if again := change(t, leader, add); first.ChangeIndex != again.ChangeIndex {
				t.Fatal("retry created a second configuration")
			}
			conflict := add
			conflict.Peer.ID = 5
			if _, err := leader.ChangeMembership(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("operation ID collision: %v", err)
			}
			promote := shardraft.MembershipChange{OperationID: "promote-4", Action: "promote", PeerID: 4}
			if _, err := leader.PromoteLearner(context.Background(), promote.OperationID, promote.PeerID); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("promoted disconnected learner: %v", err)
			}
			// Writes still need only the original quorum while the learner is offline.
			put(t, leader, "while-joining", "available")
			for id := uint64(1); id <= 3; id++ {
				c.network.Cut(group, id, 4, false)
			}
			eventually(t, "learner applied confirmed writes", func() bool {
				got, _ := backend.Get(context.Background(), &pb.GetRequest{Key: "while-joining"})
				return string(got.GetData()) == "available" && joining.Status().IsLearner
			})
			if joining.Status().Ready || joining.Status().Member {
				t.Fatal("learner advertised as ready voter")
			}
			if _, err := joining.Get(context.Background(), &pb.GetRequest{Key: "before-join"}); status.Code(err) != codes.Unavailable {
				t.Fatalf("learner served local read: %v", err)
			}
			if (backend.restores.Load() > 0) != (snapshots == 8) {
				t.Fatalf("unexpected restore count %d", backend.restores.Load())
			}
			promoteWhenCaughtUp(t, leader, promote)
			eventually(t, "promoted node serves applied data", func() bool {
				got, err := joining.Get(context.Background(), &pb.GetRequest{Key: "before-join"})
				return err == nil && string(got.GetData()) == "23" && joining.Status().Ready && !joining.Status().IsLearner
			})
			leader.Close()
			next := c.leader(t, leader.Status().PeerID)
			if again := change(t, next, add); first.ChangeIndex != again.ChangeIndex {
				t.Fatal("lost idempotency history after election/snapshot")
			}
			put(t, next, "after-election", "ok")
		})
	}
}

func TestReplaceLostNodeWithTwoSurvivors(t *testing.T) {
	c := newCluster(t, shardraft.Linearizable)
	leader := c.leader(t, 0)
	var lost *shardraft.Instance
	for _, n := range c.nodes {
		if n != leader {
			lost = n
			break
		}
	}
	put(t, leader, "confirmed", "survives")
	lost.Close()
	for i := 0; i < 24; i++ {
		put(t, leader, "after-loss", fmt.Sprint(i))
	}
	op := shardraft.MembershipChange{OperationID: "replace-lost", Action: "replace", PeerID: 4, RemoveID: lost.Status().PeerID}
	before := leader.Membership().Index
	if _, err := leader.Replace(context.Background(), op.OperationID, op.RemoveID, op.PeerID); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replacement without an existing learner: %v", err)
	}
	if leader.Membership().Index != before {
		t.Fatal("failed replacement changed membership")
	}
	joining, backend := joinNode(t, c, 4, 8)
	for id := uint64(1); id <= 3; id++ {
		c.network.Cut(joining.Status().GroupID, id, 4, true)
	}
	added := change(t, leader, shardraft.MembershipChange{OperationID: "add-replacement", Action: "add-learner", Peer: shardraft.Peer{ID: 4}})
	before = added.Membership.Index
	if _, err := leader.Replace(context.Background(), op.OperationID, op.RemoveID, op.PeerID); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replacement with a lagging learner: %v", err)
	}
	if leader.Membership().Index != before {
		t.Fatal("premature replacement changed membership")
	}
	for id := uint64(1); id <= 3; id++ {
		c.network.Cut(joining.Status().GroupID, id, 4, false)
	}
	eventually(t, "replacement restored snapshot", func() bool { return backend.restores.Load() > 0 })
	r := promoteWhenCaughtUp(t, leader, op)
	if r.Membership.Joint || len(r.Membership.Members) != 3 {
		t.Fatalf("not a final 3-member configuration: %+v", r)
	}
	for _, m := range r.Membership.Members {
		if !m.Voter || m.Learner || m.ID == lost.Status().PeerID {
			t.Fatalf("unexpected member: %+v", m)
		}
	}
	for _, key := range []string{"confirmed", "after-loss"} {
		eventually(t, "replacement contains "+key, func() bool {
			got, err := joining.Get(context.Background(), &pb.GetRequest{Key: key})
			return err == nil && got.GetStatus() == pb.GetStatus_GET_STATUS_HIT
		})
	}
	// Survive another failure after replacement: the new voter is now necessary
	// to elect a leader and commit, proving it isn't merely a stale cache copy.
	leader.Close()
	next := c.leader(t, leader.Status().PeerID)
	put(t, next, "new-quorum", "works")
	if again, err := next.Replace(context.Background(), op.OperationID, op.RemoveID, op.PeerID); err != nil || again.ChangeIndex != r.ChangeIndex {
		t.Fatalf("replacement retry: %+v %v", again, err)
	}
	if _, err := next.ChangeMembership(context.Background(), shardraft.MembershipChange{OperationID: "reuse-old", Action: "add-learner", Peer: shardraft.Peer{ID: lost.Status().PeerID}}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reused removed ID: %v", err)
	}
}

func TestRemoveLiveMemberFencesLocalReads(t *testing.T) {
	c := newCluster(t, shardraft.Local)
	leader := c.leader(t, 0)
	var removed *shardraft.Instance
	for _, n := range c.nodes {
		if n != leader {
			removed = n
			break
		}
	}
	put(t, leader, "key", "value")
	if _, err := leader.RemoveNode(context.Background(), "remove", removed.Status().PeerID); err != nil {
		t.Fatal(err)
	}
	eventually(t, "removed live node stops serving", func() bool {
		_, err := removed.Get(context.Background(), &pb.GetRequest{Key: "key"})
		return !removed.Status().Ready && status.Code(err) == codes.Unavailable
	})
	put(t, leader, "still-writable", "yes")
}

func TestMembershipNeedsQuorum(t *testing.T) {
	c := newCluster(t, shardraft.Linearizable)
	leader := c.leader(t, 0)
	group, id := leader.Status().GroupID, leader.Status().PeerID
	for _, n := range c.nodes {
		if n != leader {
			c.network.Cut(group, id, n.Status().PeerID, true)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	op := shardraft.MembershipChange{OperationID: "quorum-required", Action: "add-learner", Peer: shardraft.Peer{ID: 4}}
	if _, err := leader.ChangeMembership(ctx, op); err == nil {
		t.Fatal("changed membership without quorum")
	}
	if len(leader.Membership().Members) != 3 {
		t.Fatal("uncommitted membership became visible")
	}
	for _, n := range c.nodes {
		if n != leader {
			c.network.Cut(group, id, n.Status().PeerID, false)
		}
	}
	leader = c.leader(t, 0)
	change(t, leader, op)
}

// Hold config replication after ProposeConfChange but before it can commit.
// Cancellation must only stop the waiter, not revoke this committed intent.
type heldMembershipTransport struct {
	*shardraft.MemoryNetwork
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (t *heldMembershipTransport) Send(ctx context.Context, group string, to uint64, data []byte) error {
	var m raftpb.Message
	if err := m.Unmarshal(data); err != nil {
		return err
	}
	for _, e := range m.Entries {
		if e.Type == raftpb.EntryConfChangeV2 {
			t.once.Do(func() { close(t.entered) })
			select {
			case <-t.release:
			case <-ctx.Done():
				return ctx.Err()
			}
			break
		}
	}
	return t.MemoryNetwork.Send(ctx, group, to, data)
}

func TestCancelledMembershipProposalCommitsAndRetryDeduplicates(t *testing.T) {
	transport := &heldMembershipTransport{MemoryNetwork: shardraft.NewMemoryNetwork(), entered: make(chan struct{}), release: make(chan struct{})}
	c := &cluster{network: transport.MemoryNetwork}
	for id := uint64(1); id <= 3; id++ {
		n, err := shardraft.NewInstance(testConfig(id, 3), shardraft.AdaptCache(cache.New()), transport)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.network.Register(n); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		runNode(t, n)
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(transport.release) }) })
	leader := c.leader(t, 0)
	op := shardraft.MembershipChange{OperationID: "uncertain-add", Action: "add-learner", Peer: shardraft.Peer{ID: 4}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { _, err := leader.ChangeMembership(ctx, op); returned <- err }()
	select {
	case <-transport.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("config was not proposed")
	}
	cancel()
	if err := <-returned; status.Code(err) != codes.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	release.Do(func() { close(transport.release) })
	eventually(t, "cancelled proposal still commits", func() bool { return len(leader.Membership().Members) == 4 })
	index := leader.Membership().Index
	if r := change(t, leader, op); r.ChangeIndex != index {
		t.Fatalf("retry reproposed: %+v", r)
	}
}

func TestLeaderLossDuringAddCanBeRetriedOnNewLeader(t *testing.T) {
	transport := &heldMembershipTransport{MemoryNetwork: shardraft.NewMemoryNetwork(), entered: make(chan struct{}), release: make(chan struct{})}
	c := &cluster{network: transport.MemoryNetwork}
	for id := uint64(1); id <= 3; id++ {
		n, err := shardraft.NewInstance(testConfig(id, 3), shardraft.AdaptCache(cache.New()), transport)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.network.Register(n); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, n)
		runNode(t, n)
	}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(transport.release) }) })
	leader := c.leader(t, 0)
	put(t, leader, "confirmed-before-election", "kept")
	_, backend := joinNode(t, c, 4, 8)
	op := shardraft.MembershipChange{OperationID: "retry-on-next-leader", Action: "add-learner", Peer: shardraft.Peer{ID: 4}}
	returned := make(chan error, 1)
	go func() { _, err := leader.ChangeMembership(context.Background(), op); returned <- err }()
	select {
	case <-transport.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("config was not proposed")
	}
	leader.Close() // no follower has acknowledged the configuration yet
	if err := <-returned; err == nil {
		t.Fatal("uncommitted add returned success")
	}
	release.Do(func() { close(transport.release) })
	next := c.leader(t, leader.Status().PeerID)
	change(t, next, op)
	eventually(t, "new leader catches up learner", func() bool {
		got, _ := backend.Get(context.Background(), &pb.GetRequest{Key: "confirmed-before-election"})
		return string(got.GetData()) == "kept"
	})
	promoteWhenCaughtUp(t, next, shardraft.MembershipChange{OperationID: "replace-former-leader", Action: "replace", PeerID: 4, RemoveID: leader.Status().PeerID})
	put(t, next, "after-interrupted-add", "ok")
}
