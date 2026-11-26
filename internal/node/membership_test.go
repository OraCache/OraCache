package node_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/node"
	"github.com/OraCache/oracache/internal/replication"
	"github.com/OraCache/oracache/internal/store"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Real Store + HTTP Raft + administrative HTTP + client gRPC. Start with three
// independent processes' worth of state, lose one, join an empty fourth node
// using only one seed, and replace the lost identity without restarting peers.
func TestHTTPMembershipReplacesLostStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var peers []node.Peer
	var configs []node.Config
	var nodes []*node.Node
	var clients []pb.CacheGatewayClient
	for i := 0; i < 4; i++ {
		g, r := storageListen(t), storageListen(t)
		peer := node.Peer{ID: uint64(i + 1), NodeID: fmt.Sprintf("node-%d", i+1), ClientAddr: g.Addr().String(), RaftURL: "http://" + r.Addr().String() + "/raft"}
		peers = append(peers, peer)
		configs = append(configs, node.Config{
			NodeID: peer.NodeID, PeerID: peer.ID, Region: "region-a", GroupID: "membership-http",
			GRPCListener: g, RaftListener: r, TickInterval: 10 * time.Millisecond, ElectionTicks: 20, SnapshotEntries: 8,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		conn, err := grpc.NewClient(peer.ClientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		clients = append(clients, pb.NewCacheGatewayClient(conn))
	}
	start := func(cfg node.Config) {
		t.Helper()
		n, err := node.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)
		done := make(chan error, 1)
		go func() { done <- n.Run(ctx) }()
		t.Cleanup(func() {
			n.Close()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
	wait := func(why string, check func() bool) {
		t.Helper()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for !check() {
			select {
			case <-tick.C:
			case <-ctx.Done():
				t.Fatal(why, ctx.Err())
			}
		}
	}
	for i := 0; i < 3; i++ {
		configs[i].Peers = peers[:3]
		start(configs[i])
	}
	leader := -1
	wait("leader", func() bool {
		for i, n := range nodes {
			if s := n.Status(); s.Ready && s.Role == "StateLeader" {
				leader = i
				return true
			}
		}
		return false
	})
	for i := 0; i < 24; i++ {
		r, err := clients[leader].Put(ctx, &pb.PutRequest{Key: "key", Data: []byte(fmt.Sprint(i))})
		if err != nil || r.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
			t.Fatalf("Put: %v %v", r, err)
		}
	}
	lost := (leader + 1) % 3
	nodes[lost].Close()
	configs[3].Join = true
	configs[3].Peers = []node.Peer{peers[leader], peers[3]}
	start(configs[3])
	wait("join process running", func() bool { return nodes[3].Status().Running })
	if _, err := clients[3].Get(ctx, &pb.GetRequest{Key: "key"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("empty node served: %v", err)
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(httpClient.CloseIdleConnections)
	post := func(c shardraft.MembershipChange, group string) (int, []byte) {
		t.Helper()
		body, _ := json.Marshal(struct {
			GroupID string `json:"group_id"`
			shardraft.MembershipChange
		}{group, c})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+nodes[leader].RaftAddr()+"/membership", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return res.StatusCode, data
	}
	add := shardraft.MembershipChange{OperationID: "http-add", Action: "add-learner", Peer: shardraft.Peer{ID: 4, NodeID: peers[3].NodeID, Address: peers[3].ClientAddr, RaftAddress: peers[3].RaftURL}}
	if code, body := post(add, "wrong-group"); code != http.StatusBadRequest {
		t.Fatalf("cross-group change: %d %s", code, body)
	}
	if code, body := post(add, configs[0].GroupID); code != http.StatusOK {
		t.Fatalf("add: %d %s", code, body)
	}
	wait("learner restored versioned Store", func() bool {
		got := nodes[3].Store().Peek("key", 0)
		return string(got.Version.Data) == "23" && nodes[3].Status().IsLearner && nodes[3].Status().SnapshotIndex > 0
	})
	if nodes[3].Status().Ready {
		t.Fatal("learner advertised readiness")
	}
	replace := shardraft.MembershipChange{OperationID: "http-replace", Action: "replace", PeerID: 4, RemoveID: peers[lost].ID}
	wait("replace", func() bool {
		code, body := post(replace, configs[0].GroupID)
		if code != http.StatusOK && code != http.StatusConflict {
			t.Fatalf("replace: %d %s", code, body)
		}
		return code == http.StatusOK
	})
	wait("new member applied final configuration", func() bool {
		m := nodes[3].Membership()
		return nodes[3].Status().Ready && !m.Joint && len(m.Members) == 3
	})
	if m := nodes[3].Membership(); len(m.Members) != 3 {
		t.Fatalf("membership: %+v", m)
	}
	// The original acknowledgement can be lost: repeat after a leader change.
	nodes[leader].Close()
	wait("new leader", func() bool {
		for i, n := range nodes {
			if s := n.Status(); s.Running && s.Ready && s.Role == "StateLeader" {
				leader = i
				return true
			}
		}
		return false
	})
	if code, body := post(replace, configs[0].GroupID); code != http.StatusOK {
		t.Fatalf("retry after election: %d %s", code, body)
	}
	get, err := clients[leader].Get(ctx, &pb.GetRequest{Key: "key"})
	if err != nil || string(get.GetData()) != "23" || get.GetRev() != 24 {
		t.Fatalf("confirmed data lost: %v %v", get, err)
	}
	put, err := clients[leader].Put(ctx, &pb.PutRequest{Key: "key", Data: []byte("new quorum")})
	if err != nil || put.GetRev() != 25 {
		t.Fatalf("new quorum cannot write: %v %v", put, err)
	}
}

func TestRaftStoreSnapshotBudget(t *testing.T) {
	_, err := node.New(node.Config{NodeID: "n", Region: "r", GroupID: "g", Store: store.Config{MaxBytes: node.MaxStoreBytes + 1}})
	if err == nil || !strings.Contains(err.Error(), "snapshot-safe") {
		t.Fatalf("unsafe budget accepted: %v", err)
	}
	backend, err := store.New(store.Config{MaxBytes: 16 << 10})
	if err != nil {
		t.Fatal(err)
	}
	fsm := node.NewStateMachine(backend, &replication.State{})
	// Escaped JSON keys can expand 6x. Reject the Store's oversized-single-key
	// exception at the Raft boundary before it can become untransferable state.
	got, err := fsm.Put(t.Context(), &pb.PutRequest{Key: strings.Repeat("\x00", 16<<10)})
	if err != nil || got.GetStatus() != pb.PutStatus_PUT_STATUS_TOO_LARGE || backend.Stats().Keys != 0 {
		t.Fatalf("oversized key: %v %v", got, err)
	}
	got, err = fsm.Put(t.Context(), &pb.PutRequest{Key: strings.Repeat("\x00", (16<<10)-100), Data: []byte("ok")})
	if err != nil || got.GetStatus() != pb.PutStatus_PUT_STATUS_OK {
		t.Fatalf("bounded key: %v %v", got, err)
	}
	data, err := fsm.Checkpoint()
	if err != nil || int64(len(data)) > 6*backend.Config().MaxBytes {
		t.Fatalf("checkpoint expansion: %d %v", len(data), err)
	}
}
