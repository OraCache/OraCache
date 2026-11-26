package cpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
)

const discoveryPath = "/discovery/storage-nodes"

type discoveryTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func discoveryRequest(api *cpapi.Server, method, path string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+discoveryPath, api.StorageDiscovery)
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, httptest.NewRequest(method, path, nil))
	return res
}

func assertDiscovery(t *testing.T, api *cpapi.Server, query string, code int, want []discoveryTarget) {
	t.Helper()
	res := discoveryRequest(api, http.MethodGet, discoveryPath+query)
	if res.Code != code {
		t.Fatalf("GET %s: got HTTP %d, want %d: %s", query, res.Code, code, res.Body)
	}
	if code != http.StatusOK {
		return
	}
	if res.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type: %s", res.Header().Get("Content-Type"))
	}
	var got []discoveryTarget
	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targets: got %#v, want %#v", got, want)
	}
}

func discoveryConfig() *cpconfig.Config {
	return &cpconfig.Config{
		Revision: 1, PartitionCount: 2,
		Regions: []cpconfig.Region{{ID: "a", Role: cpconfig.RegionRoleMaster}, {ID: "b", Role: cpconfig.RegionRoleSlave}},
		Nodes: []cpconfig.Node{
			{ID: "a1", Region: "a", Address: "a1:9000", RaftAddress: "http://a1:9001/raft"},
			{ID: "a2", Region: "a", Address: "a2:9000", RaftAddress: "http://a2:9001/raft"},
			{ID: "b1", Region: "b", Address: "b1:9000", RaftAddress: "https://[fd00::1]:9001/raft"},
		},
		Partitions: []cpconfig.PartitionAssignment{
			{Region: "a", PartitionID: 0, GroupID: "a0", LeaderNodeID: "a1", ReplicaNodeIDs: []string{"a1", "a2"}},
			{Region: "a", PartitionID: 1, GroupID: "a1", LeaderNodeID: "a1", ReplicaNodeIDs: []string{"a1"}},
			{Region: "b", PartitionID: 0, GroupID: "b0", LeaderNodeID: "b1", ReplicaNodeIDs: []string{"b1"}},
			{Region: "b", PartitionID: 1, GroupID: "b1", LeaderNodeID: "b1", ReplicaNodeIDs: []string{"b1"}},
		},
	}
}

func openDiscoveryStore(t *testing.T, path string, config *cpconfig.Config) *statestore.MemoryStore {
	t.Helper()
	store, err := statestore.OpenMemory(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyConfig(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	return store
}

func target(node string) discoveryTarget {
	return discoveryTarget{Targets: []string{node + ":9001"}, Labels: map[string]string{"node_id": node, "region": "a"}}
}

func TestStorageDiscoveryBootstrapAndRegion(t *testing.T) {
	store := openDiscoveryStore(t, filepath.Join(t.TempDir(), "membership.json"), discoveryConfig())
	api := cpapi.New(store)
	a1, a2 := target("a1"), target("a2")
	b1 := discoveryTarget{Targets: []string{"[fd00::1]:9001"}, Labels: map[string]string{"node_id": "b1", "region": "b", "__scheme__": "https"}}

	assertDiscovery(t, api, "", http.StatusOK, []discoveryTarget{a1, a2, b1})
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{a1, a2})
	assertDiscovery(t, api, "?region=b", http.StatusOK, []discoveryTarget{b1})
	assertDiscovery(t, api, "?region=unknown", http.StatusNotFound, nil)
	assertDiscovery(t, api, "?region=a&region=b", http.StatusBadRequest, nil)
	if res := discoveryRequest(api, http.MethodPost, discoveryPath); res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST returned %d, want 405", res.Code)
	}
}

func TestStorageDiscoveryRejectsInvalidTargets(t *testing.T) {
	for _, address := range []string{
		"", "ftp://b1:9001/raft", "//b1:9001/raft", "http://b1:/raft", "http://b1:0/raft",
		"http://b1:65536/raft", "http://b1:abc/raft", "http://:9001/raft", "http://[example]:9001/raft",
		"http://0.0.0.0:9001/raft", "http://[::]:9001/raft", "http://bad_host:9001/raft",
		"http://user:secret@b1:9001/raft", "http://b1:9001/raft#fragment",
	} {
		t.Run(address, func(t *testing.T) {
			config := discoveryConfig()
			config.Nodes[2].RaftAddress = address
			store := openDiscoveryStore(t, filepath.Join(t.TempDir(), "membership.json"), config)
			api := cpapi.New(store)
			assertDiscovery(t, api, "?region=b", http.StatusServiceUnavailable, nil)
			assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a2")})
		})
	}
}

func TestStorageDiscoveryUsesSchemeDefaultPortAndDropsRaftQuery(t *testing.T) {
	config := discoveryConfig()
	config.Nodes[0].RaftAddress = "http://a1/raft?group=1"
	config.Nodes[2].RaftAddress = "https://B1.example/internal/raft?group=2"
	store := openDiscoveryStore(t, filepath.Join(t.TempDir(), "membership.json"), config)
	api := cpapi.New(store)
	a1 := target("a1")
	a1.Targets = []string{"a1:80"}
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{a1, target("a2")})
	assertDiscovery(t, api, "?region=b", http.StatusOK, []discoveryTarget{
		{Targets: []string{"b1.example:443"}, Labels: map[string]string{"node_id": "b1", "region": "b", "__scheme__": "https"}},
	})
}

func TestStorageDiscoveryUnavailableStateStore(t *testing.T) {
	api := cpapi.New(failingStore{err: errors.New("store unavailable")})
	assertDiscovery(t, api, "", http.StatusServiceUnavailable, nil)
}

func TestStorageDiscoveryIgnoresMembershipsFromBootstrapJSON(t *testing.T) {
	config := discoveryConfig()
	for _, a := range config.Partitions[:2] {
		config.Memberships = append(config.Memberships, cpconfig.GroupMembership{
			GroupID: a.GroupID, Region: a.Region, PartitionID: a.PartitionID, ReporterID: 1, Index: 10,
			Members: []cpconfig.GroupMember{
				{PeerID: 1, NodeID: "a1", Address: "a1:9000", RaftAddress: "http://a1:9001/raft", Voter: true},
			},
		})
	}
	store := openDiscoveryStore(t, "", config)
	assertDiscovery(t, cpapi.New(store), "?region=a", http.StatusServiceUnavailable, nil)
}

func TestStorageDiscoveryCheckpointFailurePreservesPreviousMembership(t *testing.T) {
	now := time.Unix(1000, 0)
	path := filepath.Join(t.TempDir(), "membership.json")
	store := openDiscoveryStore(t, path, discoveryConfig())
	api := cpapi.New(store, cpapi.WithClock(fixedClock{now: now}))
	voter := cpconfig.GroupMember{PeerID: 1, NodeID: "a1", Address: "a1:9000", RaftAddress: "http://a1:9001/raft", Voter: true}
	learner := cpconfig.GroupMember{PeerID: 3, NodeID: "a3", Address: "a3:9000", RaftAddress: "http://a3:9001/raft", Learner: true}
	m := cpconfig.GroupMembership{GroupID: "a0", Region: "a", ReporterID: 1, Index: 10, Members: []cpconfig.GroupMember{voter, learner}}
	reportDiscoveryMembership(t, store, now, 3, m)
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	m.Index++
	m.Members = []cpconfig.GroupMember{voter}
	_, err := store.UpsertHeartbeat(t.Context(), statestore.Heartbeat{
		NodeID: "a1", Region: "a", ReceivedAt: now,
		Partitions: []statestore.PartitionStatus{{Role: statestore.RaftRoleLeader, Term: 3, Ready: true, LeaderID: "a1", Membership: &m}},
	})
	if err == nil {
		t.Fatal("heartbeat committed despite failed checkpoint")
	}
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a3")})
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	reportDiscoveryMembership(t, store, now, 3, m)
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1")})
}

func TestStorageDiscoveryRetainsAppliedMembershipAndRecoversAfterRestart(t *testing.T) {
	now := time.Unix(1000, 0)
	path := filepath.Join(t.TempDir(), "membership.json")
	config := discoveryConfig()
	store := openDiscoveryStore(t, path, config)
	api := cpapi.New(store, cpapi.WithClock(fixedClock{now: now}))
	voter := cpconfig.GroupMember{PeerID: 1, NodeID: "a1", Address: "a1:9000", RaftAddress: "http://a1:9001/raft", Voter: true}
	old := cpconfig.GroupMember{PeerID: 2, NodeID: "a2", Address: "a2:9000", RaftAddress: "http://a2:9001/raft", Voter: true}
	learner := cpconfig.GroupMember{PeerID: 3, NodeID: "a3", Address: "a3:9000", RaftAddress: "http://a3:9001/raft", Learner: true}

	if _, err := store.UpsertHeartbeat(t.Context(), statestore.Heartbeat{NodeID: "a3", Region: "a", ReceivedAt: now}); err != nil {
		t.Fatal(err)
	}
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a2")})
	m := cpconfig.GroupMembership{GroupID: "a0", Region: "a", ReporterID: 1, Index: 10, Members: []cpconfig.GroupMember{voter, old, learner}}
	other := cpconfig.GroupMembership{GroupID: "a1", Region: "a", PartitionID: 1, ReporterID: 1, Index: 10, Members: []cpconfig.GroupMember{voter, learner}}
	reportDiscoveryMembership(t, store, now, 3, m, other)
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a2"), target("a3")})

	stale := m
	stale.Index--
	stale.Members = []cpconfig.GroupMember{voter}
	reportDiscoveryMembership(t, store, now.Add(time.Second), 2, stale)
	expiredAPI := cpapi.New(store, cpapi.WithClock(fixedClock{now: now.Add(time.Minute)}))
	assertDiscovery(t, expiredAPI, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a2"), target("a3")})
	assertDiscoveryMetric(t, expiredAPI, `oracache_cp_raft_membership_fresh{partition="0",region="a"} 0`)

	old.Voter, old.OutgoingVoter = false, true
	m.Index++
	m.Joint = true
	m.RemovedIDs = []uint64{old.PeerID}
	m.Members = []cpconfig.GroupMember{voter, old}
	reportDiscoveryMembership(t, store, now, 3, m)
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a2"), target("a3")})

	m.Index++
	m.Joint = false
	m.Members = []cpconfig.GroupMember{voter}
	reportDiscoveryMembership(t, store, now, 3, m)
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a3")})

	store = openDiscoveryStore(t, path, config)
	api = cpapi.New(store, cpapi.WithClock(fixedClock{now: now}))
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a3")})
	assertDiscovery(t, api, "?region=b", http.StatusServiceUnavailable, nil)
	assertDiscovery(t, api, "", http.StatusServiceUnavailable, nil)
	assertDiscoveryMetric(t, api, `oracache_cp_raft_membership_fresh{partition="0",region="a"} 0`)
	reportDiscoveryMembership(t, store, now, 2, stale)
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1"), target("a3")})

	reportDiscoveryMembership(t, store, now, 3, m, other)
	assertDiscoveryMetric(t, api, `oracache_cp_raft_membership_fresh{partition="0",region="a"} 1`)
	other.Index++
	other.Members = []cpconfig.GroupMember{voter}
	reportDiscoveryMembership(t, store, now, 3, other)
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{target("a1")})
}

func reportDiscoveryMembership(t *testing.T, store *statestore.MemoryStore, received time.Time, term uint64, reports ...cpconfig.GroupMembership) {
	t.Helper()
	h := statestore.Heartbeat{NodeID: "a1", Region: "a", ReceivedAt: received}
	for _, m := range reports {
		h.Partitions = append(h.Partitions, statestore.PartitionStatus{
			PartitionID: m.PartitionID, Role: statestore.RaftRoleLeader, Term: term, LeaderID: h.NodeID, Ready: true, Membership: &m,
		})
	}
	if _, err := store.UpsertHeartbeat(t.Context(), h); err != nil {
		t.Fatal(err)
	}
}

func assertDiscoveryMetric(t *testing.T, api *cpapi.Server, want string) {
	t.Helper()
	res := httptest.NewRecorder()
	api.Metrics().Handler().ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), want+"\n") {
		t.Fatalf("metrics HTTP %d: missing %q in %s", res.Code, want, res.Body)
	}
}

type snapshotStore struct {
	statestore.Store
	view statestore.Snapshot
}

func (s snapshotStore) LiveConfig(context.Context, time.Time, time.Duration) (statestore.Snapshot, error) {
	return s.view, nil
}

func TestStorageDiscoveryConfirmedEmptyList(t *testing.T) {
	api := cpapi.New(snapshotStore{view: statestore.Snapshot{Config: &cpconfig.Config{
		Regions: []cpconfig.Region{{ID: "a", Role: cpconfig.RegionRoleMaster}},
	}}})
	assertDiscovery(t, api, "?region=a", http.StatusOK, []discoveryTarget{})
}
