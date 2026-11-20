package statestore

import (
	"testing"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdStoreOptions(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		ttl    time.Duration
		want   int64
	}{
		{name: "whole seconds", prefix: "/cp", ttl: 15 * time.Second, want: 15},
		{name: "round up", prefix: "/cp/", ttl: 1500 * time.Millisecond, want: 2},
		{name: "subsecond", prefix: "/cp", ttl: time.Millisecond, want: 1},
		{name: "root namespace", prefix: "/", ttl: time.Second},
		{name: "relative namespace", prefix: "cp", ttl: time.Second},
		{name: "nonpositive TTL", prefix: "/cp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewEtcd(&clientv3.Client{}, tt.prefix, tt.ttl)
			if tt.want == 0 {
				if err == nil {
					t.Fatal("NewEtcd accepted invalid settings")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if store.ttl != tt.want || store.configKey() != "/cp/config/snapshot" {
				t.Fatalf("lease TTL = %d, config key = %q", store.ttl, store.configKey())
			}
		})
	}
}

func TestPruneReceiptsOnNodeRegionChange(t *testing.T) {
	previous := cpconfigtest.Config(1)
	next := previous.Clone()
	next.Nodes[0].Region = "us"
	next.Nodes = next.Nodes[:2]
	receipts := map[string]HeartbeatReceipt{
		"eu-1": {NodeID: "eu-1", Region: "eu"},
		"eu-2": {NodeID: "eu-2", Region: "eu"},
		"us-1": {NodeID: "us-1", Region: "us"},
	}
	if !removesNodeRegion(previous, next) || !pruneReceipts(receipts, next) {
		t.Fatal("updated topology should discard the moved and removed node receipts")
	}
	if receipt := receipts["eu-2"]; len(receipts) != 1 || receipt.Region != "eu" {
		t.Fatalf("receipts after prune = %+v, want only eu-2", receipts)
	}
	if !removesNodeRegion(next, previous) {
		t.Fatal("moving the node back also removes a node-region pair")
	}
}
