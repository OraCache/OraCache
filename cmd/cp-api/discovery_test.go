package main

import (
	"context"
	pb "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/statestore"
	"testing"
	"time"
)

func TestDiscoveryOptions(t *testing.T) {
	for _, tc := range []struct {
		name, region, group, path string
		ttl, interval             time.Duration
		valid                     bool
	}{
		{"static", "", "", "", 20 * time.Second, 5 * time.Second, true},
		{"discovery", "a", "g", "", 20 * time.Second, 5 * time.Second, true},
		{"missing group", "a", "", "", 20 * time.Second, 5 * time.Second, false},
		{"missing region", "", "g", "", 20 * time.Second, 5 * time.Second, false},
		{"mixed", "a", "g", "cp.json", 20 * time.Second, 5 * time.Second, false},
		{"equal TTL", "a", "g", "", 5 * time.Second, 5 * time.Second, false},
		{"too long TTL", "a", "g", "", 25 * time.Hour, 5 * time.Second, false},
		{"short interval", "a", "g", "", 20 * time.Second, time.Millisecond, false},
		{"long interval", "a", "g", "", 20 * time.Minute, 6 * time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, cat, err := discoveryOptions(tc.region, tc.group, tc.path, tc.ttl, tc.interval)
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v valid=%v", err, tc.valid)
			}
			if err != nil || tc.region == "" {
				return
			}
			if cat == nil {
				t.Fatal("missing catalog")
			}
			api := cpapi.New(statestore.NewMemory(), opts...)
			resp, err := api.GetConfig(context.Background(), &pb.GetConfigRequest{NodeId: "gw", Kind: pb.AgentKind_AGENT_KIND_GATEWAY})
			if err != nil || resp.GetDiscovery() == nil || resp.Discovery.GroupId != "g" {
				t.Fatalf("%v %v", resp, err)
			}
		})
	}
}
