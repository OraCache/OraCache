package cpagent

import (
	"context"
	cp "github.com/OraCache/oracache/gen/controlplane/v1"
	"testing"
)

func TestDiscoveryHeartbeatSequenceAndAddress(t *testing.T) {
	client := &fakeClient{}
	a, err := New(Options{NodeID: "eu-1", Region: "eu", ClientAddress: "eu-1:9001", Statuses: StatusProviderFunc(testStatuses), Client: client})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 2; i++ {
		if _, err := a.sendHeartbeat(context.Background()); err != nil {
			t.Fatal(err)
		}
		h := client.lastHeartbeat()
		if h.Sequence != i || h.ClientAddress != "eu-1:9001" {
			t.Fatal(h)
		}
	}
	client.configFn = func(context.Context, *cp.GetConfigRequest) (*cp.GetConfigResponse, error) {
		return &cp.GetConfigResponse{Revision: 99, Discovery: &cp.DiscoverySnapshot{Region: "eu", GroupId: "g"}}, nil
	}
	if err := a.pullConfig(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.Config() != nil {
		t.Fatal("discovery changed storage config/membership")
	}
}
