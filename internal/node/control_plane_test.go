package node

import (
	"context"
	"testing"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	gatewayv1 "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/cpagent"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type noControlPlaneIO struct {
	controlplanev1.ControlPlaneAPIClient
}

func TestMembershipCPModeStillValidatesPeer(t *testing.T) {
	n := &Node{agent: &cpagent.Agent{}}
	_, err := n.ChangeMembership(t.Context(), shardraft.MembershipChange{OperationID: "add", Action: "add-learner"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid peer accepted: %v", err)
	}
}

func TestIntegratedStorageChecksWritePlacement(t *testing.T) {
	config := cpconfigtest.Config(1)
	draining := config.Clone()
	draining.Nodes[0].State = cpconfig.NodeStateDraining
	for _, tt := range []struct {
		name       string
		nodeID     string
		region     string
		config     *cpconfig.Config
		wantStatus codes.Code
	}{
		{"no accepted topology", "eu-1", "eu", nil, codes.Unavailable},
		{"assigned master", "eu-1", "eu", config, codes.OK},
		{"assigned follower region", "us-1", "us", config, codes.FailedPrecondition},
		{"unknown node", "outside", "eu", config, codes.FailedPrecondition},
		{"draining", "eu-1", "eu", draining, codes.FailedPrecondition},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{cfg: Config{NodeID: tt.nodeID, Region: tt.region}}
			holder := cpconfig.NewHolder()
			if tt.config != nil {
				if err := holder.Apply(tt.config); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			n.agent, err = cpagent.New(cpagent.Options{
				NodeID: tt.nodeID, Region: tt.region, Client: noControlPlaneIO{}, Statuses: n, Holder: holder,
			})
			if err != nil {
				t.Fatal(err)
			}
			called := false
			_, err = n.requireMasterForWrites(t.Context(), nil,
				&grpc.UnaryServerInfo{FullMethod: gatewayv1.CacheGateway_Put_FullMethodName},
				func(context.Context, any) (any, error) {
					called = true
					return nil, nil
				})
			if status.Code(err) != tt.wantStatus || called != (tt.wantStatus == codes.OK) {
				t.Fatalf("got %v, passed to Raft %t; want %s", err, called, tt.wantStatus)
			}
		})
	}
}
