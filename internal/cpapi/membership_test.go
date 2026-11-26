package cpapi_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OraCache/oracache/internal/cpapi"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

type confirmedReader struct{ view cpapi.MembershipObservation }

func (r confirmedReader) ReadConfirmedMembership(context.Context, cpapi.RaftGroup) (cpapi.MembershipObservation, error) {
	return r.view, nil
}

func TestReconcileMembershipRejectsUnverifiedGroupState(t *testing.T) {
	group := cpapi.RaftGroup{Region: "eu", GroupID: "eu/0/current"}
	tests := []struct {
		name    string
		corrupt func(*cpapi.MembershipObservation)
	}{
		{"responding follower", func(view *cpapi.MembershipObservation) { view.ResponderNodeID = "eu-2" }},
		{"different incarnation", func(view *cpapi.MembershipObservation) { view.Group.GroupID = "eu/0/other" }},
		{"different partition", func(view *cpapi.MembershipObservation) { view.Group.PartitionID = 1 }},
		{"different region", func(view *cpapi.MembershipObservation) { view.Group.Region = "us" }},
		{"joint configuration", func(view *cpapi.MembershipObservation) { view.Joint = true }},
		{"voter from another region", func(view *cpapi.MembershipObservation) { view.VoterNodeIDs[1] = "us-1" }},
		{"duplicate voter", func(view *cpapi.MembershipObservation) { view.VoterNodeIDs[1] = "eu-1" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			api, store := testServer(t, time.Unix(100, 0))
			if err := store.ApplyConfig(ctx, cpconfigtest.Config(1)); err != nil {
				t.Fatal(err)
			}
			view := cpapi.MembershipObservation{
				Group: group, ResponderNodeID: "eu-1", LeaderNodeID: "eu-1",
				Index: 20, Term: 5, VoterNodeIDs: []string{"eu-1", "eu-2"},
			}
			test.corrupt(&view)

			if err := api.ReconcileMembership(ctx, group, confirmedReader{view: view}); !errors.Is(err, cpapi.ErrMembershipObservation) {
				t.Fatalf("invalid group observation returned %v", err)
			}
			snapshot, err := store.Config(ctx)
			if err != nil || snapshot.Revision != 1 {
				t.Fatalf("rejected observation changed revision to %d: %v", snapshot.Revision, err)
			}
		})
	}
}
