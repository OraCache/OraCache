package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/scheduler"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Result struct {
	Revision uint64 `json:"config_revision"`
	Changed  bool   `json:"changed"`
	Error    string `json:"error,omitempty"`
}

func (c *Controller) Desired(ctx context.Context) (*cpconfig.Config, error) {
	snapshot, err := c.store.Config(ctx)
	return snapshot.Config, err
}

// Apply submits a durable intent and waits for its result. Repeating the same
// plan after losing the reply finds the previous result by intent hash.
func (c *Controller) Apply(ctx context.Context, plan scheduler.Plan) (Result, error) {
	value, err := json.Marshal(plan)
	if err != nil {
		return Result{}, fmt.Errorf("encode topology plan: %w", err)
	}
	sum := sha256.Sum256(value)
	id := hex.EncodeToString(sum[:])
	pending, result := c.pending()+id, c.results()+id
	ctx = clientv3.WithRequireLeader(ctx)
	_, err = c.client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(pending), "=", 0),
		clientv3.Compare(clientv3.CreateRevision(result), "=", 0),
	).Then(clientv3.OpPut(pending, string(value))).Commit()
	if err != nil {
		return Result{}, fmt.Errorf("submit topology intent %s: %w", id, err)
	}
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		response, err := c.client.Get(ctx, result)
		if err != nil {
			return Result{}, fmt.Errorf("read topology result for %s: %w", id, err)
		}
		if len(response.Kvs) > 0 {
			var outcome Result
			if err := json.Unmarshal(response.Kvs[0].Value, &outcome); err != nil {
				return Result{}, fmt.Errorf("decode topology result for %s: %w", id, err)
			}
			return outcome, nil
		}
		select {
		case <-ctx.Done():
			return Result{}, fmt.Errorf("wait for topology result for %s: %w", id, ctx.Err())
		case <-ticker.C:
		}
	}
}

type Difference struct {
	Region          string   `json:"region"`
	Partition       uint32   `json:"partition"`
	DesiredReplicas []string `json:"desired_replicas"`
	ObservedReady   []string `json:"observed_ready"`
	DesiredLeader   string   `json:"desired_leader"`
	ObservedLeader  string   `json:"observed_leader,omitempty"`
	Term            uint64   `json:"term"`
	LeaderConflict  bool     `json:"leader_conflict"`
}

type Status struct {
	Revision     uint64       `json:"config_revision"`
	ControllerID string       `json:"controller_id,omitempty"`
	Differences  []Difference `json:"differences"`
}

func (c *Controller) Status(ctx context.Context) (Status, error) {
	desired, live, err := c.store.DesiredAndLive(ctx)
	if err != nil {
		return Status{}, err
	}
	result := Status{Revision: desired.Revision, Differences: []Difference{}}
	leader, err := c.client.Get(clientv3.WithRequireLeader(ctx), c.elections()+"/", clientv3.WithFirstCreate()...)
	if err != nil {
		return Status{}, fmt.Errorf("read Controller leader: %w", err)
	}
	if len(leader.Kvs) > 0 {
		result.ControllerID = string(leader.Kvs[0].Value)
	}
	if desired.Config == nil {
		return result, nil
	}
	for _, group := range desired.Config.Partitions {
		observed, _ := live.Config.Assignment(group.Region, group.PartitionID)
		if observed.Ready && observed.LeaderNodeID == group.LeaderNodeID && sameMembers(group.ReplicaNodeIDs, observed.ReadyReplicaNodeIDs) {
			continue
		}
		diff := Difference{Region: group.Region, Partition: group.PartitionID, DesiredReplicas: group.ReplicaNodeIDs,
			ObservedReady: observed.ReadyReplicaNodeIDs, DesiredLeader: group.LeaderNodeID, Term: observed.ObservedTerm, LeaderConflict: observed.LeaderConflict}
		if observed.Ready {
			diff.ObservedLeader = observed.LeaderNodeID
		}
		result.Differences = append(result.Differences, diff)
	}
	return result, nil
}
