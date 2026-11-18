package statestore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *EtcdStore) heartbeatPrefix() string { return s.prefix + "/heartbeats/" }

func (s *EtcdStore) heartbeatKey(nodeID string) string {
	return s.heartbeatPrefix() + base64.RawURLEncoding.EncodeToString([]byte(nodeID))
}

// UpsertHeartbeat implements Store. Only the status has a lease. The receipt
// persists while the node-region pair stays in the published configuration.
func (s *EtcdStore) UpsertHeartbeat(ctx context.Context, heartbeat Heartbeat) (uint64, error) {
	if err := ValidateHeartbeat(heartbeat); err != nil {
		return 0, err
	}
	value, err := json.Marshal(heartbeat)
	if err != nil {
		return 0, fmt.Errorf("encode heartbeat: %w", err)
	}

	ctx, cancel := etcdContext(ctx)
	defer cancel()
	var leaseID clientv3.LeaseID
	for {
		state, err := s.readState(ctx, false)
		if err != nil {
			return 0, err
		}
		node, configured := state.snapshot.Config.NodeByID(heartbeat.NodeID)
		if !configured || node.Region != heartbeat.Region {
			return state.snapshot.Revision, nil
		}
		if leaseID == 0 {
			lease, err := s.client.Grant(ctx, s.ttl)
			if err != nil {
				return 0, fmt.Errorf("grant etcd heartbeat lease: %w", err)
			}
			// The lease expires on its own, including when a prior lease is
			// superseded or a commit's outcome is unknown.
			leaseID = lease.ID
		}
		compares := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(s.configKey()), "=", state.configMod)}
		ops := []clientv3.Op{clientv3.OpPut(s.heartbeatKey(heartbeat.NodeID), string(value), clientv3.WithLease(leaseID))}
		if previous, received := state.receipts[heartbeat.NodeID]; !received || heartbeat.ReceivedAt.After(previous.ReceivedAt) {
			state.receipts[heartbeat.NodeID] = HeartbeatReceipt{
				NodeID: heartbeat.NodeID, Region: heartbeat.Region, ReceivedAt: heartbeat.ReceivedAt,
			}
			op, err := putReceipts(s.receiptsKey(), state.receipts)
			if err != nil {
				return 0, err
			}
			compares = append(compares, clientv3.Compare(clientv3.ModRevision(s.receiptsKey()), "=", state.receiptsMod))
			ops = append(ops, op)
		}
		result, err := s.client.Txn(ctx).If(compares...).Then(ops...).Commit()
		if err != nil {
			return 0, fmt.Errorf("write etcd heartbeat: %w", err)
		}
		if result.Succeeded {
			return state.snapshot.Revision, nil
		}
	}
}

func putReceipts(key string, receipts map[string]HeartbeatReceipt) (clientv3.Op, error) {
	// A single document lets publication prune any number of departed pairs
	// atomically within etcd's per-transaction operation limit.
	value, err := json.Marshal(receipts)
	if err != nil {
		return clientv3.Op{}, fmt.Errorf("encode heartbeat receipts: %w", err)
	}
	return clientv3.OpPut(key, string(value)), nil
}

func pruneReceipts(receipts map[string]HeartbeatReceipt, config *cpconfig.Config) bool {
	changed := false
	for id, receipt := range receipts {
		if node, ok := config.NodeByID(id); !ok || node.Region != receipt.Region {
			delete(receipts, id)
			changed = true
		}
	}
	return changed
}

func removesNodeRegion(current, next *cpconfig.Config) bool {
	if current != nil {
		for _, node := range current.Nodes {
			if replacement, ok := next.NodeByID(node.ID); !ok || replacement.Region != node.Region {
				return true
			}
		}
	}
	return false
}

// Heartbeat implements Store. The status disappears when its lease expires.
func (s *EtcdStore) Heartbeat(ctx context.Context, nodeID string) (Heartbeat, bool, error) {
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	result, err := s.client.Get(ctx, s.heartbeatKey(nodeID))
	if err != nil {
		return Heartbeat{}, false, fmt.Errorf("read etcd heartbeat: %w", err)
	}
	if len(result.Kvs) == 0 {
		return Heartbeat{}, false, nil
	}
	heartbeat, err := decodeEtcdHeartbeat(result.Kvs[0].Value)
	return heartbeat, err == nil, err
}

func decodeEtcdHeartbeat(value []byte) (Heartbeat, error) {
	var heartbeat Heartbeat
	if err := json.Unmarshal(value, &heartbeat); err != nil {
		return Heartbeat{}, fmt.Errorf("decode etcd heartbeat: %w", err)
	}
	return heartbeat, nil
}

// HeartbeatReceipts implements Store.
func (s *EtcdStore) HeartbeatReceipts(ctx context.Context) ([]HeartbeatReceipt, error) {
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	state, err := s.readState(ctx, false)
	if err != nil {
		return nil, err
	}
	if state.snapshot.Config == nil {
		return nil, nil
	}
	receipts := make([]HeartbeatReceipt, 0, len(state.receipts))
	for _, node := range state.snapshot.Config.Nodes {
		if receipt, ok := state.receipts[node.ID]; ok && receipt.Region == node.Region {
			receipts = append(receipts, receipt)
		}
	}
	return receipts, nil
}

// LiveConfig implements Store. Lease expiry is authoritative for liveness
// after a heartbeat from the current membership has been observed.
func (s *EtcdStore) LiveConfig(ctx context.Context, _ time.Time, ttl time.Duration) (Snapshot, error) {
	if ttl <= 0 {
		return Snapshot{}, fmt.Errorf("statestore: node TTL must be positive")
	}
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	state, err := s.readState(ctx, true)
	if err != nil {
		return Snapshot{}, err
	}
	heartbeats := make(map[string]Heartbeat, len(state.heartbeats))
	for _, kv := range state.heartbeats {
		heartbeat, err := decodeEtcdHeartbeat(kv.Value)
		if err != nil {
			return Snapshot{}, err
		}
		heartbeats[heartbeat.NodeID] = heartbeat
	}
	return projectLiveConfig(state.snapshot, func(node cpconfig.Node) replicaHeartbeat {
		receipt, recorded := state.receipts[node.ID]
		received := recorded && receipt.Region == node.Region
		heartbeat, hasLease := heartbeats[node.ID]
		return replicaHeartbeat{
			heartbeat: heartbeat,
			received:  received,
			live:      received && hasLease && heartbeat.Region == node.Region,
		}
	}), nil
}
