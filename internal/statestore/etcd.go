package statestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/OraCache/oracache/internal/cpconfig"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const etcdOperationTimeout = 2 * time.Second

// EtcdStore stores Control Plane state in an etcd namespace. The caller owns
// the client and may share it with other users of the same etcd cluster.
type EtcdStore struct {
	client *clientv3.Client
	prefix string
	ttl    int64 // etcd grants leases in whole seconds
}

var _ Store = (*EtcdStore)(nil)

// NewEtcd constructs a Store rooted at prefix. Use the same prefix and nodeTTL
// across CP API instances that serve the same topology.
func NewEtcd(client *clientv3.Client, prefix string, nodeTTL time.Duration) (*EtcdStore, error) {
	prefix = strings.TrimRight(prefix, "/")
	switch {
	case client == nil:
		return nil, errors.New("statestore: etcd client is required")
	case !strings.HasPrefix(prefix, "/"):
		return nil, errors.New("statestore: etcd prefix must be an absolute, non-root namespace")
	case nodeTTL <= 0:
		return nil, errors.New("statestore: node TTL must be positive")
	}
	ttl := int64(nodeTTL / time.Second)
	if nodeTTL%time.Second != 0 {
		ttl++
	}
	return &EtcdStore{client: client, prefix: prefix, ttl: ttl}, nil
}

func (s *EtcdStore) configKey() string   { return s.prefix + "/config/snapshot" }
func (s *EtcdStore) receiptsKey() string { return s.prefix + "/heartbeat-receipts" }

func etcdContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(clientv3.WithRequireLeader(ctx), etcdOperationTimeout)
}

// Config implements Store.
func (s *EtcdStore) Config(ctx context.Context) (Snapshot, error) {
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	response, err := s.client.Get(ctx, s.configKey())
	if err != nil {
		return Snapshot{}, fmt.Errorf("read etcd config: %w", err)
	}
	return decodeEtcdConfig(response.Kvs)
}

// BootstrapConfig publishes config only when the namespace has no snapshot.
// A concurrent bootstrap or publisher may initialize the namespace first.
func (s *EtcdStore) BootstrapConfig(ctx context.Context, config *cpconfig.Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	value, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode bootstrap config: %w", err)
	}

	ctx, cancel := etcdContext(ctx)
	defer cancel()
	_, err = s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(s.configKey()), "=", 0)).
		Then(clientv3.OpPut(s.configKey(), string(value))).
		Commit()
	if err != nil {
		return fmt.Errorf("bootstrap etcd config: %w", err)
	}
	return nil
}

// ApplyConfig implements Store. It retries validation against the committed
// snapshot whenever another publisher wins the CAS.
func (s *EtcdStore) ApplyConfig(ctx context.Context, config *cpconfig.Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	value, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}

	ctx, cancel := etcdContext(ctx)
	defer cancel()
	for {
		state, err := s.readState(ctx, false)
		if err != nil {
			return err
		}
		if err := validateConfigTransition(state.snapshot, config); err != nil {
			return err
		}

		compares := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(s.configKey()), "=", state.configMod)}
		ops := []clientv3.Op{clientv3.OpPut(s.configKey(), string(value))}
		pruned := pruneReceipts(state.receipts, config)
		if pruned {
			op, err := putReceipts(s.receiptsKey(), state.receipts)
			if err != nil {
				return err
			}
			ops = append(ops, op)
		}
		// A heartbeat may record a receipt for a departing node between the
		// read and the commit, even when no receipt needed pruning at the read.
		if pruned || removesNodeRegion(state.snapshot.Config, config) {
			compares = append(compares, clientv3.Compare(clientv3.ModRevision(s.receiptsKey()), "=", state.receiptsMod))
		}
		result, err := s.client.Txn(ctx).If(compares...).Then(ops...).Commit()
		if err != nil {
			return fmt.Errorf("publish etcd config: %w", err)
		}
		if result.Succeeded {
			return nil
		}
	}
}

func decodeEtcdConfig(kvs []*mvccpb.KeyValue) (Snapshot, error) {
	if len(kvs) == 0 {
		return Snapshot{}, nil
	}
	var config cpconfig.Config
	if err := json.Unmarshal(kvs[0].Value, &config); err != nil {
		return Snapshot{}, fmt.Errorf("decode etcd config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Snapshot{}, fmt.Errorf("validate etcd config: %w", err)
	}
	return Snapshot{Revision: config.Revision, Config: &config}, nil
}

type etcdState struct {
	snapshot    Snapshot
	configMod   int64
	receipts    map[string]HeartbeatReceipt
	receiptsMod int64
	heartbeats  []*mvccpb.KeyValue
}

// readState reads the requested keys at one etcd revision. Independent reads
// could combine old topology with observations recorded for a newer topology.
func (s *EtcdStore) readState(ctx context.Context, live bool) (etcdState, error) {
	ops := []clientv3.Op{clientv3.OpGet(s.configKey()), clientv3.OpGet(s.receiptsKey())}
	if live {
		ops = append(ops, clientv3.OpGet(s.heartbeatPrefix(), clientv3.WithPrefix()))
	}
	response, err := s.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return etcdState{}, fmt.Errorf("read etcd control plane state: %w", err)
	}
	config := response.Responses[0].GetResponseRange().Kvs
	snapshot, err := decodeEtcdConfig(config)
	if err != nil {
		return etcdState{}, err
	}
	state := etcdState{snapshot: snapshot, receipts: make(map[string]HeartbeatReceipt)}
	if len(config) != 0 {
		state.configMod = config[0].ModRevision
	}
	if receipts := response.Responses[1].GetResponseRange().Kvs; len(receipts) != 0 {
		if err := json.Unmarshal(receipts[0].Value, &state.receipts); err != nil {
			return etcdState{}, fmt.Errorf("decode etcd heartbeat receipts: %w", err)
		}
		if state.receipts == nil {
			return etcdState{}, errors.New("decode etcd heartbeat receipts: expected an object")
		}
		state.receiptsMod = receipts[0].ModRevision
	}
	if live {
		state.heartbeats = response.Responses[2].GetResponseRange().Kvs
	}
	return state, nil
}
