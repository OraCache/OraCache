package statestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/OraCache/oracache/internal/cpconfig"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrPublicationConflict = errors.New("configuration, intent or election changed before publication")

// VersionedConfig returns the application snapshot and the mod revision used by
// its later compare-and-swap.
func (s *EtcdStore) VersionedConfig(ctx context.Context) (Snapshot, int64, error) {
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	response, err := s.client.Get(ctx, s.configKey())
	if err != nil {
		return Snapshot{}, 0, fmt.Errorf("read desired topology: %w", err)
	}
	snapshot, err := decodeEtcdConfig(response.Kvs)
	if err != nil || len(response.Kvs) == 0 {
		return snapshot, 0, err
	}
	return snapshot, response.Kvs[0].ModRevision, nil
}

type ElectionFence struct {
	Key      string
	Revision int64
	Lease    clientv3.LeaseID
}

// CommitIntent finishes an operator intent, optionally publishing its topology.
// It checks the election key and snapshot CAS in the same transaction.
func (s *EtcdStore) CommitIntent(ctx context.Context, next *cpconfig.Config, modRevision int64, fence ElectionFence,
	pendingKey string, pendingRevision int64, resultKey, resultValue string) error {
	if fence.Key == "" || fence.Revision <= 0 || fence.Lease == 0 {
		return errors.New("publication requires an active election fence")
	}
	ctx, cancel := etcdContext(ctx)
	defer cancel()
	state, err := s.readState(ctx, false)
	if err != nil {
		return err
	}
	if state.configMod != modRevision {
		return ErrPublicationConflict
	}
	compares := []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(fence.Key), "=", fence.Revision),
		clientv3.Compare(clientv3.LeaseValue(fence.Key), "=", int64(fence.Lease)),
		clientv3.Compare(clientv3.ModRevision(pendingKey), "=", pendingRevision),
		clientv3.Compare(clientv3.CreateRevision(resultKey), "=", 0),
	}
	ops := []clientv3.Op{clientv3.OpDelete(pendingKey), clientv3.OpPut(resultKey, resultValue)}
	if next != nil {
		if err := next.Validate(); err != nil {
			return err
		}
		if err := validateConfigTransition(state.snapshot, next); err != nil {
			return err
		}
		value, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("encode new desired topology: %w", err)
		}
		checks, writes, err := s.configPublication(state, next, value)
		if err != nil {
			return err
		}
		compares = append(compares, checks...)
		ops = append(ops, writes...)
	} else {
		compares = append(compares, clientv3.Compare(clientv3.ModRevision(s.configKey()), "=", modRevision))
	}
	response, err := s.client.Txn(ctx).If(compares...).Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("commit static topology intent: %w", err)
	}
	if !response.Succeeded {
		return ErrPublicationConflict
	}
	return nil
}

func (s *EtcdStore) configPublication(state etcdState, config *cpconfig.Config, value []byte) ([]clientv3.Cmp, []clientv3.Op, error) {
	compares := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(s.configKey()), "=", state.configMod)}
	ops := []clientv3.Op{clientv3.OpPut(s.configKey(), string(value))}
	pruned := pruneReceipts(state.receipts, config)
	if pruned {
		op, err := putReceipts(s.receiptsKey(), state.receipts)
		if err != nil {
			return nil, nil, err
		}
		ops = append(ops, op)
	}
	// Use the receipt CAS even when no receipt for a departing node was present
	// in the read. A concurrent heartbeat may have recorded it in the meantime.
	if pruned || removesNodeRegion(state.snapshot.Config, config) {
		compares = append(compares, clientv3.Compare(clientv3.ModRevision(s.receiptsKey()), "=", state.receiptsMod))
	}
	return compares, ops, nil
}
